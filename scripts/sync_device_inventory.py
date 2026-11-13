#!/usr/bin/env python3
"""
sync_device_inventory.py - Syncs physical device intake snapshots into media_organizer.db.

Scans ManagedIntake folders (e.g. '/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/ManagedIntake/iPhone 16 '),
maps the files present on each device against cataloged assets in media_organizer.db,
and stores verified per-device inventory records in `device_actual_assets` and
`device_inventory_snapshots`.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from constants import MANAGED_INTAKE_ROOT, MEDIA_ORGANIZER_DB_PATH
except ImportError:
    MEDIA_ORGANIZER_DB_PATH = BASE_DIR / "db" / "media_organizer.db"
    MANAGED_INTAKE_ROOT = "/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/ManagedIntake"

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.tif', '.tiff', '.webp', '.dng', '.raw', '.gif', '.bmp'}
VIDEO_EXTENSIONS = {'.mov', '.mp4', '.m4v', '.avi', '.3gp'}
ALL_MEDIA_EXTS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS

# Known folder name aliases (stripped lowercase -> canonical camera_model in DB)
FOLDER_MODEL_ALIASES = {
    "iphone 16": "iPhone 16 Pro",
    "iphone 16 pro": "iPhone 16 Pro",
    "iphone 17 pro max": "iPhone 17 Pro Max",
}


def ensure_device_inventory_schema(conn: sqlite3.Connection) -> None:
    """Creates device_actual_assets and device_inventory_snapshots tables and indexes if needed."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_actual_assets (
            camera_model TEXT NOT NULL,
            asset_id TEXT NOT NULL,
            device_filename TEXT NOT NULL,
            device_file_size INTEGER DEFAULT 0,
            match_tier TEXT DEFAULT 'exact',
            source_folder TEXT,
            synced_at_utc TEXT NOT NULL,
            PRIMARY KEY (camera_model, asset_id)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_device_actual_asset_id ON device_actual_assets(asset_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_device_actual_model ON device_actual_assets(camera_model);")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_inventory_snapshots (
            camera_model TEXT PRIMARY KEY,
            folder_path TEXT NOT NULL,
            total_files_on_device INTEGER DEFAULT 0,
            matched_in_db INTEGER DEFAULT 0,
            unmatched_files INTEGER DEFAULT 0,
            synced_at_utc TEXT NOT NULL
        );
    """)
    conn.commit()


def resolve_camera_model(folder_name: str, known_models: Set[str]) -> Optional[str]:
    """Resolves a ManagedIntake subfolder name to a canonical DB camera_model."""
    cleaned = folder_name.strip()
    lowered = cleaned.lower()

    if lowered in FOLDER_MODEL_ALIASES:
        alias = FOLDER_MODEL_ALIASES[lowered]
        if alias in known_models or not known_models:
            return alias

    for m in known_models:
        if m.strip().lower() == lowered:
            return m

    prefix_matches = [m for m in known_models if m.strip().lower().startswith(lowered)]
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    return None


def _norm_ext(name: str) -> str:
    ext = os.path.splitext(name)[1].lower()
    return ".jpg" if ext == ".jpeg" else ext


def _build_asset_lookup(conn: sqlite3.Connection) -> Tuple[
    Set[str],
    Dict[Tuple[str, int], List[Tuple[str, str]]],
    Dict[Tuple[int, int], List[Tuple[str, str, Set[str]]]]
]:
    """Loads active catalog assets into fast exact-size lookup maps."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            a.asset_id,
            lower(a.original_filename),
            lower(aaa.ZORIGINALFILENAME),
            lower(za.ZFILENAME),
            COALESCE(aaa.ZORIGINALFILESIZE, 0),
            COALESCE(a.carrier_device, zea.ZCAMERAMODEL, 'Unknown'),
            COALESCE(za.ZKIND, 0)
        FROM assets a
        LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON za.ZADDITIONALATTRIBUTES = aaa.Z_PK
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON za.ZEXTENDEDATTRIBUTES = zea.Z_PK
        WHERE COALESCE(a.removed_from_source, 0) = 0
    """)

    known_models: Set[str] = set()
    by_fn_sz: Dict[Tuple[str, int], List[Tuple[str, str]]] = {}
    by_sz_kind: Dict[Tuple[int, int], List[Tuple[str, str, Set[str]]]] = {}

    for aid, ofn, aofn, zfn, sz, dev, kind in cur.fetchall():
        if dev and dev != "Unknown":
            known_models.add(dev)
        names = {n for n in (ofn, aofn, zfn) if n}
        exts = {_norm_ext(n) for n in names if _norm_ext(n)}
        for n in names:
            if sz:
                by_fn_sz.setdefault((n, sz), []).append((aid, dev))
        if sz and sz > 100000:
            by_sz_kind.setdefault((sz, kind), []).append((aid, dev, exts))

    cur.execute("SELECT DISTINCT camera_model FROM device_owners WHERE camera_model IS NOT NULL")
    for (m,) in cur.fetchall():
        if m:
            known_models.add(m)

    return known_models, by_fn_sz, by_sz_kind


def _pick_preferred(candidates: List[Tuple[str, ...]], target_model: str) -> str:
    for item in candidates:
        if item[1] == target_model:
            return item[0]
    return candidates[0][0]


def sync_intake_to_db(
    db_path: Path = Path(MEDIA_ORGANIZER_DB_PATH),
    intake_root: Path = Path(MANAGED_INTAKE_ROOT),
    target_device: Optional[str] = None
) -> Dict[str, Any]:
    """Scans ManagedIntake device folders and syncs matched inventory into media_organizer.db."""
    if not db_path.exists():
        return {"ok": False, "error": f"Database not found: {db_path}", "devices": []}
    if not intake_root.exists():
        return {"ok": False, "error": f"Intake root not found: {intake_root}", "devices": []}

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        ensure_device_inventory_schema(conn)
        known_models, by_fn_sz, by_sz_kind = _build_asset_lookup(conn)

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        results: List[Dict[str, Any]] = []

        for entry in sorted(os.scandir(intake_root), key=lambda e: e.name.lower()):
            if not entry.is_dir() or entry.name.startswith("."):
                continue

            camera_model = resolve_camera_model(entry.name, known_models)
            if not camera_model:
                continue
            if target_device and camera_model != target_device and entry.name.strip() != target_device:
                continue

            # Track matched asset_ids for this device: asset_id -> (filename, size, tier)
            matched_assets: Dict[str, Tuple[str, int, str]] = {}
            matched_stems: Set[str] = set()
            media_files_info: List[Tuple[str, str, int, str, int]] = []

            for root, _, files in os.walk(entry.path):
                for fname in files:
                    if fname.startswith("."):
                        continue
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in ALL_MEDIA_EXTS:
                        continue
                    full_path = os.path.join(root, fname)
                    try:
                        sz = os.path.getsize(full_path)
                    except OSError:
                        continue
                    stem = os.path.splitext(fname)[0]
                    base_stem = re.sub(r" \d+$", "", stem)
                    kind = 1 if ext in VIDEO_EXTENSIONS else 0
                    media_files_info.append((fname, fname.lower(), sz, (base_stem + ext).lower(), kind))

            unmatched_videos: List[Tuple[str, str]] = []
            unmatched_count = 0

            for orig_name, fn_lower, sz, fn_stripped, kind in media_files_info:
                aid = None
                tier = None
                dev_ext = _norm_ext(fn_lower)

                if (fn_lower, sz) in by_fn_sz:
                    aid = _pick_preferred(by_fn_sz[(fn_lower, sz)], camera_model)
                    tier = "exact_name_size"
                elif (fn_stripped, sz) in by_fn_sz:
                    aid = _pick_preferred(by_fn_sz[(fn_stripped, sz)], camera_model)
                    tier = "stripped_name_size"
                elif (sz, kind) in by_sz_kind and len(by_sz_kind[(sz, kind)]) == 1:
                    cand_aid, cand_dev, cand_exts = by_sz_kind[(sz, kind)][0]
                    if cand_dev == camera_model or dev_ext in cand_exts:
                        aid = cand_aid
                        tier = "unique_size_kind"

                if aid:
                    # Keep highest-confidence entry per asset_id
                    if aid not in matched_assets:
                        matched_assets[aid] = (orig_name, sz, tier or "exact")
                    matched_stems.add(os.path.splitext(fn_lower)[0])
                    matched_stems.add(os.path.splitext(fn_stripped)[0])
                else:
                    if kind == 1:
                        unmatched_videos.append((fn_lower, os.path.splitext(fn_lower)[0]))
                    else:
                        unmatched_count += 1

            # Companion Live Photo .mov files don't exist separately in assets table
            live_photo_companions = 0
            for _, v_stem in unmatched_videos:
                v_base = re.sub(r" \d+$", "", v_stem)
                if v_stem in matched_stems or v_base in matched_stems:
                    live_photo_companions += 1
                else:
                    unmatched_count += 1

            cur = conn.cursor()
            cur.execute("DELETE FROM device_actual_assets WHERE camera_model = ?", (camera_model,))
            rows_to_insert = [
                (camera_model, aid, info[0], info[1], info[2], entry.name, now_utc)
                for aid, info in matched_assets.items()
            ]
            cur.executemany("""
                INSERT INTO device_actual_assets
                    (camera_model, asset_id, device_filename, device_file_size, match_tier, source_folder, synced_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows_to_insert)

            cur.execute("""
                INSERT INTO device_inventory_snapshots
                    (camera_model, folder_path, total_files_on_device, matched_in_db, unmatched_files, synced_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(camera_model) DO UPDATE SET
                    folder_path = excluded.folder_path,
                    total_files_on_device = excluded.total_files_on_device,
                    matched_in_db = excluded.matched_in_db,
                    unmatched_files = excluded.unmatched_files,
                    synced_at_utc = excluded.synced_at_utc
            """, (
                camera_model,
                entry.path,
                len(media_files_info),
                len(matched_assets),
                unmatched_count,
                now_utc
            ))
            conn.commit()

            results.append({
                "camera_model": camera_model,
                "source_folder": entry.path,
                "total_media_files": len(media_files_info),
                "matched_unique_assets": len(matched_assets),
                "live_photo_companions": live_photo_companions,
                "unmatched_files": unmatched_count,
                "synced_at_utc": now_utc
            })

        return {"ok": True, "devices": results, "synced_at_utc": now_utc}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Sync ManagedIntake device snapshots to media_organizer.db")
    parser.add_argument("--db", type=Path, default=Path(MEDIA_ORGANIZER_DB_PATH))
    parser.add_argument("--intake-root", type=Path, default=Path(MANAGED_INTAKE_ROOT))
    parser.add_argument("--device", default=None, help="Optional single camera_model or folder name to sync")
    parser.add_argument("--json", action="store_true", help="Output JSON result")
    args = parser.parse_args()

    res = sync_intake_to_db(db_path=args.db, intake_root=args.intake_root, target_device=args.device)
    if args.json:
        print(json.dumps(res, indent=2))
    else:
        if not res.get("ok"):
            print(f"❌ Sync failed: {res.get('error')}", file=sys.stderr)
            sys.exit(1)
        print("📱 Device Actual Inventory Sync Complete:")
        for d in res["devices"]:
            print(
                f"  • {d['camera_model']} ({Path(d['source_folder']).name!r}): "
                f"{d['matched_unique_assets']:,} DB assets matched "
                f"(+{d['live_photo_companions']:,} Live Photo companions, "
                f"{d['unmatched_files']:,} unmatched out of {d['total_media_files']:,} files)"
            )


if __name__ == "__main__":
    main()
