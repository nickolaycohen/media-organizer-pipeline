#!/usr/bin/env python3
"""
dedup_unassigned_moments.py

Deduplicates unassigned high-rank assets BEFORE generating Proposed Moment groups,
using the multi-tier comparison approach from triage_media_intake.py:
  - Tier 1: Exact Byte Size + Capture Date (+-1 day)
  - Tier 2: Same Normalized Camera Filename + Capture Date (+-1 day)
  - Tier 2.5: Visual Perceptual Hash (64-bit pHash, Hamming distance <= 3)

Among any matched duplicate group, selects a canonical keeper (preferring
assets already curated/assigned to a Moment album, then above-threshold quality
format/resolution/size) and identifies redundant copies to be placed into the
Apple Photos 'Ignore' album so they are excluded from further curation.
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from constants import (
    APPLE_PHOTOS_LIBRARY_PATH,
    APPLE_PHOTOS_ORIGINALS_PATH,
    APPLE_SCRIPT_LOG_PATH,
    LOG_PATH,
    MEDIA_ORGANIZER_DB_PATH,
)
from utils.logger import setup_logger

try:
    from PIL import Image
    import imagehash
    HAS_IMAGEHASH = True
    Image.MAX_IMAGE_PIXELS = None
except ImportError:
    HAS_IMAGEHASH = False

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass

MODULE_TAG = "dedup_unassigned_moments"
logger = setup_logger(LOG_PATH, MODULE_TAG, include_console=False)
as_logger = setup_logger(APPLE_SCRIPT_LOG_PATH, "applescript_worker", include_console=False)

APPLE_PHOTOS_DERIVATIVES_PATH = os.path.join(APPLE_PHOTOS_LIBRARY_PATH, "resources", "derivatives")
EXCLUDED_ALBUM_NAMES = ("ignore", "skippublishing", "topscore excluded")
EXTENSION_PRIORITY = {"heic": 0, "heif": 0, "dng": 1, "raw": 1, "png": 2, "jpg": 3, "jpeg": 3, "mp4": 0, "mov": 1, "m4v": 2}
COPY_SUFFIX_RE = re.compile(r"(?:\s+[1-9]|\s*\([1-9]\)|-[1-9])$")
PHASH_MAX_HAMMING_DIST = 3


def normalize_filename_stem(filename: str) -> str:
    stem = os.path.splitext(filename or "")[0].strip().lower()
    while True:
        cleaned = COPY_SUFFIX_RE.sub("", stem).strip()
        if cleaned == stem:
            break
        stem = cleaned
    return stem


def has_copy_suffix(filename: str) -> bool:
    stem = os.path.splitext(filename or "")[0].strip()
    return bool(COPY_SUFFIX_RE.search(stem))


def ensure_phash_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asset_phash_cache (
            asset_id TEXT PRIMARY KEY,
            zfilename TEXT,
            file_size INTEGER,
            phash TEXT,
            computed_at_utc TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()


def _resolve_hash_image_path(zdir: str, asset_id: str, zfilename: str) -> str | None:
    if not zdir:
        return None
    if os.path.isdir(APPLE_PHOTOS_DERIVATIVES_PATH):
        for suffix in ("_1_105_c.jpeg", "_1_102_o.jpeg", "_1_100_o.jpeg"):
            cand = os.path.join(APPLE_PHOTOS_DERIVATIVES_PATH, zdir, f"{asset_id}{suffix}")
            if os.path.exists(cand):
                return cand
    if zfilename and os.path.isdir(APPLE_PHOTOS_ORIGINALS_PATH):
        orig = os.path.join(APPLE_PHOTOS_ORIGINALS_PATH, zdir, zfilename)
        if os.path.exists(orig):
            return orig
    return None


def compute_asset_phash(asset_id: str, zdir: str, zfilename: str):
    if not HAS_IMAGEHASH:
        return None, None
    img_path = _resolve_hash_image_path(zdir, asset_id, zfilename)
    if not img_path:
        return None, None
    try:
        with Image.open(img_path) as im:
            h = imagehash.phash(im)
            return h, str(h)
    except Exception:
        return None, None


def get_target_threshold(conn: sqlite3.Connection, explicit_threshold: float | None = None) -> float:
    if explicit_threshold is not None and explicit_threshold > 0:
        return float(explicit_threshold)
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT MIN(threshold_score) FROM threshold_history WHERE threshold_score > 0"
        ).fetchone()
        if row and row[0] is not None and float(row[0]) > 0:
            return float(row[0])
    except sqlite3.Error:
        pass
    return 0.5315


def select_keeper_for_group(members: list[dict], target_threshold: float) -> dict:
    """
    Picks the single canonical keeper from a connected group of duplicate assets:
      1. Already assigned to a valid Moment album (not Ignore/SkipPublishing/TopScore Excluded)
      2. Above curation threshold (so we never drop a high-rank memory in favor of a sub-threshold duplicate)
      3. Higher pixel area (with 5% bucket tolerance)
      4. Preferred archival extension (HEIC > DNG/RAW > PNG > JPG/JPEG; MP4 > MOV)
      5. Larger byte size (prefer higher bitrate / uncompressed within same format)
      6. Higher AI score, clean filename (no duplicate copy suffix), apple/google favorite
    """
    def rank_key(m: dict):
        mom = (m.get("moment") or "").strip()
        is_assigned = 1 if mom and mom.lower() not in EXCLUDED_ALBUM_NAMES else 0
        above_thresh = 1 if m["score"] > target_threshold else 0
        pixels = (m["width"] or 0) * (m["height"] or 0)
        # Bucket resolutions by ~5% to keep HEIC vs JPG comparison clean
        pixel_bucket = int(pixels // 100000)
        ext_prio = EXTENSION_PRIORITY.get(m["ext"], 99)
        clean_name = 0 if has_copy_suffix(m["filename"]) else 1
        fav = 1 if (m.get("apple_favorite") or m.get("google_favorite")) else 0
        return (
            is_assigned,
            above_thresh,
            pixel_bucket,
            -ext_prio,
            m["size"],
            round(m["score"], 4),
            clean_name,
            fav,
            m["asset_id"],
        )

    return max(members, key=rank_key)


class _UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def analyze_unassigned_duplicates(
    db_path: str = MEDIA_ORGANIZER_DB_PATH,
    threshold: float | None = None,
    use_phash: bool = True,
) -> dict:
    t0 = time.time()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    ensure_phash_cache_table(conn)
    cur = conn.cursor()

    target_threshold = get_target_threshold(conn, threshold)

    excluded_aids = {
        r[0]
        for r in cur.execute(
            """
            SELECT DISTINCT moaa.asset_id
            FROM media_organizer_album_assets moaa
            JOIN media_organizer_albums moa ON moa.album_pk = moaa.album_pk
            WHERE LOWER(moa.album_name) IN ('ignore', 'skippublishing', 'topscore excluded')
            """
        ).fetchall()
    }

    unassigned_rows = cur.execute(
        """
        SELECT v.asset_id, v.original_filename, v.score_normalized, v.date_created_utc,
               za.ZDIRECTORY, za.ZFILENAME, COALESCE(aaa.ZORIGINALFILESIZE, 0),
               COALESCE(za.ZKIND, 0), COALESCE(za.ZWIDTH, 0), COALESCE(za.ZHEIGHT, 0),
               (za.ZDATECREATED + 978307200)
        FROM ranked_assets_view v
        JOIN month_batches mb ON v.month = mb.month
        JOIN ZASSET za ON za.ZUUID = v.asset_id
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.Z_PK = za.ZADDITIONALATTRIBUTES
        WHERE mb.status_code >= '600'
          AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '')
          AND COALESCE(za.ZTRASHEDSTATE, 0) = 0
          AND v.score_normalized > ?
        ORDER BY v.score_normalized DESC
        """,
        (target_threshold,),
    ).fetchall()

    unassigned_ids = {r[0] for r in unassigned_rows if r[0] not in excluded_aids}
    candidate_dates = set()
    for r in unassigned_rows:
        if r[0] not in unassigned_ids:
            continue
        dt_str = r[3] or ""
        if len(dt_str) >= 10:
            try:
                dt_base = datetime.strptime(dt_str[:10], "%Y-%m-%d")
                for delta in (-1, 0, 1):
                    candidate_dates.add((dt_base + timedelta(days=delta)).strftime("%Y-%m-%d"))
            except ValueError:
                pass

    if not candidate_dates or not unassigned_ids:
        conn.close()
        return {
            "threshold": round(target_threshold, 4),
            "scanned_unassigned": len(unassigned_ids),
            "pool_size": 0,
            "duplicate_count": 0,
            "reclaimable_bytes": 0,
            "duration_sec": round(time.time() - t0, 2),
            "has_imagehash": HAS_IMAGEHASH,
            "duplicates": [],
        }

    date_list = sorted(candidate_dates)
    placeholders = ",".join(["?"] * len(date_list))
    pool_rows = cur.execute(
        f"""
        SELECT za.ZUUID,
               COALESCE(aaa.ZORIGINALFILENAME, za.ZFILENAME),
               datetime(za.ZDATECREATED + 978307200, 'unixepoch'),
               za.ZDIRECTORY,
               za.ZFILENAME,
               COALESCE(aaa.ZORIGINALFILESIZE, 0),
               COALESCE(za.ZKIND, 0),
               COALESCE(za.ZWIDTH, 0),
               COALESCE(za.ZHEIGHT, 0),
               (za.ZDATECREATED + 978307200),
               ast.MomentsAlbumName,
               COALESCE(v.score_normalized, ast.aesthetic_score, 0.0),
               COALESCE(ast.apple_favorite, 0),
               COALESCE(ast.google_favorite, 0),
               COALESCE(ast.month, '')
        FROM ZASSET za
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.Z_PK = za.ZADDITIONALATTRIBUTES
        LEFT JOIN assets ast ON ast.asset_id = za.ZUUID
        LEFT JOIN ranked_assets_view v ON v.asset_id = za.ZUUID
        WHERE COALESCE(za.ZTRASHEDSTATE, 0) = 0
          AND substr(datetime(za.ZDATECREATED + 978307200, 'unixepoch'), 1, 10) IN ({placeholders})
        """,
        tuple(date_list),
    ).fetchall()

    pool_map: dict[str, dict] = {}
    by_date_kind: dict[tuple[str, int], list[dict]] = {}
    for row in pool_rows:
        aid = row[0]
        if aid in excluded_aids:
            continue
        fn = row[1] or row[4] or ""
        dt = row[2] or ""
        info = {
            "asset_id": aid,
            "filename": fn,
            "date_utc": dt,
            "ymd": dt[:10] if len(dt) >= 10 else "",
            "zdir": row[3] or "",
            "zfilename": row[4] or "",
            "size": int(row[5] or 0),
            "kind": int(row[6] or 0),
            "width": int(row[7] or 0),
            "height": int(row[8] or 0),
            "unix_ts": float(row[9] or 0.0),
            "moment": row[10] or "",
            "score": float(row[11] or 0.0),
            "apple_favorite": int(row[12] or 0),
            "google_favorite": int(row[13] or 0),
            "month": row[14] or (dt[:7] if len(dt) >= 7 else ""),
            "stem": normalize_filename_stem(fn),
            "ext": os.path.splitext(fn.lower())[1].lstrip("."),
        }
        pool_map[aid] = info
        if info["ymd"]:
            by_date_kind.setdefault((info["ymd"], info["kind"]), []).append(info)

    # Preload cached pHashes
    cached_hashes_db = {
        r[0]: r[1]
        for r in cur.execute("SELECT asset_id, phash FROM asset_phash_cache WHERE phash IS NOT NULL").fetchall()
    }
    hash_int_cache: dict[str, int | None] = {}
    new_hashes_to_persist: list[tuple[str, str, int, str]] = []

    def get_phash_int(info: dict) -> int | None:
        aid = info["asset_id"]
        if aid in hash_int_cache:
            return hash_int_cache[aid]
        if aid in cached_hashes_db:
            try:
                val = int(cached_hashes_db[aid], 16)
                hash_int_cache[aid] = val
                return val
            except Exception:
                pass
        if HAS_IMAGEHASH:
            _, h_hex = compute_asset_phash(aid, info["zdir"], info["zfilename"])
            if h_hex:
                new_hashes_to_persist.append((aid, info["zfilename"], info["size"], h_hex))
                try:
                    val = int(h_hex, 16)
                    hash_int_cache[aid] = val
                    return val
                except Exception:
                    pass
        hash_int_cache[aid] = None
        return None

    uf = _UnionFind(pool_map.keys())
    edge_meta: dict[tuple[str, str], dict] = {}

    def record_match(a_id: str, b_id: str, tier: str, dist: int, sim: float):
        uf.union(a_id, b_id)
        key = (a_id, b_id) if a_id < b_id else (b_id, a_id)
        prev = edge_meta.get(key)
        # Keep highest-precision edge metadata (Tier 1 < Tier 2 < Tier 2.5)
        if not prev or sim > prev["similarity_pct"] or (sim == prev["similarity_pct"] and dist < prev["hamming_distance"]):
            edge_meta[key] = {
                "match_tier": tier,
                "hamming_distance": dist,
                "similarity_pct": round(sim, 2),
            }

    for aid in sorted(unassigned_ids):
        me = pool_map.get(aid)
        if not me or not me["ymd"]:
            continue
        try:
            base_dt = datetime.strptime(me["ymd"], "%Y-%m-%d")
        except ValueError:
            continue

        cands = []
        for delta in (-1, 0, 1):
            d_str = (base_dt + timedelta(days=delta)).strftime("%Y-%m-%d")
            cands.extend(by_date_kind.get((d_str, me["kind"]), []))

        matched_metadata_tier = False
        for c in cands:
            cid = c["asset_id"]
            if cid == aid:
                continue
            dt_diff = abs(c["unix_ts"] - me["unix_ts"])
            if dt_diff > 86400:
                continue
            if me["size"] > 0 and c["size"] == me["size"] and (me["stem"] == c["stem"] or dt_diff <= 2.0):
                record_match(aid, cid, "Tier 1: Exact Byte+Date", 0, 100.0)
                matched_metadata_tier = True
            elif me["size"] > 0 and c["size"] == me["size"] and me["ext"] == c["ext"]:
                record_match(aid, cid, "Tier 1: Exact Byte+Date", 0, 100.0)
                matched_metadata_tier = True
            elif me["stem"] and c["stem"] == me["stem"] and dt_diff <= 10.0:
                if me["ext"] == c["ext"]:
                    record_match(aid, cid, "Tier 2: Same Name+Time", 0, 99.5)
                else:
                    # Camera HEIC + JPG export pair sharing exact stem & timestamp
                    record_match(aid, cid, "Tier 2: Format/Quality Pair", 0, 99.0)
                matched_metadata_tier = True

        # Visual perceptual hash check for images (works from cached DB hashes even without PIL/imagehash)
        if use_phash and me["kind"] == 0 and not matched_metadata_tier and (HAS_IMAGEHASH or cached_hashes_db):
            my_h = get_phash_int(me)
            if my_h is not None:
                near_cands = sorted(
                    [c for c in cands if c["asset_id"] != aid and abs(c["unix_ts"] - me["unix_ts"]) <= 86400],
                    key=lambda x: abs(x["unix_ts"] - me["unix_ts"]),
                )[:40]
                for c in near_cands:
                    ch = get_phash_int(c)
                    if ch is None:
                        continue
                    dist = (my_h ^ ch).bit_count()
                    if dist <= PHASH_MAX_HAMMING_DIST:
                        sim = max(0.0, 1.0 - (dist / 64.0)) * 100.0
                        record_match(aid, c["asset_id"], "Tier 2.5: Visual pHash", dist, sim)

    if new_hashes_to_persist:
        cur.executemany(
            """
            INSERT OR REPLACE INTO asset_phash_cache (asset_id, zfilename, file_size, phash)
            VALUES (?, ?, ?, ?)
            """,
            new_hashes_to_persist,
        )
        conn.commit()

    # Collect connected components containing at least one unassigned high-rank asset
    components: dict[str, list[dict]] = {}
    for aid, info in pool_map.items():
        root = uf.find(aid)
        components.setdefault(root, []).append(info)

    duplicates_out = []
    for root, members in components.items():
        if len(members) < 2:
            continue
        if not any(m["asset_id"] in unassigned_ids for m in members):
            continue

        keeper = select_keeper_for_group(members, target_threshold)
        keeper_id = keeper["asset_id"]

        for m in members:
            mid = m["asset_id"]
            if mid == keeper_id or mid not in unassigned_ids:
                continue
            ekey = (mid, keeper_id) if mid < keeper_id else (keeper_id, mid)
            em = edge_meta.get(ekey)
            if not em:
                # Pick best direct edge connected to this member
                best_e = None
                for peer in members:
                    pid = peer["asset_id"]
                    if pid == mid:
                        continue
                    pk = (mid, pid) if mid < pid else (pid, mid)
                    cand_e = edge_meta.get(pk)
                    if cand_e and (not best_e or cand_e["similarity_pct"] > best_e["similarity_pct"]):
                        best_e = cand_e
                em = best_e or {"match_tier": "Cluster Duplicate", "hamming_distance": 0, "similarity_pct": 98.0}

            keeper_assigned = bool(keeper["moment"] and keeper["moment"].lower() not in EXCLUDED_ALBUM_NAMES)
            duplicates_out.append(
                {
                    "duplicate_asset_id": mid,
                    "duplicate_filename": m["filename"],
                    "duplicate_ext": m["ext"].upper(),
                    "duplicate_score": round(m["score"], 4),
                    "duplicate_size_bytes": m["size"],
                    "duplicate_size_mb": round(m["size"] / 1048576.0, 2),
                    "duplicate_resolution": f"{m['width']}x{m['height']}" if m["width"] and m["height"] else "—",
                    "duplicate_date": m["date_utc"],
                    "duplicate_month": m["month"],
                    "keeper_asset_id": keeper_id,
                    "keeper_filename": keeper["filename"],
                    "keeper_ext": keeper["ext"].upper(),
                    "keeper_score": round(keeper["score"], 4),
                    "keeper_size_bytes": keeper["size"],
                    "keeper_size_mb": round(keeper["size"] / 1048576.0, 2),
                    "keeper_resolution": f"{keeper['width']}x{keeper['height']}" if keeper["width"] and keeper["height"] else "—",
                    "keeper_date": keeper["date_utc"],
                    "keeper_moment": keeper["moment"] if keeper_assigned else None,
                    "match_tier": em["match_tier"],
                    "hamming_distance": em["hamming_distance"],
                    "similarity_pct": em["similarity_pct"],
                }
            )

    duplicates_out.sort(key=lambda d: (-d["duplicate_score"], d["duplicate_date"], d["duplicate_filename"]))
    total_reclaimable_bytes = sum(d["duplicate_size_bytes"] for d in duplicates_out)
    conn.close()

    return {
        "threshold": round(target_threshold, 4),
        "scanned_unassigned": len(unassigned_ids),
        "pool_size": len(pool_map),
        "duplicate_count": len(duplicates_out),
        "reclaimable_bytes": total_reclaimable_bytes,
        "duration_sec": round(time.time() - t0, 2),
        "has_imagehash": HAS_IMAGEHASH,
        "newly_hashed": len(new_hashes_to_persist),
        "duplicates": duplicates_out,
    }


def _add_ids_to_photos_ignore_via_applescript(asset_ids: list[str], batch_size: int = 80) -> tuple[int, list[str]]:
    if not asset_ids:
        return 0, []
    total_added = 0
    errors = []

    for i in range(0, len(asset_ids), batch_size):
        chunk = asset_ids[i : i + batch_size]
        ids_lit = ", ".join(f'"{aid.replace(chr(34), "")}"' for aid in chunk)
        script = f'''
        tell application "Photos"
            set topFolderName to "Media Organizer on LaCie"
            if not (exists folder topFolderName) then
                make new folder named topFolderName
            end if
            set topFolder to folder topFolderName

            set targetAlbum to missing value
            if exists folder "Moments" of topFolder then
                set momentsFolder to folder "Moments" of topFolder
                if exists album "Ignore" of momentsFolder then
                    set targetAlbum to album "Ignore" of momentsFolder
                else
                    make new album named "Ignore" at momentsFolder
                    set targetAlbum to album "Ignore" of momentsFolder
                end if
            else if exists album "Ignore" of topFolder then
                set targetAlbum to album "Ignore" of topFolder
            else
                make new album named "Ignore" at topFolder
                set targetAlbum to album "Ignore" of topFolder
            end if

            set assetIds to {{{ids_lit}}}
            set assetsToAdd to {{}}
            repeat with anId in assetIds
                try
                    set end of assetsToAdd to media item id (anId & "/L0/001")
                on error
                    try
                        set foundItems to (media items whose id contains anId)
                        if (count of foundItems) > 0 then
                            set end of assetsToAdd to item 1 of foundItems
                        end if
                    end try
                end try
            end repeat

            if (count of assetsToAdd) > 0 then
                add assetsToAdd to targetAlbum
            end if
            return (count of assetsToAdd) as string
        end tell
        '''
        as_logger.info(f"Adding {len(chunk)} duplicate asset(s) to Photos Ignore album...")
        proc = subprocess.Popen(["osascript", "-e", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0 or (stderr and "error" in stderr.lower()):
            err_msg = (stderr or stdout or f"osascript exit {proc.returncode}").strip()
            errors.append(err_msg)
            as_logger.error(f"AppleScript Ignore error: {err_msg}")
        else:
            try:
                total_added += int((stdout or "0").strip() or 0)
            except ValueError:
                pass
    return total_added, errors


def apply_ignore_to_assets(asset_ids: list[str], db_path: str = MEDIA_ORGANIZER_DB_PATH) -> dict:
    clean_ids = []
    seen = set()
    for aid in asset_ids:
        s = (aid or "").strip()
        if s and s not in seen:
            seen.add(s)
            clean_ids.append(s)

    if not clean_ids:
        return {"ok": True, "requested": 0, "photos_added": 0, "db_updated": 0, "errors": []}

    photos_added, errors = _add_ids_to_photos_ignore_via_applescript(clean_ids)

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    cur = conn.cursor()

    # Locate Moments/Ignore (preferred) or ToBeCurated/Ignore album_pk in local replica
    alb_row = cur.execute(
        """
        SELECT album_pk, full_path
        FROM media_organizer_albums
        WHERE LOWER(album_name) = 'ignore'
        ORDER BY CASE WHEN LOWER(COALESCE(parent_folder_name, '')) = 'moments' THEN 0 ELSE 1 END, album_pk ASC
        LIMIT 1
        """
    ).fetchone()
    if alb_row:
        ignore_album_pk, ignore_path = int(alb_row[0]), alb_row[1]
    else:
        ignore_album_pk = -9999
        ignore_path = "Media Organizer on LaCie/Moments/Ignore"
        cur.execute(
            """
            INSERT OR IGNORE INTO media_organizer_albums
                (album_pk, album_name, parent_folder_pk, parent_folder_name, full_path, is_folder, depth, zkind, asset_count, synced_at_utc)
            VALUES (?, 'Ignore', NULL, 'Moments', ?, 0, 3, 2, 0, datetime('now'))
            """,
            (ignore_album_pk, ignore_path),
        )

    db_inserted = 0
    for aid in clean_ids:
        exists = cur.execute(
            "SELECT 1 FROM media_organizer_album_assets WHERE album_pk = ? AND asset_id = ? LIMIT 1",
            (ignore_album_pk, aid),
        ).fetchone()
        if not exists:
            cur.execute(
                "INSERT INTO media_organizer_album_assets (album_pk, asset_id) VALUES (?, ?)",
                (ignore_album_pk, aid),
            )
            db_inserted += 1

    cur.execute(
        """
        UPDATE media_organizer_albums
        SET asset_count = (SELECT COUNT(*) FROM media_organizer_album_assets WHERE album_pk = ?)
        WHERE album_pk = ?
        """,
        (ignore_album_pk, ignore_album_pk),
    )
    conn.commit()
    conn.close()

    logger.info(
        f"🚫 Intake Dedup added {len(clean_ids)} duplicate(s) to Ignore ({ignore_path}): "
        f"Photos={photos_added}, DB={db_inserted}"
    )
    return {
        "ok": len(errors) == 0,
        "requested": len(clean_ids),
        "photos_added": photos_added,
        "db_updated": db_inserted,
        "ignore_album_path": ignore_path,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate unassigned high-rank assets using Media Intake comparison and move duplicates to Ignore."
    )
    parser.add_argument("--apply", action="store_true", help="Add identified duplicates to Apple Photos Ignore album & DB")
    parser.add_argument("--dry-run", action="store_true", help="Only report matches without modifying Ignore album (default)")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    parser.add_argument("--threshold", type=float, default=None, help="Override curation threshold floor")
    parser.add_argument("--no-phash", action="store_true", help="Disable visual perceptual hash comparison")
    parser.add_argument("--asset-ids", nargs="*", help="When used with --apply, only move these exact duplicate UUIDs to Ignore")
    args = parser.parse_args()

    if args.apply and args.asset_ids:
        res = apply_ignore_to_assets(args.asset_ids)
        if args.json:
            print(json.dumps(res, indent=2))
        else:
            print(f"✅ Applied Ignore to {res['requested']} asset(s) (Photos={res['photos_added']}, DB={res['db_updated']}).")
        return

    analysis = analyze_unassigned_duplicates(threshold=args.threshold, use_phash=not args.no_phash)

    apply_res = None
    if args.apply and analysis["duplicates"]:
        dup_ids = [d["duplicate_asset_id"] for d in analysis["duplicates"]]
        apply_res = apply_ignore_to_assets(dup_ids)

    payload = {**analysis, "apply_result": apply_res}

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print("=" * 95)
    print("🧹 Media-Intake Deduplication & Comparison for Unassigned Moment Candidates")
    print("=" * 95)
    print(
        f"Scanned {analysis['scanned_unassigned']} unassigned assets > {analysis['threshold']:.4f} "
        f"against {analysis['pool_size']} library peers ({analysis['duration_sec']:.2f}s)"
    )
    print(f"Identified {analysis['duplicate_count']} duplicate(s) ({analysis['reclaimable_bytes'] / 1048576.0:.1f} MB)")
    for idx, d in enumerate(analysis["duplicates"], 1):
        mom_info = f" [Moment: {d['keeper_moment']}]" if d["keeper_moment"] else ""
        print(
            f"  {idx:>3}. {d['duplicate_filename']} ({d['duplicate_size_mb']:.2f} MB, sc={d['duplicate_score']:.4f}) "
            f"--[{d['match_tier']} {d['similarity_pct']:.1f}%]--> KEEP {d['keeper_filename']} "
            f"({d['keeper_size_mb']:.2f} MB, sc={d['keeper_score']:.4f}){mom_info}"
        )
    if apply_res:
        print(
            f"\n🚫 Added {apply_res['requested']} duplicate(s) to '{apply_res['ignore_album_path']}' "
            f"(Apple Photos added: {apply_res['photos_added']}, DB updated: {apply_res['db_updated']})"
        )


if __name__ == "__main__":
    main()

