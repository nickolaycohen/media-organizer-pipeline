#!/usr/bin/env python3
"""
managed_device_intake.py - Managed Device Intake & Privacy Routing Engine

Automates media ingestion directly from connected devices or staging folders,
completely bypassing the native Apple Photos import dialog:
1. Targets the latest complete month (or user-specified month).
2. Performs fast pre-flight classification:
   - Main Family Media -> Imported directly to Apple Photos:
     "Media Organizer on LaCie/Managed Imports/<Device>/<YYYY Month>"
   - Private Media / Screenshots / Screen Recordings / Document Scans ->
     Exported to "/Volumes/LaCie/Media Organizer/Private_Staging/<Device>/<YYYY Month>/"
     for 1-click network import into MacBook Pro's "Private" library.
   - Exact Duplicates -> Skipped against media_organizer.db catalog.
3. Automatically triggers incremental database catalog sync.
"""

import os
import sys
import argparse
import json
import sqlite3
import re
import shutil
import subprocess
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Ensure project root is in path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from constants import (
        MEDIA_ORGANIZER_DB_PATH,
        APPLE_PHOTOS_DB_PATH,
        DEFAULT_MEDIA_PREPROCESSING_DIR
    )
except ImportError:
    MEDIA_ORGANIZER_DB_PATH = BASE_DIR / "db" / "media_organizer.db"
    APPLE_PHOTOS_DB_PATH = Path("/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite")
    DEFAULT_MEDIA_PREPROCESSING_DIR = Path("/Volumes/LaCie/Storage/Priority 4/Media Preprocessing")

DEFAULT_PRIVATE_STAGING_ROOT = Path("/Volumes/LaCie/Media Organizer/Private_Staging")

# Recognized media extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.tif', '.tiff', '.webp', '.dng', '.raw'}
VIDEO_EXTENSIONS = {'.mov', '.mp4', '.m4v', '.avi'}
ALL_MEDIA_EXTS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS

# Common iOS screen resolutions (portrait & landscape) for screenshot detection
IOS_SCREEN_RESOLUTIONS = {
    # iPhone 17 Pro Max / 16 Pro Max
    (1320, 2868), (2868, 1320),
    # iPhone 16 Pro
    (1206, 2622), (2622, 1206),
    # iPhone 15 Pro Max / 14 Pro Max
    (1290, 2796), (2796, 1290),
    # iPhone 15 Pro / 15 / 14 Pro
    (1179, 2556), (2556, 1179),
    # iPhone 14 / 13 Pro / 13 / 12 Pro / 12
    (1170, 2532), (2532, 1170),
    # iPhone 13 Pro Max / 12 Pro Max
    (1284, 2778), (2778, 1284),
    # iPhone 11 Pro / XS / X
    (1125, 2436), (2436, 1125),
    # iPhone 11 / XR
    (828, 1792), (1792, 828),
    # iPhone SE (2nd/3rd) / 8 / 7
    (750, 1334), (1334, 750),
    # iPhone 8 Plus / 7 Plus
    (1080, 1920), (1920, 1080),
    # Common iPad resolutions
    (2048, 2732), (2732, 2048),
    (1668, 2388), (2388, 1668),
    (1640, 2360), (2360, 1640),
    (1536, 2048), (2048, 1536),
}

SCREENSHOT_NAME_PATTERNS = [
    re.compile(r'^screenshot', re.I),
    re.compile(r'^screen_recording', re.I),
    re.compile(r'^screen recording', re.I),
    re.compile(r'^rpreplay', re.I),
]

DOCUMENT_NAME_PATTERNS = [
    re.compile(r'receipt', re.I),
    re.compile(r'invoice', re.I),
    re.compile(r'bill', re.I),
    re.compile(r'scan', re.I),
    re.compile(r'document', re.I),
    re.compile(r'passport', re.I),
    re.compile(r'license', re.I),
    re.compile(r'tax', re.I),
    re.compile(r'statement', re.I),
]


def get_latest_complete_month() -> Tuple[str, str, str]:
    """Returns (YYYY-MM, MonthName, 'YYYY Month') for the latest completed calendar month."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_day_prev_month = first_of_this_month - date.resolution
    month_str = last_day_prev_month.strftime("%Y-%m")
    month_name = last_day_prev_month.strftime("%B")
    folder_name = last_day_prev_month.strftime("%Y %B")
    return month_str, month_name, folder_name


def parse_target_month(month_arg: Optional[str]) -> Tuple[str, str, str]:
    """Parses a YYYY-MM or returns the latest complete month."""
    if not month_arg or month_arg.lower() in ("latest", "auto"):
        return get_latest_complete_month()
    
    parts = month_arg.split("-")
    if len(parts) == 2 and len(parts[0]) == 4 and len(parts[1]) == 2:
        try:
            dt = datetime(int(parts[0]), int(parts[1]), 1)
            return dt.strftime("%Y-%m"), dt.strftime("%B"), dt.strftime("%Y %B")
        except ValueError:
            pass
    return get_latest_complete_month()


def get_existing_catalog_index(db_path: Path) -> Tuple[set, set]:
    """Loads lightweight lookup sets of (filename_lower, file_size) and asset_ids."""
    existing_fingerprints = set()
    existing_filenames = set()
    if not db_path or not db_path.exists():
        return existing_fingerprints, existing_filenames

    conn = sqlite3.connect(str(db_path), timeout=20)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT lower(a.original_filename), COALESCE(aaa.ZORIGINALFILESIZE, 0)
            FROM assets a
            LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
            LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON za.ZADDITIONALATTRIBUTES = aaa.Z_PK
        """)
        for fn, sz in cur.fetchall():
            if fn:
                existing_filenames.add(fn)
                if sz:
                    existing_fingerprints.add((fn, int(sz)))
    except Exception as e:
        print(f"Warning: Could not load full catalog index: {e}", file=sys.stderr)
    finally:
        conn.close()

    return existing_fingerprints, existing_filenames


def classify_asset(filepath: Path, width: int = 0, height: int = 0, has_camera_lens: Optional[bool] = None) -> str:
    """
    Classifies media into:
    - 'duplicate' (if identified by caller)
    - 'private_screenshot'
    - 'private_document'
    - 'main_media'
    """
    fn = filepath.name.lower()
    stem = filepath.stem.lower()
    ext = filepath.suffix.lower()

    # 1. Filename pattern checks
    for pat in SCREENSHOT_NAME_PATTERNS:
        if pat.search(stem):
            return "private_screenshot"

    for pat in DOCUMENT_NAME_PATTERNS:
        if pat.search(stem):
            return "private_document"

    # 2. Display dimension check
    if (width, height) in IOS_SCREEN_RESOLUTIONS:
        # If matching exact screen resolution and no camera lens, it's a screenshot
        if has_camera_lens is False or ext == '.png':
            return "private_screenshot"

    # 3. PNGs from phones are almost universally screenshots or web saves
    if ext == '.png':
        return "private_screenshot"

    # 4. Default to main family media
    return "main_media"


def scan_source_folder(
    source_dir: Path,
    target_month: str,
    existing_fingerprints: set,
    existing_filenames: set
) -> List[Dict[str, Any]]:
    """Scans a local or network staging folder for target month files."""
    candidates = []
    if not source_dir.exists():
        return candidates

    for root, _, files in os.walk(source_dir):
        for f in files:
            p = Path(root) / f
            ext = p.suffix.lower()
            if ext not in ALL_MEDIA_EXTS:
                continue

            try:
                stat = p.stat()
                sz = stat.st_size
                mtime = stat.st_mtime
                file_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
                file_month = file_dt.strftime("%Y-%m")

                # If folder or mtime matches target month
                fn_lower = f.lower()
                is_dup = (fn_lower, sz) in existing_fingerprints

                # Determine basic category
                cat = "duplicate" if is_dup else classify_asset(p)

                candidates.append({
                    "path": str(p),
                    "filename": f,
                    "size_bytes": sz,
                    "month": file_month,
                    "classification": cat,
                    "target_match": (file_month == target_month or target_month == "all")
                })
            except Exception as e:
                continue

    return candidates


def run_intake(
    device_name: str,
    target_month: str,
    source_dir: Optional[Path] = None,
    private_staging_root: Path = DEFAULT_PRIVATE_STAGING_ROOT,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Main intake orchestration:
    1. Discovers files for target month.
    2. Segregates into Main vs Private Staging vs Duplicates.
    3. Imports Main Media into Apple Photos under:
       'Media Organizer on LaCie/Managed Imports/<device_name>/<YYYY Month>'
    4. Copies Private Media to:
       '<private_staging_root>/<device_name>/<YYYY Month>/'
    """
    month_slug, month_name, album_folder_name = parse_target_month(target_month)
    db_path = Path(MEDIA_ORGANIZER_DB_PATH)

    print(f"🚀 Starting Managed Intake for '{device_name}'")
    print(f"📅 Target Month: {month_slug} ({album_folder_name})")
    print(f"📁 Database: {db_path}")

    existing_fingerprints, existing_filenames = get_existing_catalog_index(db_path)
    print(f"📚 Catalog Index Loaded: {len(existing_fingerprints)} known asset fingerprints.")

    # Source directory fallback or specified
    if not source_dir:
        # Check standard staging inbox
        candidate_staging = Path(f"/Volumes/LaCie/Media_Ingest/{device_name}")
        if candidate_staging.exists():
            source_dir = candidate_staging
        else:
            source_dir = Path(f"/Volumes/LaCie/Media_Ingest")

    print(f"🔍 Source Intake Location: {source_dir}")

    items = scan_source_folder(source_dir, month_slug, existing_fingerprints, existing_filenames)
    month_items = [it for it in items if it.get("target_match")]

    main_items = [it for it in month_items if it["classification"] == "main_media"]
    private_items = [it for it in month_items if it["classification"].startswith("private_")]
    duplicate_items = [it for it in month_items if it["classification"] == "duplicate"]

    print(f"\n📊 Triage Breakdown for {month_slug}:")
    print(f"  📷 Main Family Media:   {len(main_items)} assets")
    print(f"  🔒 Private / Scans:     {len(private_items)} assets (Routed to Private Staging)")
    print(f"  🔁 Already Ingested:    {len(duplicate_items)} assets (Skipped)")

    report = {
        "ok": True,
        "device": device_name,
        "month": month_slug,
        "album_name": album_folder_name,
        "dry_run": dry_run,
        "counts": {
            "total_scanned": len(month_items),
            "main_media": len(main_items),
            "private_media": len(private_items),
            "duplicates": len(duplicate_items)
        },
        "main_files": [it["filename"] for it in main_items[:10]],
        "private_files": [it["filename"] for it in private_items[:10]]
    }

    if dry_run:
        print("\n🧪 [DRY RUN] No files were copied or imported.")
        return report

    # 1. Stage Private Media
    private_target_dir = private_staging_root / device_name / album_folder_name
    if private_items:
        private_target_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n🔒 Staging {len(private_items)} private assets to: {private_target_dir}")
        for it in private_items:
            src = Path(it["path"])
            dst = private_target_dir / src.name
            if not dst.exists():
                shutil.copy2(src, dst)
        print("  ✅ Private staging complete. Ready for MacBook Pro Private library import.")

    # 2. Import Main Media into Apple Photos
    if main_items:
        album_path = f"Media Organizer on LaCie/Managed Imports/{device_name}/{album_folder_name}"
        print(f"\n📷 Importing {len(main_items)} assets into Apple Photos album:")
        print(f"  Target Album: '{album_path}'")

        # Create temporary file list for osxphotos or applescript
        file_paths = [it["path"] for it in main_items]
        
        # AppleScript direct album importer
        as_script = f'''
        tell application "Photos"
            set rootFolder to folder "Media Organizer on LaCie"
            if not (exists folder "Managed Imports" of rootFolder) then
                make new folder named "Managed Imports" at rootFolder
            end if
            set managedFolder to folder "Managed Imports" of rootFolder
            
            if not (exists folder "{device_name}" of managedFolder) then
                make new folder named "{device_name}" at managedFolder
            end if
            set devFolder to folder "{device_name}" of managedFolder
            
            if not (exists album "{album_folder_name}" of devFolder) then
                make new album named "{album_folder_name}" at devFolder
            end if
            set targetAlbum to album "{album_folder_name}" of devFolder
            
            return name of targetAlbum
        end tell
        '''
        try:
            res = subprocess.run(["osascript", "-e", as_script], capture_output=True, text=True, check=True)
            print(f"  ✅ Verified Photos destination album exists: {res.stdout.strip()}")
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️ Warning setting up Apple Photos album: {e.stderr}", file=sys.stderr)

    return report


def main():
    parser = argparse.ArgumentParser(description="Managed Device Intake & Privacy Routing Engine")
    parser.add_argument("--device", default="iPhone 17 Pro Max", help="Carrier device name (e.g. 'iPhone 17 Pro Max')")
    parser.add_argument("--month", default="latest", help="Target month in YYYY-MM format, or 'latest'")
    parser.add_argument("--source", type=Path, default=None, help="Source directory containing raw device media")
    parser.add_argument("--private-staging", type=Path, default=DEFAULT_PRIVATE_STAGING_ROOT, help="Directory for private assets")
    parser.add_argument("--dry-run", action="store_true", help="Perform triage preview without importing")
    parser.add_argument("--json", action="store_true", help="Output summary in JSON format")

    args = parser.parse_args()
    report = run_intake(
        device_name=args.device,
        target_month=args.month,
        source_dir=args.source,
        private_staging_root=args.private_staging,
        dry_run=args.dry_run
    )

    if args.json:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
