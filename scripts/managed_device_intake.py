#!/usr/bin/env python3
"""
managed_device_intake.py - Managed Device Intake & Privacy Routing Engine

Automates media ingestion directly from connected devices or staging folders,
completely bypassing the native Apple Photos import dialog:
1. Targets the latest complete month (or user-specified month).
2. Probes for connected iOS / camera devices via ImageCaptureCore over USB/Wi-Fi:
   - Detects device model and authorization/lock state.
   - Prompts if passcode unlock is required.
   - Pulls net-new assets into Priority 4 staging.
3. Performs fast pre-flight classification:
   - Main Family Media -> Imported directly into Apple Photos album:
     "Media Organizer on LaCie/Managed Imports/<Device>/<YYYY Month>"
   - Private Media / Screenshots / Screen Recordings / Document Scans ->
     Exported to "/Volumes/LaCie/Storage/Priority 4/ToBeImportedToMBP Personal PhotoLIbrary/<Device>/<YYYY Month>/"
     for 1-click import into MacBook Pro's "Private" library.
   - Exact Duplicates -> Skipped against media_organizer.db catalog.
4. Auto-creates all required directories under '/Volumes/LaCie/Storage/Priority 4'.
5. Updates database catalog.
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
        DEFAULT_MEDIA_PREPROCESSING_DIR,
        MANAGED_INTAKE_ROOT,
        PRIVATE_STAGING_ROOT
    )
except ImportError:
    MEDIA_ORGANIZER_DB_PATH = BASE_DIR / "db" / "media_organizer.db"
    APPLE_PHOTOS_DB_PATH = Path("/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite")
    DEFAULT_MEDIA_PREPROCESSING_DIR = Path("/Volumes/LaCie/Storage/Priority 4/Media Preprocessing")
    MANAGED_INTAKE_ROOT = Path("/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/ManagedIntake")
    PRIVATE_STAGING_ROOT = Path("/Volumes/LaCie/Storage/Priority 4/ToBeImportedToMBP Personal PhotoLIbrary")

DEFAULT_MANAGED_INTAKE_ROOT = Path(MANAGED_INTAKE_ROOT)
DEFAULT_PRIVATE_STAGING_ROOT = Path(PRIVATE_STAGING_ROOT)

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


def ensure_directories_exist(intake_root: Path, private_root: Path) -> None:
    """Ensures Priority 4 intake and private directories exist, creating them if needed."""
    try:
        intake_root.mkdir(parents=True, exist_ok=True)
        print(f"📁 Verified Intake Staging Folder: {intake_root}")
    except Exception as e:
        print(f"⚠️ Warning: Could not create intake folder {intake_root}: {e}", file=sys.stderr)

    try:
        private_root.mkdir(parents=True, exist_ok=True)
        print(f"📁 Verified Private MBP Staging Folder: {private_root}")
    except Exception as e:
        print(f"⚠️ Warning: Could not create private folder {private_root}: {e}", file=sys.stderr)


def probe_connected_device(timeout_sec: float = 3.0) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Probes for connected iOS / camera devices via ImageCaptureCore.
    Returns: (device_obj, device_name, error_or_lock_message)
    """
    try:
        import objc
        from Foundation import NSRunLoop, NSDate, NSObject
        objc.loadBundle('ImageCaptureCore', globals(), bundle_path='/System/Library/Frameworks/ImageCaptureCore.framework')
    except Exception as e:
        return None, None, f"ImageCaptureCore unavailable ({e})"

    class BrowserDelegate(NSObject):
        def init(self):
            self = objc.super(BrowserDelegate, self).init()
            if self is None:
                return None
            self.device = None
            return self

        def deviceBrowser_didAddDevice_moreComing_(self, browser, dev, more):
            if not self.device:
                self.device = dev

    class DeviceDelegate(NSObject):
        def init(self):
            self = objc.super(DeviceDelegate, self).init()
            if self is None:
                return None
            self.session_opened = False
            self.error = None
            return self

        def device_didOpenSessionWithError_(self, dev, err):
            self.session_opened = (err is None)
            self.error = err

    b_del = BrowserDelegate.alloc().init()
    try:
        from ImageCaptureCore import ICDeviceBrowser  # type: ignore
        browser = ICDeviceBrowser.alloc().init()
    except Exception:
        browser = globals().get('ICDeviceBrowser', None)
        if browser:
            browser = browser.alloc().init()
        else:
            return None, None, "ICDeviceBrowser not found in framework"

    browser.setDelegate_(b_del)
    browser.setBrowsedDeviceTypeMask_(1 | (1 << 8))  # Camera + Mass storage
    browser.start()

    iterations = int(timeout_sec / 0.1)
    for _ in range(iterations):
        NSRunLoop.currentRunLoop().runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))
        if b_del.device:
            break

    dev = b_del.device
    if not dev:
        browser.stop()
        return None, None, None

    dev_name = str(dev.name() or "Connected Device")
    d_del = DeviceDelegate.alloc().init()
    dev.setDelegate_(d_del)
    dev.requestOpenSession()

    for _ in range(25):
        NSRunLoop.currentRunLoop().runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))
        if d_del.error is not None or d_del.session_opened:
            break

    if d_del.error:
        err_msg = str(d_del.error.localizedDescription() or d_del.error)
        return dev, dev_name, err_msg

    return dev, dev_name, None


def download_device_assets_for_month(
    dev: Any,
    target_month: str,
    dest_dir: Path,
    existing_fingerprints: set,
    dry_run: bool = False
) -> int:
    """
    Downloads assets matching target_month from connected device into dest_dir.
    Returns count of downloaded assets.
    """
    try:
        import objc
        from Foundation import NSRunLoop, NSDate, NSObject, NSURL, NSDictionary

        class DownloadDelegate(NSObject):
            def init(self):
                self = objc.super(DownloadDelegate, self).init()
                if self is None:
                    return None
                self.downloaded = False
                self.error = None
                return self

            def didDownloadFile_error_options_contextInfo_(self, file, err, opts, ctx):
                self.downloaded = True
                self.error = err

        dest_dir.mkdir(parents=True, exist_ok=True)
        media_files = dev.mediaFiles() if hasattr(dev, "mediaFiles") else []
        if not media_files:
            return 0

        dest_url = NSURL.fileURLWithPath_isDirectory_(str(dest_dir), True)
        download_opts = NSDictionary.dictionaryWithObjects_forKeys_(
            [dest_url, True],
            ['downloadsDirectoryURL', 'overwrite']
        )

        month_candidates = []
        for f in media_files:
            try:
                fn = str(f.name())
                ext = Path(fn).suffix.lower()
                if ext not in ALL_MEDIA_EXTS:
                    continue

                sz = int(f.fileSize()) if hasattr(f, "fileSize") else 0
                c_date = f.creationDate() if hasattr(f, "creationDate") else None

                file_month = None
                if c_date:
                    ts = c_date.timeIntervalSince1970()
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    file_month = dt.strftime("%Y-%m")

                if target_month != "all" and file_month != target_month:
                    continue

                fn_lower = fn.lower()
                is_dup = (fn_lower, sz) in existing_fingerprints
                if is_dup:
                    continue

                month_candidates.append((f, fn, sz))
            except Exception:
                continue

        print(f"\n📲 Discovered {len(month_candidates)} new assets on device for {target_month}.")
        if dry_run or not month_candidates:
            return len(month_candidates)

        success_count = 0
        for f, fn, sz in month_candidates:
            dest_file = dest_dir / fn
            if dest_file.exists() and dest_file.stat().st_size == sz:
                success_count += 1
                continue

            dl_del = DownloadDelegate.alloc().init()
            dev.requestDownloadFile_options_downloadDelegate_didDownloadSelector_contextInfo_(
                f,
                download_opts,
                dl_del,
                'didDownloadFile:error:options:contextInfo:',
                0
            )

            for _ in range(300):
                NSRunLoop.currentRunLoop().runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))
                if dl_del.downloaded:
                    break

            if dl_del.error:
                print(f"  ⚠️ Error downloading {fn}: {dl_del.error.localizedDescription() or dl_del.error}")
            else:
                success_count += 1
                print(f"  ⬇️ Downloaded: {fn} ({sz / (1024*1024):.1f} MB)")

        return success_count
    except Exception as e:
        print(f"⚠️ Error pulling device media: {e}", file=sys.stderr)
        return 0


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
    - 'private_screenshot'
    - 'private_document'
    - 'main_media'
    """
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
    """Scans a staging folder for target month files."""
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

                fn_lower = f.lower()
                is_dup = (fn_lower, sz) in existing_fingerprints

                cat = "duplicate" if is_dup else classify_asset(p)

                candidates.append({
                    "path": str(p),
                    "filename": f,
                    "size_bytes": sz,
                    "month": file_month,
                    "classification": cat,
                    "target_match": (file_month == target_month or target_month == "all")
                })
            except Exception:
                continue

    return candidates


def run_intake(
    device_name: Optional[str] = None,
    target_month: str = "latest",
    source_dir: Optional[Path] = None,
    intake_root: Path = DEFAULT_MANAGED_INTAKE_ROOT,
    private_staging_root: Path = DEFAULT_PRIVATE_STAGING_ROOT,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Main intake orchestration:
    1. Ensures Priority 4 directories exist.
    2. Probes for connected iOS / camera devices.
    3. Discovers files for target month.
    4. Segregates into Main vs Private Staging vs Duplicates.
    5. Imports Main Media into Apple Photos under:
       'Media Organizer on LaCie/Managed Imports/<device_name>/<YYYY Month>'
    6. Copies Private Media to:
       '<private_staging_root>/<device_name>/<YYYY Month>/'
    """
    month_slug, month_name, album_folder_name = parse_target_month(target_month)
    db_path = Path(MEDIA_ORGANIZER_DB_PATH)

    print("=" * 60)
    print("🚀 Managed Device Intake & Privacy Routing Engine")
    print("=" * 60)
    print(f"📅 Target Month: {month_slug} ({album_folder_name})")
    print(f"📁 Database: {db_path}")

    # Ensure base Priority 4 directories exist
    ensure_directories_exist(intake_root, private_staging_root)

    # 1. Load DB Catalog Index for deduplication
    existing_fingerprints, existing_filenames = get_existing_catalog_index(db_path)
    print(f"📚 Catalog Index Loaded: {len(existing_fingerprints):,} known asset fingerprints.")

    # 2. Device Discovery Probe
    print("\n🔍 Checking connected USB / Wi-Fi devices...")
    dev_obj, detected_device_name, dev_status = probe_connected_device()
    
    if detected_device_name:
        if not device_name or device_name.lower() in ("auto", "detect"):
            if detected_device_name.startswith("iPhone"):
                rest = detected_device_name[6:]
                s = re.sub(r'(\d+)', r' \1 ', rest)
                s = re.sub(r'([A-Z])', r' \1', s)
                device_name = f"iPhone {' '.join(s.split())}"
            else:
                device_name = detected_device_name
        print(f"📱 Detected Connected Device: '{detected_device_name}' (Mapped to: '{device_name}')")
        
        if dev_status:
            print(f"  ⚠️ Device Access State: {dev_status}")
            if "unlock" in dev_status.lower():
                print(f"  💡 Notice: Device is currently locked.")
                print(f"     Please wake the screen of '{detected_device_name}' and enter your passcode.")
        else:
            print(f"  🔓 Device session opened and authorized.")
            if dev_obj and hasattr(dev_obj, "mediaFiles"):
                m_count = len(dev_obj.mediaFiles()) if dev_obj.mediaFiles() else 0
                print(f"  📊 Camera Roll contains {m_count:,} total items.")
                target_stage_dir = intake_root / device_name / album_folder_name
                download_device_assets_for_month(
                    dev=dev_obj,
                    target_month=month_slug,
                    dest_dir=target_stage_dir,
                    existing_fingerprints=existing_fingerprints,
                    dry_run=dry_run
                )
    else:
        if not device_name:
            device_name = "iPhone 17 Pro Max"
        print(f"  ℹ️ No direct camera/iOS device detected on USB/Wi-Fi.")
        print(f"  📂 Using target device profile: '{device_name}'")

    # 3. Determine Intake Staging Location
    if not source_dir:
        dev_month_sub = intake_root / device_name / album_folder_name
        dev_sub = intake_root / device_name
        if dev_month_sub.exists():
            source_dir = dev_month_sub
        elif dev_sub.exists():
            source_dir = dev_sub
        else:
            source_dir = intake_root

    print(f"\n📂 Ingest Source Location: {source_dir}")
    print(f"🔒 Private Staging Root:   {private_staging_root}")

    # 4. Scan Staging Files
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

    if not month_items:
        print("\nℹ️ No assets currently found in staging for this month.")
        print(f"   To import: connect and unlock '{device_name}' or place raw files into:")
        print(f"   '{source_dir}'")
        return report

    # 5. Stage Private Media
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

    # 6. Import Main Media into Apple Photos
    if main_items:
        album_path = f"Media Organizer on LaCie/Managed Imports/{device_name}/{album_folder_name}"
        print(f"\n📷 Importing {len(main_items)} assets into Apple Photos album:")
        print(f"  Target Album: '{album_path}'")

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
    parser.add_argument("--device", default=None, help="Carrier device name (e.g. 'iPhone 17 Pro Max', or auto-detect)")
    parser.add_argument("--month", default="latest", help="Target month in YYYY-MM format, or 'latest'")
    parser.add_argument("--source", type=Path, default=None, help="Source directory containing raw device media")
    parser.add_argument("--intake-root", type=Path, default=DEFAULT_MANAGED_INTAKE_ROOT, help="Base Managed Intake staging directory")
    parser.add_argument("--private-staging", type=Path, default=DEFAULT_PRIVATE_STAGING_ROOT, help="Directory for private assets")
    parser.add_argument("--dry-run", action="store_true", help="Perform triage preview without importing")
    parser.add_argument("--json", action="store_true", help="Output summary in JSON format")

    args = parser.parse_args()
    report = run_intake(
        device_name=args.device,
        target_month=args.month,
        source_dir=args.source,
        intake_root=args.intake_root,
        private_staging_root=args.private_staging,
        dry_run=args.dry_run
    )

    if args.json:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
