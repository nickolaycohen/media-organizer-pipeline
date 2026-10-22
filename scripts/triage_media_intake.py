import os
import sys
import shutil
import sqlite3
import argparse
import time
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LOG_PATH,
    DEFAULT_MEDIA_PREPROCESSING_DIR
)
from utils.logger import setup_logger

try:
    import exifread
    HAS_EXIFREAD = True
except ImportError:
    HAS_EXIFREAD = False

MODULE_TAG = "triage_media_intake"
logger = setup_logger(TRIAGE_LOG_PATH, MODULE_TAG)

MEDIA_EXTENSIONS = {
    '.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4', '.m4v',
    '.avi', '.dng', '.cr2', '.cr3', '.raw', '.gif', '.tif',
    '.tiff', '.webp', '.3gp'
}

def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

def extract_exif_date(file_path):
    if not HAS_EXIFREAD:
        return None
    try:
        with open(file_path, 'rb') as f:
            tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)
            dt = tags.get('EXIF DateTimeOriginal') or tags.get('Image DateTime')
            if dt:
                # Format: "YYYY:MM:DD HH:MM:SS" -> "YYYY-MM-DD"
                val = str(dt).strip()
                if len(val) >= 10:
                    return val[:10].replace(':', '-')
    except Exception:
        pass
    return None

class LibraryIndex:
    def __init__(self, db_path):
        self.db_path = db_path
        self.size_name_map = {}
        self.size_date_map = {}
        self.total_assets = 0

    def load(self):
        t0 = time.time()
        logger.info(f"📚 Loading All-Media library catalog from {self.db_path}...")
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Query ZASSET and ZADDITIONALASSETATTRIBUTES for all assets
        query = """
            SELECT 
                aaa.ZORIGINALFILESIZE, 
                aaa.ZORIGINALFILENAME, 
                a.ZFILENAME, 
                a.ZUUID, 
                datetime(a.ZDATECREATED + 978307200, 'unixepoch') AS date_created
            FROM ZASSET a 
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.total_assets = len(rows)

        for sz, orig_name, cur_name, uuid, dt in rows:
            if not sz:
                continue
            entry = {
                "uuid": uuid,
                "original_filename": orig_name or cur_name,
                "date_created": dt
            }
            if orig_name:
                self.size_name_map[(sz, orig_name.lower())] = entry
            if cur_name and cur_name != orig_name:
                self.size_name_map[(sz, cur_name.lower())] = entry
            if dt and len(dt) >= 10:
                dt_ymd = dt[:10]
                self.size_date_map[(sz, dt_ymd)] = entry

        # Also pull from assets table to cover any secondary imports
        cursor.execute("SELECT asset_id, original_filename, date_created_utc FROM assets WHERE original_filename IS NOT NULL")
        for asset_id, orig_filename, dt_utc in cursor.fetchall():
            # If we don't have size for assets table rows, they're typically covered by ZASSET above
            pass

        conn.close()
        load_time = time.time() - t0
        logger.info(f"✅ Loaded {self.total_assets:,} library assets in {load_time:.2f}s.")
        logger.info(f"   - Size+Name Index: {len(self.size_name_map):,} keys")
        logger.info(f"   - Size+Date Index: {len(self.size_date_map):,} keys")

    def match(self, file_path, file_size, filename):
        fname_lower = filename.lower()
        key_name = (file_size, fname_lower)

        # 1. Exact size + filename match
        if key_name in self.size_name_map:
            matched = self.size_name_map[key_name]
            return True, f"Exact size ({format_size(file_size)}) and filename ({matched['original_filename']}) match All-Media asset {matched['uuid']}"

        # 2. Exact size + EXIF date match
        exif_ymd = extract_exif_date(file_path)
        if exif_ymd:
            key_date = (file_size, exif_ymd)
            if key_date in self.size_date_map:
                matched = self.size_date_map[key_date]
                return True, f"Exact size ({format_size(file_size)}) and EXIF date ({exif_ymd}) match All-Media asset {matched['uuid']} ({matched['original_filename']})"

        return False, "No match found in All-Media library"

def safe_move(src, dest):
    dest_dir = os.path.dirname(dest)
    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(dest):
        src_sz = os.path.getsize(src)
        dst_sz = os.path.getsize(dest)
        if src_sz == dst_sz:
            # Identical duplicate already at destination; remove source
            os.remove(src)
            return dest
        # Disambiguate collision
        base, ext = os.path.splitext(dest)
        idx = 1
        while os.path.exists(f"{base}_dup{idx}{ext}"):
            idx += 1
        dest = f"{base}_dup{idx}{ext}"

    shutil.move(src, dest)
    return dest

def clean_empty_directories(root_dir, protected_subdirs):
    protected_paths = {os.path.abspath(p) for p in protected_subdirs}
    for current_root, dirs, files in os.walk(root_dir, topdown=False):
        current_abs = os.path.abspath(current_root)
        if any(current_abs == p or current_abs.startswith(p + os.sep) for p in protected_paths):
            continue
        if current_abs == os.path.abspath(root_dir):
            continue

        try:
            if not os.listdir(current_root):
                os.rmdir(current_root)
                logger.debug(f"🧹 Cleaned empty directory: {current_root}")
        except Exception as e:
            logger.debug(f"Could not remove directory {current_root}: {e}")

def triage_intake_folder(target_dir, dry_run=False, clean_empty=True):
    target_dir = os.path.abspath(target_dir)
    logger.info(f"🚀 Starting Media Intake Triage for: {target_dir}")
    if dry_run:
        logger.info("🔎 DRY RUN MODE: No files will be moved.")

    if not os.path.exists(target_dir):
        logger.error(f"❌ Target intake directory does not exist: {target_dir}")
        return

    duplicates_dir = os.path.join(target_dir, "Duplicates")
    new_dir = os.path.join(target_dir, "New")

    # Load All-Media library catalog
    index = LibraryIndex(MEDIA_ORGANIZER_DB_PATH)
    index.load()

    # Step 1: Discover all media files outside Duplicates/ and New/
    candidate_files = []
    ignored_special = {"duplicates", "new", ".ds_store", "thumbs.db", "$recycle.bin", ".spotlight-v100", ".trashes"}

    for root, dirs, files in os.walk(target_dir):
        rel_root = os.path.relpath(root, target_dir)
        first_segment = rel_root.split(os.sep)[0].lower() if rel_root != '.' else ''

        if first_segment in ["duplicates", "new"]:
            continue

        for f in sorted(files):
            if f.startswith('.') or f.lower() in ignored_special:
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext not in MEDIA_EXTENSIONS:
                continue

            full_p = os.path.join(root, f)
            rel_p = os.path.relpath(full_p, target_dir)
            try:
                sz = os.path.getsize(full_p)
                candidate_files.append((full_p, rel_p, f, sz))
            except OSError as e:
                logger.warning(f"Could not stat {full_p}: {e}")

    logger.info(f"📂 Found {len(candidate_files):,} candidate media files in intake directory.")

    if not candidate_files:
        logger.info("✨ No media files found in intake directory to triage.")
        return

    # Step 2: Live Photo companion grouping
    # Group by directory and base stem (e.g. ('photos/vacation', 'IMG_0001'))
    grouped_by_stem = {}
    for full_p, rel_p, f, sz in candidate_files:
        dirname = os.path.dirname(rel_p)
        stem, ext = os.path.splitext(f)
        key = (dirname, stem.lower())
        if key not in grouped_by_stem:
            grouped_by_stem[key] = []
        grouped_by_stem[key].append((full_p, rel_p, f, sz, ext.lower()))

    # Step 3: Evaluate and Triage
    duplicates_list = []
    new_list = []

    total_dup_bytes = 0
    total_new_bytes = 0

    for (dirname, stem_lower), file_group in grouped_by_stem.items():
        # Separate primary image (e.g. .heic/.jpg) from companion .mov
        primary_candidates = [item for item in file_group if item[4] not in ('.mov', '.mp4')]
        video_candidates = [item for item in file_group if item[4] in ('.mov', '.mp4')]

        # Evaluate primary image if available
        matched_primary = False
        primary_match_reason = ""

        if primary_candidates:
            # Check the primary image
            p_full, p_rel, p_f, p_sz, p_ext = primary_candidates[0]
            matched_primary, primary_match_reason = index.match(p_full, p_sz, p_f)

            if matched_primary:
                for item in primary_candidates:
                    duplicates_list.append((item[0], item[1], item[2], item[3], primary_match_reason))
                    total_dup_bytes += item[3]
            else:
                for item in primary_candidates:
                    new_list.append((item[0], item[1], item[2], item[3], primary_match_reason))
                    total_new_bytes += item[3]

            # Pair companion videos with primary
            for v_item in video_candidates:
                if matched_primary:
                    reason = f"Companion Live Photo video to duplicate {p_f} ({primary_match_reason})"
                    duplicates_list.append((v_item[0], v_item[1], v_item[2], v_item[3], reason))
                    total_dup_bytes += v_item[3]
                else:
                    reason = f"Companion Live Photo video to new asset {p_f}"
                    new_list.append((v_item[0], v_item[1], v_item[2], v_item[3], reason))
                    total_new_bytes += v_item[3]
        else:
            # Only videos in this group (standalone videos)
            for v_item in video_candidates:
                v_full, v_rel, v_f, v_sz, v_ext = v_item
                is_dup, v_reason = index.match(v_full, v_sz, v_f)
                if is_dup:
                    duplicates_list.append((v_full, v_rel, v_f, v_sz, v_reason))
                    total_dup_bytes += v_sz
                else:
                    new_list.append((v_full, v_rel, v_f, v_sz, v_reason))
                    total_new_bytes += v_sz

    # Step 4: Summary Output & Execution
    print("\n" + "=" * 70)
    print("📊 MEDIA INTAKE TRIAGE SUMMARY")
    print("=" * 70)
    print(f"Target Directory:    {target_dir}")
    print(f"Total Files Scanned: {len(candidate_files):,}")
    print(f"Duplicates (in All-Media): {len(duplicates_list):,} assets ({format_size(total_dup_bytes)})")
    print(f"New Assets (Not in Library): {len(new_list):,} assets ({format_size(total_new_bytes)})")
    print("=" * 70 + "\n")

    logger.info(f"Triage result: {len(duplicates_list):,} Duplicates ({format_size(total_dup_bytes)}), {len(new_list):,} New ({format_size(total_new_bytes)})")

    # Log individual files
    for src_full, rel_p, fname, sz, reason in duplicates_list:
        logger.info(f"  [DUPLICATE] {rel_p} ({format_size(sz)}) -> Duplicates/{rel_p} | Reason: {reason}")
    for src_full, rel_p, fname, sz, reason in new_list:
        logger.info(f"  [NEW]       {rel_p} ({format_size(sz)}) -> New/{rel_p} | Reason: {reason}")

    if dry_run:
        print(f"🔍 [DRY RUN] Would move {len(duplicates_list):,} duplicates to 'Duplicates/' and {len(new_list):,} new assets to 'New/'.")
        print(f"Logs written to: {TRIAGE_LOG_PATH}")
        return

    # Step 5: Moving files
    logger.info("🚚 Moving triaged files into destination folders...")
    moved_duplicates = 0
    moved_new = 0

    for src_full, rel_p, fname, sz, reason in duplicates_list:
        dest_full = os.path.join(duplicates_dir, rel_p)
        try:
            safe_move(src_full, dest_full)
            moved_duplicates += 1
        except Exception as e:
            logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")

    for src_full, rel_p, fname, sz, reason in new_list:
        dest_full = os.path.join(new_dir, rel_p)
        try:
            safe_move(src_full, dest_full)
            moved_new += 1
        except Exception as e:
            logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")

    logger.info(f"✅ Moved {moved_duplicates:,} duplicates to {duplicates_dir}")
    logger.info(f"✅ Moved {moved_new:,} new assets to {new_dir}")

    # Step 6: Clean empty directories left behind
    if clean_empty:
        logger.info("🧹 Cleaning empty source directories...")
        clean_empty_directories(target_dir, protected_subdirs=[duplicates_dir, new_dir])

    print(f"✅ Triage Complete! {moved_duplicates:,} duplicates moved to 'Duplicates/', {moved_new:,} new assets moved to 'New/'.")
    print(f"📝 Full audit log: {TRIAGE_LOG_PATH}\n")

def parse_args():
    parser = argparse.ArgumentParser(description="Triage media files in an intake folder into 'Duplicates' and 'New' based on All-Media database.")
    parser.add_argument("target_dir", nargs="?", default=DEFAULT_MEDIA_PREPROCESSING_DIR, help=f"Directory to scan and triage (default: {DEFAULT_MEDIA_PREPROCESSING_DIR})")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the triage process without moving any files.")
    parser.add_argument("--no-clean", action="store_true", help="Do not delete empty directories left behind in the intake folder.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    triage_intake_folder(args.target_dir, dry_run=args.dry_run, clean_empty=not args.no_clean)
