import os
import sys
import shutil
import sqlite3
import argparse
import time
import re
import bisect
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
    '.tiff', '.webp', '.3gp', '.bmp'
}

DISCARD_FOLDER_KEYWORDS = {
    'delete', 'deleted', 'trash', 'discard', 'discarded',
    'junk', 'to_delete', 'todelete', 'remove', 'removed',
    'to_remove', 'bad', 'reject', 'rejected', 'temp_delete'
}

WEB_ARTIFACT_EXTS = {'.gif', '.png', '.ico', '.bmp', '.svg', '.webp'}
WEB_ARTIFACT_MAX_SIZE = 50 * 1024  # 50 KB

SEQ_REGEX = re.compile(r'^([a-zA-Z_-]+)(\d+)\.')

def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

def extract_exif_data(file_path):
    """
    Extracts (date_ymd, unix_timestamp) from file EXIF if available.
    """
    if not HAS_EXIFREAD:
        return None, None
    try:
        with open(file_path, 'rb') as f:
            tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)
            dt = tags.get('EXIF DateTimeOriginal') or tags.get('Image DateTime')
            if dt:
                val = str(dt).strip()
                if len(val) >= 19:
                    # Format: "YYYY:MM:DD HH:MM:SS"
                    date_part = val[:10].replace(':', '-')
                    time_part = val[11:19]
                    dt_str = f"{date_part} {time_part}"
                    try:
                        parsed = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        return date_part, parsed.timestamp()
                    except ValueError:
                        return date_part, None
                elif len(val) >= 10:
                    return val[:10].replace(':', '-'), None
    except Exception:
        pass
    return None, None

class LibraryIndex:
    def __init__(self, db_path):
        self.db_path = db_path
        self.size_name_map = {}
        self.size_date_map = {}
        self.date_seq_map = {}   # date_ymd -> set of (prefix_lower, seq_num)
        self.date_ts_map = {}    # date_ymd -> sorted list of unix_ts
        self.removed_assets_set = set() # (size, name_lower) of assets previously marked removed_from_source
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
                datetime(a.ZDATECREATED + 978307200, 'unixepoch') AS date_created,
                (a.ZDATECREATED + 978307200) AS unix_ts
            FROM ZASSET a 
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.total_assets = len(rows)

        for sz, orig_name, cur_name, uuid, dt, unix_ts in rows:
            entry = {
                "uuid": uuid,
                "original_filename": orig_name or cur_name,
                "date_created": dt,
                "unix_ts": unix_ts
            }
            fname = orig_name or cur_name or ''

            if sz:
                if orig_name:
                    self.size_name_map[(sz, orig_name.lower())] = entry
                if cur_name and cur_name != orig_name:
                    self.size_name_map[(sz, cur_name.lower())] = entry
                if dt and len(dt) >= 10:
                    dt_ymd = dt[:10]
                    self.size_date_map[(sz, dt_ymd)] = entry

            # Sequence parsing
            if dt and len(dt) >= 10 and fname:
                ymd = dt[:10]
                m = SEQ_REGEX.match(fname)
                if m:
                    pfx = m.group(1).lower()
                    snum = int(m.group(2))
                    if ymd not in self.date_seq_map:
                        self.date_seq_map[ymd] = set()
                    self.date_seq_map[ymd].add((pfx, snum))

                if unix_ts:
                    if ymd not in self.date_ts_map:
                        self.date_ts_map[ymd] = []
                    self.date_ts_map[ymd].append(unix_ts)

        for ymd in self.date_ts_map:
            self.date_ts_map[ymd].sort()

        # Check assets table for assets previously marked removed_from_source = 1
        try:
            cursor.execute("SELECT original_filename, date_created_utc FROM assets WHERE removed_from_source = 1")
            for r_name, r_dt in cursor.fetchall():
                if r_name:
                    self.removed_assets_set.add(r_name.lower())
        except sqlite3.OperationalError:
            pass

        conn.close()
        load_time = time.time() - t0
        logger.info(f"✅ Loaded {self.total_assets:,} library assets in {load_time:.2f}s.")
        logger.info(f"   - Size+Name Index: {len(self.size_name_map):,} keys")
        logger.info(f"   - Size+Date Index: {len(self.size_date_map):,} keys")
        logger.info(f"   - Dates Indexed for Sequence/Burst Proximity: {len(self.date_seq_map):,} dates")

    def match_duplicate(self, file_path, file_size, filename):
        """
        Tier 1: Check for 100% confirmed identical duplicate in All-Media.
        """
        fname_lower = filename.lower()
        key_name = (file_size, fname_lower)

        # 1. Exact size + filename match
        if key_name in self.size_name_map:
            matched = self.size_name_map[key_name]
            return True, f"Exact size ({format_size(file_size)}) and filename ({matched['original_filename']}) match All-Media asset {matched['uuid']}"

        # 2. Exact size + EXIF date match
        exif_ymd, _ = extract_exif_data(file_path)
        if exif_ymd:
            key_date = (file_size, exif_ymd)
            if key_date in self.size_date_map:
                matched = self.size_date_map[key_date]
                return True, f"Exact size ({format_size(file_size)}) and EXIF date ({exif_ymd}) match All-Media asset {matched['uuid']} ({matched['original_filename']})"

        return False, None

    def match_similar(self, file_path, rel_path, file_size, filename):
        """
        Tier 2: Check if file represents a discarded/similar/burst sibling or web artifact.
        """
        fname_lower = filename.lower()
        rel_lower = rel_path.lower()
        ext = os.path.splitext(fname_lower)[1]

        # 1. Check path keywords (e.g. 'Delete Images 21-23 June 2023')
        path_segments = [seg.strip().lower() for seg in rel_lower.replace('\\', '/').split('/')]
        for seg in path_segments[:-1]: # exclude filename itself
            for kw in DISCARD_FOLDER_KEYWORDS:
                if kw in seg:
                    return True, f"Located inside folder marked for deletion ('{seg}')"

        # 2. Check if previously marked as removed_from_source in DB
        if fname_lower in self.removed_assets_set:
            return True, "Matches an asset previously removed from Apple Photos library (marked removed_from_source in DB)"

        # 3. Check for web / UI graphic artifacts (<50KB non-camera graphics)
        if ext in WEB_ARTIFACT_EXTS and file_size < WEB_ARTIFACT_MAX_SIZE:
            return True, f"Web graphic / UI snippet artifact ({ext}, {format_size(file_size)})"

        # 4. Extract EXIF date & timestamp for sequence / burst checks
        exif_ymd, exif_ts = extract_exif_data(file_path)
        
        # If no EXIF, attempt to parse YYYY-MM-DD from parent path or filename
        if not exif_ymd:
            date_match = re.search(r'20\d{2}[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])', rel_path)
            if date_match:
                exif_ymd = date_match.group(0).replace('_', '-')
            else:
                # E.g. "June 23, 2023"
                date_text_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})', rel_path, re.IGNORECASE)
                if date_text_match:
                    try:
                        m_str, d_str, y_str = date_text_match.groups()
                        dt_parsed = datetime.strptime(f"{m_str} {d_str} {y_str}", "%B %d %Y")
                        exif_ymd = dt_parsed.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        if exif_ymd and exif_ymd in self.date_seq_map:
            # Check camera sequence number proximity on that day
            m = SEQ_REGEX.match(filename)
            if m:
                pfx = m.group(1).lower()
                snum = int(m.group(2))
                existing_nums = [n for p, n in self.date_seq_map[exif_ymd] if p == pfx]
                if existing_nums:
                    min_diff = min(abs(n - snum) for n in existing_nums)
                    if min_diff <= 15: # Sequence sibling within 15 shots on that date
                        closest_num = min(existing_nums, key=lambda n: abs(n - snum))
                        return True, f"Sequence burst sibling on {exif_ymd} ({pfx}{snum} is {min_diff} shots away from existing library asset {pfx}{closest_num})"

        if exif_ymd and exif_ts and exif_ymd in self.date_ts_map:
            # Check temporal burst proximity within 30 seconds
            ts_list = self.date_ts_map[exif_ymd]
            idx = bisect.bisect_left(ts_list, exif_ts)
            candidates_to_check = []
            if idx > 0:
                candidates_to_check.append(ts_list[idx - 1])
            if idx < len(ts_list):
                candidates_to_check.append(ts_list[idx])
            
            for c_ts in candidates_to_check:
                diff = abs(c_ts - exif_ts)
                if diff <= 30.0:
                    return True, f"Temporal burst proximity on {exif_ymd} ({diff:.1f}s away from an existing library photo)"

        return False, None

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
    review_dir = os.path.join(target_dir, "Review-Similar")
    new_dir = os.path.join(target_dir, "New")

    # Load All-Media library catalog
    index = LibraryIndex(MEDIA_ORGANIZER_DB_PATH)
    index.load()

    # Step 1: Discover all media files outside Duplicates/, Review-Similar/, and New/
    candidate_files = []
    ignored_special = {
        "duplicates", "review-similar", "new",
        ".ds_store", "thumbs.db", "$recycle.bin", ".spotlight-v100", ".trashes"
    }

    for root, dirs, files in os.walk(target_dir):
        rel_root = os.path.relpath(root, target_dir)
        first_segment = rel_root.split(os.sep)[0].lower() if rel_root != '.' else ''

        if first_segment in ["duplicates", "review-similar", "new"]:
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

    # Step 3: Evaluate and Triage into 3 Categories
    duplicates_list = []
    review_list = []
    new_list = []

    total_dup_bytes = 0
    total_review_bytes = 0
    total_new_bytes = 0

    for (dirname, stem_lower), file_group in grouped_by_stem.items():
        primary_candidates = [item for item in file_group if item[4] not in ('.mov', '.mp4')]
        video_candidates = [item for item in file_group if item[4] in ('.mov', '.mp4')]

        if primary_candidates:
            p_full, p_rel, p_f, p_sz, p_ext = primary_candidates[0]

            # 1. Check for Duplicate
            is_dup, dup_reason = index.match_duplicate(p_full, p_sz, p_f)
            if is_dup:
                for item in primary_candidates:
                    duplicates_list.append((item[0], item[1], item[2], item[3], dup_reason))
                    total_dup_bytes += item[3]
                for v_item in video_candidates:
                    v_reason = f"Companion Live Photo video to duplicate {p_f}"
                    duplicates_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason))
                    total_dup_bytes += v_item[3]
                continue

            # 2. Check for Review-Similar (Suspected discards / burst siblings / web artifacts)
            is_similar, sim_reason = index.match_similar(p_full, p_rel, p_sz, p_f)
            if is_similar:
                for item in primary_candidates:
                    review_list.append((item[0], item[1], item[2], item[3], sim_reason))
                    total_review_bytes += item[3]
                for v_item in video_candidates:
                    v_reason = f"Companion Live Photo video to review-similar asset {p_f}"
                    review_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason))
                    total_review_bytes += v_item[3]
                continue

            # 3. Otherwise: Confirmed New
            for item in primary_candidates:
                new_list.append((item[0], item[1], item[2], item[3], "Confirmed new asset (not in library, clean sequence)"))
                total_new_bytes += item[3]
            for v_item in video_candidates:
                v_reason = f"Companion Live Photo video to new asset {p_f}"
                new_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason))
                total_new_bytes += v_item[3]

        else:
            # Standalone videos
            for v_item in video_candidates:
                v_full, v_rel, v_f, v_sz, v_ext = v_item
                is_dup, dup_reason = index.match_duplicate(v_full, v_sz, v_f)
                if is_dup:
                    duplicates_list.append((v_full, v_rel, v_f, v_sz, dup_reason))
                    total_dup_bytes += v_sz
                    continue

                is_sim, sim_reason = index.match_similar(v_full, v_rel, v_sz, v_f)
                if is_sim:
                    review_list.append((v_full, v_rel, v_f, v_sz, sim_reason))
                    total_review_bytes += v_sz
                else:
                    new_list.append((v_full, v_rel, v_f, v_sz, "Confirmed new video"))
                    total_new_bytes += v_sz

    # Step 4: Summary Output & Detailed Logging
    print("\n" + "=" * 75)
    print("📊 MEDIA INTAKE 3-WAY TRIAGE SUMMARY")
    print("=" * 75)
    print(f"Target Directory:    {target_dir}")
    print(f"Total Files Scanned: {len(candidate_files):,}")
    print(f"1. Duplicates (Confirmed in All-Media):       {len(duplicates_list):>5,} assets ({format_size(total_dup_bytes):>10})")
    print(f"2. Review-Similar (Suspected Discard/Burst):  {len(review_list):>5,} assets ({format_size(total_review_bytes):>10})")
    print(f"3. New (High-Confidence Clean Assets):        {len(new_list):>5,} assets ({format_size(total_new_bytes):>10})")
    print("=" * 75 + "\n")

    logger.info(f"Triage result: {len(duplicates_list):,} Duplicates ({format_size(total_dup_bytes)}), {len(review_list):,} Review-Similar ({format_size(total_review_bytes)}), {len(new_list):,} New ({format_size(total_new_bytes)})")

    # Log individual files
    for src_full, rel_p, fname, sz, reason in duplicates_list:
        logger.info(f"  [DUPLICATE]      {rel_p} ({format_size(sz)}) -> Duplicates/{rel_p} | Reason: {reason}")
    for src_full, rel_p, fname, sz, reason in review_list:
        logger.info(f"  [REVIEW-SIMILAR] {rel_p} ({format_size(sz)}) -> Review-Similar/{rel_p} | Reason: {reason}")
    for src_full, rel_p, fname, sz, reason in new_list:
        logger.info(f"  [NEW]            {rel_p} ({format_size(sz)}) -> New/{rel_p} | Reason: {reason}")

    if dry_run:
        print(f"🔍 [DRY RUN] Would move:")
        print(f"   - {len(duplicates_list):,} duplicates to 'Duplicates/'")
        print(f"   - {len(review_list):,} suspected discards/bursts to 'Review-Similar/'")
        print(f"   - {len(new_list):,} clean new assets to 'New/'")
        print(f"📝 Logs written to: {TRIAGE_LOG_PATH}")
        return

    # Step 5: Moving files
    logger.info("🚚 Moving triaged files into destination folders...")
    moved_duplicates = 0
    moved_review = 0
    moved_new = 0

    for src_full, rel_p, fname, sz, reason in duplicates_list:
        dest_full = os.path.join(duplicates_dir, rel_p)
        try:
            safe_move(src_full, dest_full)
            moved_duplicates += 1
        except Exception as e:
            logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")

    for src_full, rel_p, fname, sz, reason in review_list:
        dest_full = os.path.join(review_dir, rel_p)
        try:
            safe_move(src_full, dest_full)
            moved_review += 1
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
    logger.info(f"✅ Moved {moved_review:,} review assets to {review_dir}")
    logger.info(f"✅ Moved {moved_new:,} new assets to {new_dir}")

    # Step 6: Clean empty directories left behind
    if clean_empty:
        logger.info("🧹 Cleaning empty source directories...")
        clean_empty_directories(target_dir, protected_subdirs=[duplicates_dir, review_dir, new_dir])

    print(f"✅ Triage Complete!")
    print(f"   - {moved_duplicates:,} duplicates moved to 'Duplicates/'")
    print(f"   - {moved_review:,} review assets moved to 'Review-Similar/'")
    print(f"   - {moved_new:,} clean assets moved to 'New/'")
    print(f"📝 Full audit log: {TRIAGE_LOG_PATH}\n")

def parse_args():
    parser = argparse.ArgumentParser(description="Triage media files in an intake folder into 'Duplicates', 'Review-Similar', and 'New' based on All-Media database.")
    parser.add_argument("target_dir", nargs="?", default=DEFAULT_MEDIA_PREPROCESSING_DIR, help=f"Directory to scan and triage (default: {DEFAULT_MEDIA_PREPROCESSING_DIR})")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the triage process without moving any files.")
    parser.add_argument("--no-clean", action="store_true", help="Do not delete empty directories left behind in the intake folder.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    triage_intake_folder(args.target_dir, dry_run=args.dry_run, clean_empty=not args.no_clean)
