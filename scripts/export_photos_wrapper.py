import os
import glob
import shutil
import sqlite3
import argparse
import time
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH, STAGING_ROOT, APPLE_PHOTOS_DB_PATH
from utils.logger import setup_logger, close_logger
from utils.utils import set_batch_status

MODULE_TAG = 'export_wrapper'

def format_eta(seconds):
    """Formats remaining seconds into a compact, human-readable ETA string (e.g. '45s', '2m 15s', '1h 05m')."""
    if seconds is None or seconds < 0:
        return "--:--"
    s = int(seconds)
    if s >= 3600:
        h = s // 3600
        m = (s % 3600) // 60
        return f"{h}h {m:02d}m"
    elif s >= 60:
        m = s // 60
        sec = s % 60
        return f"{m}m {sec:02d}s"
    else:
        return f"{s}s"

def get_originals_dir():
    photos_library_dir = os.path.dirname(os.path.dirname(APPLE_PHOTOS_DB_PATH))
    return os.path.join(photos_library_dir, "originals")

def locate_source_files(orig_dir, uuid, original_filename):
    """
    Fast O(1) direct stat lookup for the primary original file and companion Live Photo video.
    """
    char = uuid[0].upper()
    sub_dir = os.path.join(orig_dir, char)
    _, ext = os.path.splitext(original_filename)
    ext_clean = ext.lstrip('.').lower()

    # Common extensions to probe via direct stat
    probe_exts = [ext_clean, ext_clean.upper()]
    if ext_clean in ('jpg', 'jpeg'):
        probe_exts.extend(['jpg', 'JPG', 'jpeg', 'JPEG'])
    elif ext_clean == 'heic':
        probe_exts.extend(['heic', 'HEIC'])
    elif ext_clean == 'png':
        probe_exts.extend(['png', 'PNG'])
    elif ext_clean in ('mov', 'mp4'):
        probe_exts.extend(['mov', 'MOV', 'mp4', 'MP4'])

    primary_path = None
    for cand_ext in probe_exts:
        p = os.path.join(sub_dir, f"{uuid}.{cand_ext}")
        if os.path.exists(p):
            primary_path = p
            break

    # Fallback to glob only if direct stat missed
    if not primary_path:
        pattern = os.path.join(sub_dir, f"{uuid}.*")
        matches = [f for f in glob.glob(pattern) if not os.path.basename(f).startswith(f"{uuid}_")]
        if matches:
            primary_path = matches[0]

    # Check for companion Live Photo video (e.g. UUID_3.mov)
    companion_path = None
    if ext_clean in ('heic', 'jpg', 'jpeg', 'png'):
        for comp_name in [f"{uuid}_3.mov", f"{uuid}_3.MOV", f"{uuid}_2.mov"]:
            p = os.path.join(sub_dir, comp_name)
            if os.path.exists(p):
                companion_path = p
                break
        if not companion_path:
            c_matches = glob.glob(os.path.join(sub_dir, f"{uuid}_*.mov"))
            if c_matches:
                companion_path = c_matches[0]

    return primary_path, companion_path

def export_batch_from_db(month, logger, dry_run=False):
    """
    Directly exports original assets and Live Photo companions for a month from
    the Apple Photos Library originals directory to the staging directory using
    metadata from media_organizer.db.
    """
    orig_dir = get_originals_dir()
    if not os.path.isdir(orig_dir):
        logger.error(f"❌ Photos Library originals directory not found at: {orig_dir} (is external drive mounted?)")
        return False

    destination_folder = os.path.join(STAGING_ROOT, month)
    if not dry_run:
        os.makedirs(destination_folder, exist_ok=True)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT asset_id, original_filename, date_created_utc
        FROM assets
        WHERE month = ?
        ORDER BY date_created_utc ASC, asset_id ASC
    """, (month,))
    assets = cursor.fetchall()
    conn.close()

    if not assets:
        logger.error(f"❌ No assets found in Media Organizer DB for month: {month}")
        return False

    total_assets = len(assets)
    logger.info(f"🚀 Starting direct DB export of {total_assets} assets for {month} to {destination_folder}...")
    print(f"🚀 Starting direct DB export of {total_assets} assets for {month} to {destination_folder}...")

    used_names = set()
    if os.path.exists(destination_folder):
        used_names = set(os.listdir(destination_folder))

    exported_primary = 0
    skipped_primary = 0
    exported_companions = 0
    skipped_companions = 0
    missing_files = []
    total_bytes = 0
    start_time = time.time()

    for idx, (uuid, orig_fname, dt) in enumerate(assets, start=1):
        src_primary, src_comp = locate_source_files(orig_dir, uuid, orig_fname)

        if not src_primary:
            logger.warning(f"⚠️ Source file not found for asset {uuid} ({orig_fname}) in {orig_dir}")
            missing_files.append((uuid, orig_fname))
            continue

        src_size = os.path.getsize(src_primary)

        # Resolve duplicate name collisions within the batch
        base, ext = os.path.splitext(orig_fname)
        target_fname = orig_fname
        counter = 1
        while target_fname in used_names and not (
            os.path.exists(os.path.join(destination_folder, target_fname))
            and os.path.getsize(os.path.join(destination_folder, target_fname)) == src_size
        ):
            target_fname = f"{base} ({counter}){ext}"
            counter += 1

        used_names.add(target_fname)
        target_path = os.path.join(destination_folder, target_fname)

        # Copy primary file
        if os.path.exists(target_path) and os.path.getsize(target_path) == src_size:
            skipped_primary += 1
        else:
            if not dry_run:
                shutil.copy2(src_primary, target_path)
            exported_primary += 1
            total_bytes += src_size

        # Copy companion Live Photo video if present
        if src_comp:
            comp_size = os.path.getsize(src_comp)
            target_base, _ = os.path.splitext(target_fname)
            comp_target_fname = f"{target_base}.MOV"
            comp_target_path = os.path.join(destination_folder, comp_target_fname)

            if os.path.exists(comp_target_path) and os.path.getsize(comp_target_path) == comp_size:
                skipped_companions += 1
            else:
                if not dry_run:
                    shutil.copy2(src_comp, comp_target_path)
                exported_companions += 1
                total_bytes += comp_size
            used_names.add(comp_target_fname)

        if idx % 100 == 0 or idx == total_assets:
            elapsed = time.time() - start_time
            rate_mb = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            eta_str = format_eta((total_assets - idx) / (idx / elapsed)) if elapsed > 0 and idx > 0 else "--:--"
            msg = f"  [{idx}/{total_assets}] Processed assets... ({exported_primary} copied, {skipped_primary} cached, {rate_mb:.1f} MB/s | ETA: {eta_str})"
            logger.info(msg)
            print(f"\r{msg}", end="", flush=True)

    print()
    elapsed = time.time() - start_time
    total_mb = total_bytes / (1024 * 1024)
    summary = (
        f"✅ Export completed for {month} in {elapsed:.2f}s:\n"
        f"   • Primary Assets: {exported_primary} copied, {skipped_primary} already cached\n"
        f"   • Live Photo Videos: {exported_companions} copied, {skipped_companions} already cached\n"
        f"   • Total Transferred: {total_mb:.1f} MB ({total_mb / (elapsed if elapsed > 0 else 1):.1f} MB/s)\n"
        f"   • Missing: {len(missing_files)}"
    )
    logger.info(summary)
    print(summary)

    if missing_files:
        logger.warning(f"⚠️ {len(missing_files)} file(s) could not be located in originals/.")
        return False

    return True

def main(month, dry_run=False, session_id=None):
    logger = setup_logger(LOG_PATH, MODULE_TAG)

    try:
        if not month:
            logger.error("❌ No month provided. Please specify the month using month argument.")
            sys.exit(1)

        logger.info(f"📦 Selected batch for direct DB export: {month}")

        success = export_batch_from_db(month, logger, dry_run)
        if not success:
            logger.error("❌ Direct DB export failed. Exiting with error.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Unexpected error in direct DB export: {e}")
        raise
    finally:
        close_logger(logger)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Directly export month batch from Media Organizer DB to staging.")
    parser.add_argument("month", help="Batch month in YYYY-MM format")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without copying files")
    parser.add_argument("--session-id", help="Pipeline session ID")
    args = parser.parse_args()

    main(month=args.month, dry_run=args.dry_run, session_id=args.session_id)