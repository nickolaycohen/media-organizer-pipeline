import os
import sys
import shutil
import sqlite3
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import STAGING_ROOT, LOG_PATH, MEDIA_ORGANIZER_DB_PATH
from utils.logger import setup_logger

MODULE_TAG = "cleanup_staging_exports"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

def get_folder_size(path):
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
    except OSError:
        pass
    return total

def get_active_planned_months(cursor):
    cursor.execute("SELECT planned_month FROM planned_execution WHERE active = 1")
    return set(r[0] for r in cursor.fetchall())

def get_month_statuses(cursor):
    cursor.execute("SELECT month, status_code FROM month_batches")
    return dict(cursor.fetchall())

def identify_cleanup_candidates(staging_root, target_month=None):
    if not os.path.exists(staging_root):
        logger.error(f"Staging root does not exist: {staging_root}")
        return [], 0

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    active_planned = get_active_planned_months(cursor)
    status_map = get_month_statuses(cursor)
    conn.close()

    candidates = []
    total_bytes = 0

    for entry in sorted(os.listdir(staging_root)):
        full_path = os.path.join(staging_root, entry)
        if not os.path.isdir(full_path):
            continue

        # Ignore special or unmanaged folders
        if entry in ["iPhone16", ".DS_Store", "System Volume Information"]:
            continue

        prefix = entry[:7]
        # Validate prefix format YYYY-MM
        is_ym = len(prefix) == 7 and prefix[:4].isdigit() and prefix[4] == '-' and prefix[5:7].isdigit()

        if target_month:
            if not entry.startswith(target_month):
                continue
        else:
            # Protect active planned batches
            if is_ym and prefix in active_planned:
                logger.info(f"🛡️ Skipping active planned month in staging: {entry} ({prefix})")
                continue

        batch_status = status_map.get(prefix) if is_ym else None

        is_eligible = False
        reason = ""

        if target_month:
            is_eligible = True
            reason = f"Explicitly targeted ({target_month})"
        elif batch_status in ['400', '500', '550', '600', '650']:
            is_eligible = True
            reason = f"Batch status {batch_status} (uploaded / completed)"
        elif '__' in entry or entry.endswith('~') or entry.endswith('-Photos'):
            is_eligible = True
            reason = "Stale temporary or timestamped export folder"

        if is_eligible:
            size = get_folder_size(full_path)
            candidates.append({
                "name": entry,
                "path": full_path,
                "size": size,
                "reason": reason,
                "status": batch_status
            })
            total_bytes += size

    return candidates, total_bytes

def clean_staging(month=None, dry_run=False, auto_confirm=False):
    usage_before = shutil.disk_usage(STAGING_ROOT)
    logger.info(f"Local disk before cleanup: {format_size(usage_before.free)} free / {format_size(usage_before.total)} total")

    candidates, total_bytes = identify_cleanup_candidates(STAGING_ROOT, target_month=month)

    if not candidates:
        logger.info("✨ No staging directories need cleanup.")
        return 0

    print(f"\nFound {len(candidates)} candidate folder(s) eligible for cleanup ({format_size(total_bytes)} total):")
    for c in candidates:
        print(f"  - {c['name']:32} | {format_size(c['size']):>10} | {c['reason']}")

    if dry_run:
        print(f"\n[DRY RUN] Would reclaim {format_size(total_bytes)} across {len(candidates)} folders. No files deleted.")
        return 0

    if not auto_confirm:
        confirm = input(f"\nProceed with deleting {len(candidates)} staging folder(s) to reclaim {format_size(total_bytes)}? [y/N]: ").strip().lower()
        if confirm != 'y':
            print("Operation aborted by user.")
            return 0

    deleted_count = 0
    reclaimed_bytes = 0

    for c in candidates:
        try:
            logger.info(f"🗑️ Deleting staging folder: {c['path']} ({format_size(c['size'])})")
            shutil.rmtree(c['path'])
            deleted_count += 1
            reclaimed_bytes += c['size']
        except Exception as e:
            logger.error(f"❌ Failed to delete {c['path']}: {e}")

    usage_after = shutil.disk_usage(STAGING_ROOT)
    logger.info(f"✅ Reclaimed {format_size(reclaimed_bytes)} across {deleted_count} folder(s).")
    logger.info(f"Local disk after cleanup: {format_size(usage_after.free)} free / {format_size(usage_after.total)} total")
    print(f"\n✅ Successfully cleaned {deleted_count} folder(s). Disk space free: {format_size(usage_after.free)} (reclaimed {format_size(reclaimed_bytes)}).")
    return reclaimed_bytes

def parse_args():
    parser = argparse.ArgumentParser(description="Clean up temporary monthly export staging directories on LaCie.")
    parser.add_argument("--month", help="Target specific month (YYYY-MM) to clean.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate cleanup without deleting any files.")
    parser.add_argument("-y", "--yes", action="store_true", help="Auto-confirm deletion without interactive prompt.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    clean_staging(month=args.month, dry_run=args.dry_run, auto_confirm=args.yes)
