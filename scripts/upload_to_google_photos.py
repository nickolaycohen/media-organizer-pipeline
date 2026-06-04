import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mimetypes
import sqlite3
import argparse
import hashlib
import re
from constants import MEDIA_ORGANIZER_DB_PATH, STAGING_ROOT, LOG_PATH
from db.queries import get_planned_month
from db.connections import get_connection, get_cursor, commit, close as close_conn
from utils.logger import setup_logger
from google_photos import create_or_get_album, upload_media, human_readable_size, check_google_quota, authenticate, GOOGLE_PHOTOS_READONLY_SCOPES, GOOGLE_PHOTOS_APPEND_ONLY_SCOPES, PLANNER_REQUIRED_SCOPES
from datetime import datetime
import logging
from utils.logger import compute_file_hash


MODULE_TAG = 'upload_to_google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}


def get_files_to_upload(folder_path):
    files = []
    for f in os.listdir(folder_path):
        ext = os.path.splitext(f)[1].lower()
        if ext in SUPPORTED_EXTENSIONS:
            full_path = os.path.join(folder_path, f)
            file_size = os.path.getsize(full_path)
            files.append((full_path, file_size))
    return files

def main(args):
    logger.info(f"📤 Starting upload to Google Photos for month: {args.month}")
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))


    month = args.month
    conn = get_connection()
    cursor = get_cursor()


    album_path = os.path.join(STAGING_ROOT, month)
    if not os.path.exists(album_path):
        logger.error(f"Expected folder for batch {month} not found: {album_path}")
        return

    files = get_files_to_upload(album_path)
    if not files:
        logger.warning(f"No supported media files found in {album_path}")
        return

    logger.info(f"Found {len(files)} media files to upload from batch {month}")

    # Store metadata by original_filename. Multiple sources can have the same original_filename.
    # We store them in a list to match against exported files (which might have suffixes like ' 2').
    existing_metadata = {} 
    cursor.execute("""
        SELECT original_filename, month, import_id, aesthetic_score, date_created_utc, imported_date_utc, asset_id, uploaded_to_google
        FROM assets
        WHERE month = ?
    """, (month,))
    for row in cursor.fetchall():
        fname_lower = row[0].lower()
        if fname_lower not in existing_metadata:
            existing_metadata[fname_lower] = []
            
        existing_metadata[fname_lower].append({
            "import_id": row[2],
            "aesthetic_score": row[3],
            "original_filename": row[0],
            "date_created_utc": row[4],
            "imported_date_utc": row[5],
            "asset_id": row[6],
            "uploaded_to_google": row[7],
            "matched": False # internal flag to track matches for disk files with suffixes
        })

    def find_metadata_match(filename):
        # Strip the " 2", " 3" suffix added by Apple Photos export to find the original DB record
        clean_name = re.sub(r'\s\d+(\.[^.]+)$', r'\1', filename.lower())
        for m in existing_metadata.get(clean_name, []):
            if not m['matched']:
                m['matched'] = True
                return m
        return None

    # Filter physical files: ignore those that DB says belong to other months or are already uploaded
    files_to_process = []
    already_uploaded_size = 0
    total_eligible_size = 0
    skipped_count = 0
    for file_path, file_size in files:
        disk_filename = os.path.basename(file_path)
        metadata = find_metadata_match(disk_filename)
        
        if not metadata:
            logger.warning(f"⏭️ Skipping {disk_filename}: Not found in database for month {month}. It may belong to another batch.")
            continue
            
        total_eligible_size += file_size

        if metadata.get("uploaded_to_google") == 1:
            already_uploaded_size += file_size
            skipped_count += 1
            logger.debug(f"⏭️ Skipping {disk_filename}: Already marked as uploaded in database.")
            continue
            
        files_to_process.append((file_path, file_size, metadata))

    if not files_to_process:
        logger.info(f"✅ No new files to upload for month {month}. (Checked {len(files)} files, {skipped_count} already uploaded).")
        return
    else:
        logger.info(f"🔍 Batch Analysis: {len(files_to_process) + skipped_count} total files found. {skipped_count} skipped (already uploaded), {len(files_to_process)} remaining in queue.")

    # Calculate total size of the remaining files
    batch_remaining_size = sum(f[1] for f in files_to_process)

    if args.dry_run:
        logger.info("[Dry Run] Dry run enabled. Skipping authentication and upload.")
    else:
        logger.info(f"Batch {month} Upload Progress: Total: {human_readable_size(total_eligible_size)}, "
                    f"Already Uploaded: {human_readable_size(already_uploaded_size)}, "
                    f"Remaining: {human_readable_size(batch_remaining_size)}")

        # Use the centralized quota check, which handles its own authentication
        remaining_quota_bytes = check_google_quota()
        if remaining_quota_bytes is None:
            logger.error("❌ Aborting: Failed to verify Google Drive quota via API.")
            close_conn()
            sys.exit(1)

        if remaining_quota_bytes is not None and batch_remaining_size > remaining_quota_bytes:
            batch_remaining_gb = batch_remaining_size / (1024 ** 3)
            remaining_quota_gb = remaining_quota_bytes / (1024 ** 3)
            logger.warning(f"Not enough space on Google Drive to upload the remaining files. Need {batch_remaining_gb:.2f} GB but only {remaining_quota_gb:.2f} GB is available.")
            
            # Sort files_to_process by aesthetic_score descending
            files_to_process.sort(key=lambda x: x[2].get("aesthetic_score") or -float('inf'), reverse=True)

            selected_files = []
            total_selected_size = 0
            remaining_quota = remaining_quota_bytes
            for f_item in files_to_process:
                if total_selected_size + f_item[1] <= remaining_quota:
                    selected_files.append(f_item)
                    total_selected_size += f_item[1]
                else:
                    break

            if not selected_files:
                logger.error(f"Not enough space on Google Drive to upload even the smallest file. Aborting upload.")
                return

            skipped_count = len(files_to_process) - len(selected_files)
            logger.info(f"Selected {len(selected_files)} files to upload based on aesthetic score to fit available quota. Skipped {skipped_count} files.")
            files_to_process = selected_files

            # Mark batch as partial upload if not all files fit AND it's not already further along (e.g. 500)
            cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
            current_status_row = cursor.fetchone()
            current_status = current_status_row[0] if current_status_row else '000'
            
            if len(files) < len(files_to_process) and current_status < '400':
                cursor.execute("""
                    UPDATE month_batches
                    SET status_code = '399'
                    WHERE month = ?
                """, (month,))
                conn.commit()
                logger.info(f"Batch {month} status set to partial upload (399).")

        album_title = f"Currently Curating - {month}"
        # Authenticate with a scope that can list albums
        creds_read = authenticate(scopes=GOOGLE_PHOTOS_READONLY_SCOPES)
        album_id = create_or_get_album(creds_read, album_title)

        # Authenticate with append-only scope for uploading
        creds_append = authenticate(scopes=GOOGLE_PHOTOS_APPEND_ONLY_SCOPES)

    total_files = len(files_to_process)
    for idx, (file_path, file_size, metadata) in enumerate(files_to_process, start=1):
        filename = os.path.basename(file_path)
        file_size_mb = file_size / (1024 * 1024)
        file_hash = compute_file_hash(file_path)

        if args.dry_run:
            logger.info(f"[Dry Run] [{idx}/{total_files}] Would upload: {filename} ({file_size_mb:.2f} MB)")
        else:
            try:
                logger.info(f"[{idx}/{total_files}] Uploading: {filename} ({file_size_mb:.2f} MB)")
                upload_media(creds_append, file_path, album_id)
                logger.info(f"[{idx}/{total_files}] Uploaded: {filename}")
                cursor.execute("""
                    UPDATE assets SET
                        file_hash = ?,
                        uploaded_to_google = 1,
                        updated_at_utc = datetime('now')
                    WHERE asset_id = ?
                """, (file_hash, metadata.get("asset_id")))
                conn.commit()
            except Exception as e:
                logger.error(f"[{idx}/{total_files}] Failed to upload {filename}: {e}")
                logger.error("Halting upload process due to error.")
                sys.exit(1)

    # Final check: Verify if the entire DB batch for this month is now uploaded.
    cursor.execute("SELECT COUNT(*) FROM assets WHERE month = ?", (month,))
    total_assets_expected = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM assets WHERE month = ? AND uploaded_to_google = 1", (month,))
    uploaded_count = cursor.fetchone()[0]

    if uploaded_count >= total_assets_expected:
        logger.info(f"🎊 All {total_assets_expected} assets for {month} are verified as uploaded in the database.")
        # Finalize status from partial (399) or error (400E) to complete (400)
        cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
        row = cursor.fetchone()
        if row and row[0] in ['399', '400E']:
            cursor.execute("UPDATE month_batches SET status_code = '400' WHERE month = ?", (month,))
            logger.info(f"✅ Batch {month} status finalized to 400.")
    else:
        logger.info(f"⚠️ Month {month} remains partially uploaded ({uploaded_count}/{total_assets_expected} assets).")

    conn.commit()
    logger.info(f"✅ Upload process completed at {datetime.utcnow().isoformat()}Z")

def parse_args():
    parser = argparse.ArgumentParser(description="Upload media files to Google Photos.")
    parser.add_argument("month", help="Month to process (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Only log actions without uploading files.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args)