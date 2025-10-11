import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mimetypes
import sqlite3
import argparse
import hashlib
from constants import MEDIA_ORGANIZER_DB_PATH, STAGING_ROOT, LOG_PATH
from db.queries import get_planned_month
from db.connections import get_connection, get_cursor, commit, close as close_conn
from utils.logger import setup_logger, compute_file_hash
from google_photos import create_or_get_album, upload_media, human_readable_size, get_google_storage_quota, ensure_google_photos_credentials
# from pull_google_favorites import get_album_id
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import google.auth.exceptions
from datetime import datetime
import logging


#SCOPES = [
#    "https://www.googleapis.com/auth/photoslibrary",
#    "https://www.googleapis.com/auth/photoslibrary.readonly"
#    'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
#]

MODULE_TAG = 'upload_to_google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}

CLIENT_SECRET_FILE = 'secrets/client_secret.json'
SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary.readonly",
]







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

    existing_metadata = {}
    cursor.execute("""
        SELECT original_filename, month, import_id, aesthetic_score, date_created_utc, imported_date_utc
        FROM assets
        WHERE month = ?
    """, (month,))
    for row in cursor.fetchall():
        existing_metadata[(row[0].lower(), row[1])] = {
            "import_id": row[2],
            "aesthetic_score": row[3],
            "original_filename": row[0],
            "date_created_utc": row[4],
            "imported_date_utc": row[5]
        }

    if args.dry_run:
        logger.info("Dry run enabled. Skipping authentication and upload.")
    else:
        service = ensure_google_photos_credentials(SCOPES)
        # Retrieve and log Google storage quota
        quota_info = get_google_storage_quota(service)
        if quota_info:
            used_gb = quota_info["used"] / (1024 ** 3)
            total_gb = quota_info["total"] / (1024 ** 3)
            if quota_info["remaining"] is not None:
                remaining_gb = quota_info["remaining"] / (1024 ** 3)
                logger.info(f"Google Storage Quota: Used {used_gb:.2f} GB / Total {total_gb:.2f} GB / Remaining {remaining_gb:.2f} GB")
            else:
                logger.info(f"Google Storage Quota: Used {used_gb:.2f} GB / Total {total_gb:.2f} GB / Remaining: Unlimited or unknown")

            # Calculate total size of current batch
            batch_size = sum(size for _, size in files)
            batch_size_gb = batch_size / (1024 ** 3)
            logger.info(f"Current batch size: {batch_size_gb:.2f} GB")

            if quota_info["remaining"] is not None and batch_size > quota_info["remaining"]:
                # Sort files by aesthetic_score descending
                files_with_scores = []
                for file_path, file_size in files:
                    filename = os.path.basename(file_path).lower()
                    metadata = existing_metadata.get((filename, month), {})
                    score = metadata.get("aesthetic_score")
                    if score is None:
                        score = -float('inf')  # Treat missing score as lowest
                    files_with_scores.append((file_path, file_size, score))
                files_with_scores.sort(key=lambda x: x[2], reverse=True)

                selected_files = []
                total_selected_size = 0
                remaining_quota = quota_info["remaining"]
                for file_path, file_size, score in files_with_scores:
                    if total_selected_size + file_size <= remaining_quota:
                        selected_files.append((file_path, file_size))
                        total_selected_size += file_size
                    else:
                        break

                if not selected_files:
                    logger.error(f"Not enough space on Google Drive to upload any files. Batch requires {batch_size_gb:.2f} GB but only {remaining_gb:.2f} GB available.")
                    return

                skipped_count = len(files) - len(selected_files)
                logger.info(f"Selected {len(selected_files)} files to upload based on aesthetic score to fit available quota. Skipped {skipped_count} files.")
                files = selected_files

                # Mark batch as partial upload if not all files fit
                if len(files) < len(get_files_to_upload(album_path)):
                    cursor.execute("""
                        UPDATE month_batches
                        SET status_code = '399'
                        WHERE month = ?
                    """, (month,))
                    conn.commit()
                    logger.info(f"Batch {month} marked as partial upload (399) due to Google Drive quota limits.")

        album_title = f"Currently Curating - {month}"
        existing_album_id = create_or_get_album(service, album_title)
        if existing_album_id:
            album_id = existing_album_id
            logger.info(f"Using existing album: {album_title} (ID: {album_id})")
        else:
            album_id = create_or_get_album(service, album_title)
            logger.info(f"Created new album: {album_title} (ID: {album_id})")

    total_files = len(files)
    for idx, (file_path, file_size) in enumerate(files, start=1):
        filename = os.path.basename(file_path)
        file_size_mb = file_size / (1024 * 1024)
        file_hash = compute_file_hash(file_path)

        cursor.execute("""
            SELECT uploaded_to_google FROM assets
            WHERE original_filename = ? AND month = ?
            LIMIT 1
        """, (filename, month))
        row = cursor.fetchone()
        if row and row[0] == 1:
            logger.info(f"[{idx}/{total_files}] Skipping already-uploaded file: {filename} ({file_size_mb:.2f} MB)")
            continue

        normalized_filename = filename.lower()
        metadata = existing_metadata.get((normalized_filename, month), {})
        if not metadata:
            logger.warning(f"⚠️ No metadata found for {filename} in month {month}")

        if args.dry_run:
            logger.info(f"[Dry Run] [{idx}/{total_files}] Would upload: {filename} ({file_size_mb:.2f} MB)")
        else:
            try:
                logger.info(f"[{idx}/{total_files}] Uploading: {filename} ({file_size_mb:.2f} MB)")
                upload_media(service, file_path, album_id)
                logger.info(f"[{idx}/{total_files}] Uploaded: {filename}")
                cursor.execute("""
                    INSERT INTO assets (
                        file_hash, 
                        month, 
                        import_id,
                        aesthetic_score, 
                        original_filename, 
                        date_created_utc, 
                        imported_date_utc, 
                        score_imported_at_utc, 
                        uploaded_to_google, 
                        created_at_utc, 
                        updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 1, datetime('now'), datetime('now'))
                    ON CONFLICT(original_filename, month) DO UPDATE SET
                        file_hash = excluded.file_hash,
                        uploaded_to_google = 1,
                        updated_at_utc = datetime('now')
                """, (
                    file_hash,
                    month,
                    metadata.get("import_id"),
                    metadata.get("aesthetic_score"),
                    metadata.get("original_filename", filename),
                    metadata.get("date_created_utc"),
                    metadata.get("imported_date_utc")
                ))
                conn.commit()
            except Exception as e:
                logger.error(f"[{idx}/{total_files}] Failed to upload {filename}: {e}")

    # Step 4: Mark batch as uploaded - 
    # cursor.execute("""
    #     UPDATE month_batches
    #     SET status_code = 'uploaded'
    #     WHERE month = ?
    # """, (month,))
    conn.commit()
    #conn.close()

    logger.info(f"✅ Upload process completed at {datetime.utcnow().isoformat()}Z")

def parse_args():
    parser = argparse.ArgumentParser(description="Upload media files to Google Photos.")
    parser.add_argument("month", help="Month to process (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Only log actions without uploading files.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args)