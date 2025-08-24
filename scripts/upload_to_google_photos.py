import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mimetypes
import sqlite3
import argparse
import hashlib
from constants import MEDIA_ORGANIZER_DB_PATH, STAGING_ROOT, LOG_PATH
from db.queries import get_planned_month
from utils.logger import setup_logger, compute_file_hash
from google_photos import create_or_get_album, upload_media
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

MODULE_TAG = 'upload_to_google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}

CLIENT_SECRET_FILE = 'secrets/client_secret.json'
TOKEN_FILE = 'secrets/token.json'
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.appendonly',
    'https://www.googleapis.com/auth/photoslibrary.readonly'
]

def ensure_google_photos_credentials(force_refresh=False):
    creds = None
    if force_refresh and os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        logger.info(f"[Google Photos] New credentials saved to {TOKEN_FILE}")

    logger.info("[Google Photos] Authentication ready")
    return creds

def get_files_to_upload(folder_path):
    files = []
    for f in os.listdir(folder_path):
        ext = os.path.splitext(f)[1].lower()
        if ext in SUPPORTED_EXTENSIONS:
            files.append(os.path.join(folder_path, f))
    return files

def main(args):
    logger.info(f"📤 Starting upload to Google Photos for month: {args.month}")

    month = args.month
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

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
        service = ensure_google_photos_credentials()
        album_title = f"Currently Curating - {month}"
        album_id = create_or_get_album(service, album_title)
        logger.info(f"Using album: {album_title}")

    total_files = len(files)
    for idx, file_path in enumerate(files, start=1):
        filename = os.path.basename(file_path)
        file_hash = compute_file_hash(file_path)

        cursor.execute("""
            SELECT uploaded_to_google FROM assets
            WHERE original_filename = ? AND month = ?
            LIMIT 1
        """, (filename, month))
        row = cursor.fetchone()
        if row and row[0] == 1:
            logger.info(f"[{idx}/{total_files}] Skipping already-uploaded file: {filename}")
            continue

        normalized_filename = filename.lower()
        metadata = existing_metadata.get((normalized_filename, month), {})
        if not metadata:
            logger.warning(f"⚠️ No metadata found for {filename} in month {month}")

        if args.dry_run:
            logger.info(f"[Dry Run] [{idx}/{total_files}] Would upload: {filename}")
        else:
            try:
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
    conn.close()

    from datetime import datetime
    logger.info(f"✅ Upload process completed at {datetime.utcnow().isoformat()}Z")

def parse_args():
    parser = argparse.ArgumentParser(description="Upload media files to Google Photos.")
    parser.add_argument("month", help="Month to process (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Only log actions without uploading files.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args)