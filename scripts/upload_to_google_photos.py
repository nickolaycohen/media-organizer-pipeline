import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mimetypes
import sqlite3
import argparse
import hashlib
import re
import subprocess
from constants import MEDIA_ORGANIZER_DB_PATH, STAGING_ROOT, LOG_PATH, MAX_UPLOAD_FILE_SIZE_MB, MAX_UPLOAD_FILE_SIZE_BYTES, SKIPPED_UPLOADS_ALBUM_NAME
from db.queries import get_planned_month
from db.connections import get_connection, get_cursor, commit, close as close_conn
from utils.logger import setup_logger
from google_photos import create_or_get_album, upload_media, human_readable_size, check_google_quota, authenticate, GOOGLE_PHOTOS_READONLY_SCOPES, GOOGLE_PHOTOS_APPEND_ONLY_SCOPES, PLANNER_REQUIRED_SCOPES
from datetime import datetime, timezone
import logging
from utils.logger import compute_file_hash


MODULE_TAG = 'upload_to_google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}


def add_skipped_assets_to_apple_photos(asset_ids, album_name=SKIPPED_UPLOADS_ALBUM_NAME):
    """Add skipped assets to the dedicated Apple Photos album under 'Media Organizer on LaCie'."""
    if not asset_ids:
        return
    logger.info(f"📸 Adding {len(asset_ids)} skipped asset(s) to Apple Photos album '{album_name}'...")
    safe_album_name = album_name.replace('"', '\\"')
    ids_string = ",".join([f'"{aid}"' for aid in sorted(list(set(asset_ids)))])
    script = f'''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        set targetAlbumName to "{safe_album_name}"
        
        if not (exists folder topFolderName) then
            make new folder named topFolderName
        end if
        set topFolder to folder topFolderName
        
        set targetAlbum to missing value
        if exists album targetAlbumName of topFolder then
            set targetAlbum to album targetAlbumName of topFolder
        else
            repeat with f in folders of topFolder
                if exists album targetAlbumName of f then
                    set targetAlbum to album targetAlbumName of f
                    exit repeat
                end if
            end repeat
        end if
        
        if targetAlbum is missing value then
            make new album named targetAlbumName at topFolder
            set targetAlbum to album targetAlbumName of topFolder
        end if
        
        set assetIds to {{{ids_string}}}
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
    try:
        process = subprocess.Popen(['osascript', '-e', script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        if stderr and "error" in stderr.lower():
            logger.warning(f"AppleScript warning while adding skipped assets: {stderr.strip()}")
        added_count = stdout.strip() if stdout else "0"
        logger.info(f"✅ Added {added_count} skipped asset(s) to '{album_name}' in Apple Photos.")
    except Exception as e:
        logger.warning(f"⚠️ Failed to add skipped assets to Apple Photos album: {e}")

def get_files_to_upload(folder_path):
    files = []
    for f in os.listdir(folder_path):
        ext = os.path.splitext(f)[1].lower()
        if ext in SUPPORTED_EXTENSIONS:
            full_path = os.path.join(folder_path, f)
            file_size = os.path.getsize(full_path)
            files.append((full_path, file_size))
    return files

def calculate_historical_throughput():
    import time
    historical_speed = 0.7 * 1024 * 1024 # default fallback: 0.7 MB/s
    try:
        log_dir = os.path.dirname(LOG_PATH)
        if os.path.exists(log_dir):
            log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.startswith("media_organizer.log")]
            total_history_bytes = 0
            total_history_seconds = 0
            
            for log_path in log_files:
                with open(log_path, 'r', errors='ignore') as lf:
                    lines = lf.readlines()
                
                uploading_events = {}
                for line in lines:
                    if 'Uploading:' in line and 'MB)' in line:
                        match_time = re.match(r'^([\d\-]+\s[\d:,]+)', line)
                        match_file = re.search(r'Uploading:\s+(\S+)\s+\(([\d.]+)\s+MB\)', line)
                        if match_time and match_file:
                            try:
                                t_str = match_time.group(1)
                                dt = datetime.strptime(t_str.split(',')[0], "%Y-%m-%d %H:%M:%S")
                                ms = float(t_str.split(',')[1]) / 1000.0
                                ts = dt.timestamp() + ms
                                fname = match_file.group(1)
                                fsize_mb = float(match_file.group(2))
                                uploading_events[fname] = (ts, fsize_mb)
                            except Exception:
                                continue
                    elif 'Uploaded:' in line:
                        match_time = re.match(r'^([\d\-]+\s[\d:,]+)', line)
                        match_file = re.search(r'Uploaded:\s+(\S+)', line)
                        if match_time and match_file:
                            fname = match_file.group(1)
                            if fname in uploading_events:
                                try:
                                    t_str = match_time.group(1)
                                    dt = datetime.strptime(t_str.split(',')[0], "%Y-%m-%d %H:%M:%S")
                                    ms = float(t_str.split(',')[1]) / 1000.0
                                    ts_end = dt.timestamp() + ms
                                    ts_start, fsize_mb = uploading_events[fname]
                                    duration = ts_end - ts_start
                                    if 0.1 < duration < 300: # filter out outliers
                                        total_history_bytes += fsize_mb * 1024 * 1024
                                        total_history_seconds += duration
                                except Exception:
                                    continue
            if total_history_seconds > 0:
                historical_speed = total_history_bytes / total_history_seconds
    except Exception as le:
        logger.debug(f"Failed to calculate historical speed from logs: {le}")
    return historical_speed

def main(args):
    logger.info(f"📤 Starting upload to Google Photos for month: {args.month}")
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))


    month = args.month
    max_upload_size_mb = getattr(args, 'max_size_mb', MAX_UPLOAD_FILE_SIZE_MB)
    max_upload_size_bytes = max_upload_size_mb * 1024 * 1024
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

    logger.info(f"Found {len(files)} media files to upload from batch {month} (Max size per file: {max_upload_size_mb:.0f} MB)")

    # Store metadata by original_filename. Multiple sources can have the same original_filename.
    # We store them in a list to match against exported files (which might have suffixes like ' 2').
    existing_metadata = {} 
    cursor.execute("""
        SELECT original_filename, month, import_id, aesthetic_score, date_created_utc, imported_date_utc, asset_id, uploaded_to_google, ignore_continuity_check
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
            "ignore_continuity_check": row[8],
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

    # Filter physical files: ignore those that DB says belong to other months or are already uploaded,
    # or that exceed the max upload size threshold.
    files_to_process = []
    already_uploaded_size = 0
    total_eligible_size = 0
    skipped_count = 0
    skipped_oversized_count = 0
    skipped_quota_count = 0
    skipped_oversized_size = 0
    skipped_oversized_asset_ids = []

    for file_path, file_size in files:
        disk_filename = os.path.basename(file_path)
        metadata = find_metadata_match(disk_filename)
        
        if not metadata:
            logger.warning(f"⏭️ Skipping {disk_filename}: Not found in database for month {month}. It may belong to another batch.")
            continue

        if metadata.get("ignore_continuity_check") == 1:
            logger.info(f"⏭️ Skipping {disk_filename}: Marked as unreasonable/ignored in database.")
            skipped_count += 1
            continue

        if file_size > max_upload_size_bytes:
            file_size_mb = file_size / (1024 * 1024)
            logger.info(f"⏭️ Skipping {disk_filename} ({file_size_mb:.2f} MB): Exceeds upload threshold of {max_upload_size_mb:.0f} MB.")
            skipped_oversized_count += 1
            skipped_oversized_size += file_size
            if metadata.get("asset_id"):
                skipped_oversized_asset_ids.append(metadata.get("asset_id"))
            continue
            
        total_eligible_size += file_size

        if metadata.get("uploaded_to_google") == 1:
            already_uploaded_size += file_size
            skipped_count += 1
            logger.debug(f"⏭️ Skipping {disk_filename}: Already marked as uploaded in database.")
            continue
            
        files_to_process.append((file_path, file_size, metadata))

    if skipped_oversized_asset_ids:
        if args.dry_run:
            logger.info(f"[Dry Run] Would add {len(skipped_oversized_asset_ids)} skipped asset(s) to Apple Photos album '{SKIPPED_UPLOADS_ALBUM_NAME}'.")
        else:
            add_skipped_assets_to_apple_photos(skipped_oversized_asset_ids, album_name=SKIPPED_UPLOADS_ALBUM_NAME)

    if not files_to_process:
        logger.info(f"✅ No new files to upload for month {month}. (Checked {len(files)} files, {skipped_count} already uploaded, {skipped_oversized_count} skipped > {max_upload_size_mb:.0f} MB).")
        # Since all eligible files in the staging folder have been processed, we finalize the status to 400.
        cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
        row = cursor.fetchone()
        if row and row[0] < '400':
            cursor.execute("UPDATE month_batches SET status_code = '400' WHERE month = ?", (month,))
            conn.commit()
            logger.info(f"✅ Batch {month} status finalized to 400.")
        return
    else:
        logger.info(f"🔍 Batch Analysis: {len(files_to_process) + skipped_count + skipped_oversized_count} total files found. "
                    f"{skipped_count} skipped (already uploaded), {skipped_oversized_count} skipped (> {max_upload_size_mb:.0f} MB), "
                    f"{len(files_to_process)} remaining in queue.")

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

            skipped_quota_count = len(files_to_process) - len(selected_files)
            logger.info(f"Selected {len(selected_files)} files to upload based on aesthetic score to fit available quota. Skipped {skipped_quota_count} files due to quota.")
            files_to_process = selected_files

            # Mark batch as partial upload if not all files fit AND it's not already further along (e.g. 500)
            cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
            current_status_row = cursor.fetchone()
            current_status = current_status_row[0] if current_status_row else '000'
            
            if skipped_quota_count > 0 and current_status < '400':
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

    import time
    historical_speed = calculate_historical_throughput()
    total_files = len(files_to_process)
    total_remaining_size = sum(f[1] for f in files_to_process)
    uploaded_size_this_session = 0
    session_durations = 0.0

    for idx, (file_path, file_size, metadata) in enumerate(files_to_process, start=1):
        filename = os.path.basename(file_path)
        file_size_mb = file_size / (1024 * 1024)
        file_hash = compute_file_hash(file_path)

        if args.dry_run:
            logger.info(f"[Dry Run] [{idx}/{total_files}] Would upload: {filename} ({file_size_mb:.2f} MB)")
        else:
            try:
                from datetime import timedelta
                # Use current session's speed if we have enough sample size, otherwise historical fallback
                if idx > 3 and session_durations > 0:
                    current_speed = uploaded_size_this_session / session_durations
                else:
                    current_speed = historical_speed

                eta_seconds = total_remaining_size / current_speed if current_speed > 0 else 0
                eta_minutes = eta_seconds / 60
                eta_completion_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_completion_str = eta_completion_time.strftime("%H:%M:%S")
                
                logger.info(f"[{idx}/{total_files}] Uploading: {filename} ({file_size_mb:.2f} MB) - Est. Completion: {eta_completion_str} ({eta_minutes:.1f}m remaining at {human_readable_size(current_speed)}/s)")
                
                t_start = time.time()
                upload_media(creds_append, file_path, album_id)
                duration = time.time() - t_start

                uploaded_size_this_session += file_size
                total_remaining_size -= file_size
                session_durations += duration

                # Recalculate ETA for the remaining files after this file finishes
                if idx >= 3 and session_durations > 0:
                    current_speed = uploaded_size_this_session / session_durations
                else:
                    current_speed = historical_speed

                remaining_eta_seconds = total_remaining_size / current_speed if current_speed > 0 else 0
                remaining_eta_minutes = remaining_eta_seconds / 60
                remaining_completion_time = datetime.now() + timedelta(seconds=remaining_eta_seconds)
                remaining_completion_str = remaining_completion_time.strftime("%H:%M:%S")

                logger.info(f"[{idx}/{total_files}] Uploaded: {filename} - Est. Completion: {remaining_completion_str} ({remaining_eta_minutes:.1f}m remaining)")
                
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

    # Final check: Verify if all eligible assets for this month are now uploaded.
    cursor.execute("SELECT COUNT(*) FROM assets WHERE month = ? AND (ignore_continuity_check = 0 OR ignore_continuity_check IS NULL)", (month,))
    total_assets_expected = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM assets WHERE month = ? AND uploaded_to_google = 1", (month,))
    uploaded_count = cursor.fetchone()[0]

    # If no files were skipped due to quota limits, we can finalize the status to 400
    if skipped_quota_count == 0 and not args.dry_run:
        logger.info(f"🎊 All eligible assets for {month} are verified as uploaded in the database ({uploaded_count} uploaded, {skipped_oversized_count} skipped > {max_upload_size_mb:.0f} MB).")
        # Finalize status to complete (400)
        cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
        row = cursor.fetchone()
        if row and row[0] < '400':
            cursor.execute("UPDATE month_batches SET status_code = '400' WHERE month = ?", (month,))
            logger.info(f"✅ Batch {month} status finalized to 400.")
    else:
        logger.info(f"⚠️ Month {month} remains partially uploaded due to quota limits ({uploaded_count}/{total_assets_expected} assets).")

    conn.commit()
    logger.info(f"✅ Upload process completed at {datetime.now(timezone.utc).isoformat()}")

def parse_args():
    parser = argparse.ArgumentParser(description="Upload media files to Google Photos.")
    parser.add_argument("month", help="Month to process (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Only log actions without uploading files.")
    parser.add_argument("--max-size-mb", type=float, default=MAX_UPLOAD_FILE_SIZE_MB, help=f"Maximum file size in MB to upload to Google Photos (default: {MAX_UPLOAD_FILE_SIZE_MB} MB).")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args)