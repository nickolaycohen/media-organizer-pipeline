import sqlite3
import os
import sys
from datetime import datetime, timezone
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import logging
from utils.logger import setup_logger, close_logger
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH

MODULE_TAG = 'generate_batches'

def utc_to_local_month(utc_str):
    if not utc_str:
        return None
    dt_utc = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    dt_local = dt_utc.astimezone()
    return dt_local.strftime("%Y-%m")

def get_existing_batches(cursor):
    cursor.execute("SELECT month, status_code FROM month_batches;")
    return {row[0]: row[1] for row in cursor.fetchall()}

def get_available_months(cursor):
    current_month = datetime.now().strftime('%Y-%m')
    cursor.execute('''
        SELECT DISTINCT import_datetime_utc FROM photos_assets_view
        WHERE import_datetime_utc IS NOT NULL
    ''')
    all_imports = cursor.fetchall()
    months = set()
    for (import_utc,) in all_imports:
        local_month = utc_to_local_month(import_utc)
        if local_month and local_month != current_month:
            months.add(local_month)
    return sorted(months)

def create_batch(cursor, month):
    """Insert a new batch for the given month."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        INSERT INTO month_batches (month, batch_number, assets_count, status_code, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (month, 1, 0, '000', now, now))

def merge_batch(cursor, month):
    """Merge new assets from a different device into an existing batch."""
    cursor.execute('''
        UPDATE month_batches 
        SET assets_count = assets_count + (SELECT COUNT(*) FROM photos_assets_view WHERE strftime('%Y-%m', import_datetime) = ?)
        WHERE month = ?
    ''', (month, month))

def mark_batch_complete(cursor, month):
    """Mark the batch as completed when all assets are imported."""
    cursor.execute('''
        UPDATE month_batches 
        SET status = 'completed', updated_at_utc = ?
        WHERE month = ? AND status != 'completed'
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), month))

def main_process(logger):
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    try:
        logger.info("Starting batch generation process...")

        existing_batches = get_existing_batches(cursor)
        available_months = get_available_months(cursor)

        logger.info(f"Available months from assets: {available_months}")
        logger.info(f"Already processed months: {existing_batches}")

        # Fetch all assets with their creation datetime UTC for filtering in Python
        cursor.execute("""
            SELECT uuid, import_id, creation_datetime_utc FROM photos_assets_view
            WHERE creation_datetime_utc IS NOT NULL
        """)
        assets = cursor.fetchall()

        # Build a dict month -> list of (uuid, import_id)
        assets_by_month = {}
        for uuid, import_id, creation_utc in assets:
            month = utc_to_local_month(creation_utc)
            if not month:
                continue
            assets_by_month.setdefault(month, []).append((uuid, import_id))

        for month in available_months:
            month_assets = assets_by_month.get(month, [])
            total_assets_count = len(month_assets)

            # Fetch uploaded assets for this month using the same UTC-to-local-month conversion
            cursor.execute("""
                SELECT asset_id, created_at_utc FROM assets
                WHERE created_at_utc IS NOT NULL
            """)
            uploaded_ids = set()
            for asset_id, creation_utc in cursor.fetchall():
                asset_month = utc_to_local_month(creation_utc)
                if asset_month == month:
                    uploaded_ids.add(asset_id)

            missing_assets = [asset for asset in month_assets if asset[0] not in uploaded_ids]

            logger.info(f"Month {month}: total assets = {total_assets_count}, uploaded assets = {len(uploaded_ids)}")
            count_missing = len(missing_assets)
            if count_missing == 0:
                logger.info(f"✔️ Skipping {month} — all assets already uploaded.")
                continue

            max_display = 10
            logger.info(f"🔍 {count_missing} missing assets detected for {month}.")
            if count_missing > max_display:
                logger.info(f"Displaying first {max_display} missing assets:")
                for uuid, import_id in missing_assets[:max_display]:
                    logger.info(f"  ↪️ Missing asset: UUID={uuid}, Import ID={import_id}")
                logger.info(f"  ... {count_missing - max_display} more missing assets not shown.")
            else:
                for uuid, import_id in missing_assets:
                    logger.info(f"  ↪️ Missing asset: UUID={uuid}, Import ID={import_id}")

            if month not in existing_batches:
                logger.info(f"Creating new batch for completed month {month}")
                create_batch(cursor, month)
            elif existing_batches[month] == 'completed':
                logger.info(f"Merging new assets for completed batch month {month}")
                merge_batch(cursor, month)
            else:
                logger.info(f"Skipping month {month} — batch is still incomplete.")

        conn.commit()
        logger.info("Monthly batches created/updated successfully.")
    except Exception as e:
        logger.error(f"Error during batch generation: {e}")
        raise
    finally:
        conn.close()
        logger.info("Closed connection to Media Organizer DB.")

if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    try:
        main_process(logger)
        logger.info("Script completed successfully.")
    except Exception as e:
        logger.error(f"Script failed: {e}")
    finally:
        close_logger(logger)