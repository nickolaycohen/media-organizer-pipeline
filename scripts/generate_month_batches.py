import sqlite3
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import logging
from datetime import datetime
from utils.logger import setup_logger, close_logger
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH

MODULE_TAG = 'generate_batches'

def get_existing_batches(cursor):
    """Retrieve months already present in month_batches."""
    cursor.execute("SELECT month, status_code FROM month_batches;")
    return {row[0]: row[1] for row in cursor.fetchall()}

def get_available_months(cursor):
    """Extract available months from assets view, only include completed months and exclude the current month."""
    current_month = datetime.now().strftime('%Y-%m')  # Get current month
    cursor.execute('''
        SELECT DISTINCT strftime('%Y-%m', import_datetime) as month
        FROM photos_assets_view
        GROUP BY month
        HAVING MAX(import_datetime) < datetime('now', 'start of day')  -- Ensure it's a completed month
        AND strftime('%Y-%m', import_datetime) != ?  -- Exclude the current month
        ORDER BY month ASC;
    ''', (current_month,))
    return [row[0] for row in cursor.fetchall()]

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

        for month in available_months:
            if month not in existing_batches:
                logger.info(f"Creating new batch for completed month {month}")
                create_batch(cursor, month)
            elif existing_batches[month] == 'completed':
                logger.info(f"Merging new assets for completed batch month {month}")
                merge_batch(cursor, month)
            else:
                logger.info(f"Skipping month {month} — batch is still incomplete.")

        # Do not mark as completed here anymore
        # After processing all months, marking of batch completion will be done in the final step of the pipeline
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