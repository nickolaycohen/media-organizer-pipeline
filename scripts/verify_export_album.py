import sqlite3
import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import subprocess
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH
from utils.logger import setup_logger, close_logger
from utils.utils import set_batch_status
# from scripts.db.queries import get_month_batch_added as get_next_batch
from db.queries import get_month_batch_added as get_next_batch


MODULE_TAG = 'verify_export_album'

def check_smart_album_exists(cursor, album_name):
    """Check if the Smart Album exists in the Media Organizer DB."""
    cursor.execute('''
        SELECT 1 FROM smart_albums
        WHERE album_name = ?
        LIMIT 1;
    ''', (album_name,))
    
    result = cursor.fetchone()
    return result is not None

def run_applescript_export(album_name, logger):
    """Run AppleScript export if album exists."""
    logger.info(f"Smart Album '{album_name}' found. Starting export...")
    try:
        applescript_path = os.path.join(os.path.dirname(__file__), "export_photos_applescript.scpt")
        subprocess.run([
            "osascript",
            applescript_path,
            album_name
        ], check=True)
        logger.info("✅ AppleScript export completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ AppleScript export failed: {e}")
        sys.exit(1)

def main_process(logger, dry_run=False):
    """Main logic to verify if a Smart Album exists."""
    logger.info("Connecting to Media Organizer DB...")

    # Connect to the Media Organizer DB
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Get the next batch
    next_batch = get_next_batch(cursor)

    if next_batch:
        logger.info(f"Next batch is for {next_batch}. Checking Smart Album...")

        if check_smart_album_exists(cursor, next_batch):
            logger.info(f"✅ Smart Album '{next_batch}' exists in Media Organizer DB.")

            if dry_run:
                logger.info("Dry run enabled.")
            else:
                logger.info("Smart Album verified. No export performed in this step.")

            set_batch_status(cursor, next_batch, '100')
            conn.commit()
        else:
            logger.error(f"❌ Smart Album '{next_batch}' does not exist. Please create it manually in Apple Photos.")
            sys.exit(1)  # Block further processing
    else:
        logger.error("No pending batches found in the database.")
        sys.exit(1)  # Exit if no pending batches

    conn.close()

if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    dry_run = '--dry-run' in sys.argv

    try:
        main_process(logger, dry_run=dry_run)
        logger.info("Smart Album verification completed successfully.")
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        close_logger(logger)