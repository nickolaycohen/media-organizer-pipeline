import sqlite3
import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import subprocess
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH
from utils.logger import setup_logger, close_logger
from utils.utils import set_batch_status

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

def main_process(logger, month=None, dry_run=False):
    """Main logic to verify if a Smart Album exists."""
    logger.info("Connecting to Media Organizer DB...")

    if not month:
        logger.error("Month parameter is required.")
        sys.exit(1)

    # Connect to the Media Organizer DB
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    logger.info(f"Next batch is for {month}. Checking Smart Album...")

    if check_smart_album_exists(cursor, month):
        logger.info(f"✅ Smart Album '{month}' exists in Media Organizer DB.")

        if dry_run:
            logger.info("Dry run enabled.")
        else:
            logger.info("Smart Album verified. No export performed in this step.")

        # set_batch_status(cursor, month, '100')
        conn.commit()
    else:
        logger.error(f"❌ Smart Album '{month}' does not exist. Please create it manually in Apple Photos.")
        sys.exit(1)  # Block further processing

    conn.close()
    
if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    dry_run = '--dry-run' in sys.argv
    month = None

    for arg in sys.argv:
        if arg.startswith('--month='):
            month = arg.split('=')[1]
        elif not arg.startswith('--') and os.path.basename(__file__) not in arg:
            month = arg

    try:
        main_process(logger, month=month, dry_run=dry_run)
        logger.info("Smart Album verification completed successfully.")
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        close_logger(logger)