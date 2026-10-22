import sqlite3
import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import subprocess
from constants import LOG_PATH
from utils.logger import setup_logger, close_logger
from utils.utils import set_batch_status
from db.connections import get_connection, get_cursor, commit, close as close_conn


MODULE_TAG = 'verify_export_album'

def check_batch_assets_exist(cursor, month):
    """Check that assets exist in the Media Organizer DB for the specified month."""
    cursor.execute('''
        SELECT COUNT(*) FROM assets WHERE month = ?
    ''', (month,))
    count = cursor.fetchone()[0]
    return count

def main_process(logger, month=None, dry_run=False):
    """Main logic to verify assets exist for a batch before export."""
    logger.info("Connecting to Media Organizer DB...")

    if not month:
        logger.error("Month parameter is required.")
        sys.exit(1)

    # Connect to the Media Organizer DB
    conn = get_connection()
    cursor = get_cursor()

    logger.info(f"Next batch is for {month}. Verifying assets in DB...")

    count = check_batch_assets_exist(cursor, month)
    if count > 0:
        logger.info(f"✅ Verified {count} assets exist in Media Organizer DB for batch '{month}'. Ready for direct export.")

        if dry_run:
            logger.info("Dry run enabled.")
        else:
            logger.info("Batch assets verified. Ready for direct file export.")

        conn.commit()
    else:
        logger.error(f"❌ No assets found in Media Organizer DB for month '{month}'.")
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