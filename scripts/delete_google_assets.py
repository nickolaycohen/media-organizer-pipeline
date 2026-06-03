import os
import sys
import argparse
import sqlite3

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import LOG_PATH
from db.connections import get_connection, get_cursor, close as close_conn

MODULE_TAG = "delete_google_assets"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def main(month):
    """
    Handles the cleanup transition for Google Photos assets.
    
    This script is now strictly for manual intervention. It provides instructions
    for the user to manually delete assets and albums within the Google Photos UI
    to reclaim storage quota, as programmatic deletion is not supported or desired.
    """
    logger.info(f"🧹 Processing cleanup transition (Stage 650) for batch: {month}")

    try:
        conn = get_connection()
        cursor = get_cursor()

        # Verify batch exists in the database
        cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
        row = cursor.fetchone()
        if not row:
            logger.error(f"❌ Batch {month} not found in month_batches.")
            sys.exit(1)
            
        current_status = row[0]
        logger.info(f"Current batch status for {month}: {current_status}")

        print(f"\n{'='*60}")
        print(f"⚠️  MANUAL ACTION REQUIRED FOR {month} ⚠️")
        print(f"{'='*60}")
        print("1. Open Google Photos (web or mobile app).")
        print(f"2. Locate the album: 'Currently Curating - {month}'")
        print("3. Select ALL items in the album and move them to TRASH (Delete from Library).")
        print("4. Empty the Google Photos TRASH/BIN to reclaim storage space.")
        print(f"5. Delete the empty album: 'Currently Curating - {month}'")
        print(f"{'='*60}\n")
        
        confirm = input(f"Have you manually deleted assets for {month} and emptied the trash? [y/N]: ").strip().lower()
        if confirm != 'y':
            logger.error("❌ Cleanup not confirmed by user. Aborting transition.")
            sys.exit(1)
            
        logger.info(f"✅ User confirmed manual cleanup for {month}. Reclaiming status...")

    except Exception as e:
        logger.error(f"❌ Cleanup transition failed for {month}: {e}")
        sys.exit(1)
    finally:
        close_conn()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup transition for Google Photos.")
    parser.add_argument("month", help="Month batch to clean up (YYYY-MM)")
    args = parser.parse_args()
    main(args.month)