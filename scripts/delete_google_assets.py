import os
import sys
import argparse
import sqlite3

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import LOG_PATH
from db.connections import get_connection, get_cursor, close as close_conn
from scripts.cleanup_staging_exports import clean_staging

MODULE_TAG = "delete_google_assets"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def main(month, manual_delete=False):
    """
    Handles the final batch completion transition (Stage 650).
    
    With 5TB Google storage, assets are retained permanently in Google Photos
    as a complete cloud archive and memory library. This step ensures any
    residual local staging folders on LaCie are purged and finalizes batch status.
    """
    logger.info(f"🏁 Finalizing batch completion (Stage 650) for batch: {month}")

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

        if manual_delete:
            print(f"\n{'='*60}")
            print(f"⚠️  MANUAL DELETION REQUESTED FOR {month} ⚠️")
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
            logger.info(f"✅ User confirmed manual cleanup for {month}.")
        else:
            logger.info(f"☁️ Cloud Archive Mode: Retaining all {month} assets in Google Photos permanently (5TB tier).")

        # Reclaim any residual local staging scratch files
        clean_staging(month=month, auto_confirm=True)

        cursor.execute("UPDATE month_batches SET status_code = '650' WHERE month = ?", (month,))
        conn.commit()
        logger.info(f"✅ Batch {month} finalized to status 650 (Completed).")

    except Exception as e:
        logger.error(f"❌ Completion transition failed for {month}: {e}")
        sys.exit(1)
    finally:
        close_conn()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finalize batch completion (Stage 650).")
    parser.add_argument("month", help="Month batch to complete (YYYY-MM)")
    parser.add_argument("--manual-delete", action="store_true", help="Prompt for manual deletion of Google Photos items (legacy mode).")
    args = parser.parse_args()
    main(args.month, manual_delete=args.manual_delete)