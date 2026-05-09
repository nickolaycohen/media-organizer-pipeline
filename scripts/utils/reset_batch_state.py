import sys
import os
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.connections import get_connection, get_cursor, commit, close as close_conn
from utils.logger import setup_logger
from constants import LOG_PATH

MODULE_TAG = 'reset_batch_state'
logger = setup_logger(LOG_PATH, MODULE_TAG)

def reset_month(month):
    conn = get_connection()
    cursor = get_cursor()

    logger.info(f"🔄 Resetting state for batch: {month}")

    # 1. Reset asset flags for this month
    cursor.execute("""
        UPDATE assets 
        SET uploaded_to_google = 0, 
            google_favorite = 0,
            updated_at_utc = datetime('now')
        WHERE month = ?
    """, (month,))
    assets_affected = cursor.rowcount
    logger.info(f"   - Cleared upload/favorite flags for {assets_affected} assets.")

    # 2. Reset the batch status to '210' (Deduplicated/Ready to Upload)
    # This allows the planner to suggest the upload transition (210 -> 399/400)
    cursor.execute("""
        UPDATE month_batches 
        SET status_code = '210', 
            updated_at_utc = datetime('now')
        WHERE month = ?
    """, (month,))
    
    if cursor.rowcount:
        logger.info(f"   - Month {month} status reset to '210' (Ready to Upload).")
        commit()
        logger.info(f"✅ Successfully reset {month}. You can now run the pipeline_planner.")
    else:
        logger.error(f"❌ Month {month} not found in month_batches.")

    close_conn()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset a month batch for re-upload.")
    parser.add_argument("month", help="Month to reset (YYYY-MM)")
    args = parser.parse_args()

    confirm = input(f"This will reset all local tracking for {args.month}, allowing a full re-upload. Continue? [y/N]: ")
    if confirm.lower() == 'y':
        reset_month(args.month)
    else:
        logger.info("Operation aborted.")