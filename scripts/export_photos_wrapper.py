import os
import sqlite3
import argparse
import subprocess
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH
from utils.logger import setup_logger, close_logger
from utils.utils import set_batch_status

MODULE_TAG = 'export_wrapper'

def run_applescript_export(month, logger, dry_run=False):
    script_path = os.path.join(os.path.dirname(__file__), "export_photos_applescript.scpt")
    if dry_run:
        logger.info(f"[Dry Run] Would export album '{month}' using AppleScript.")
        return True
    try:
        logger.info(f"Running AppleScript export for album: {month}")
        subprocess.run(["osascript", script_path, month], check=True)
        logger.info("✅ AppleScript export completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ AppleScript export failed: {e}")
        return False

def main(month, dry_run=False, session_id=None):
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    try:
        if not month:
            logger.error("❌ No month provided. Please specify the month using --month.")
            return

        logger.info(f"📦 Selected batch: {month}")

        current_status = '100'  # Assume correct status is passed via --month param

        # We assume the caller has validated the correct month; this check is no longer needed

        success = run_applescript_export(month, logger, dry_run)

        # if success and not dry_run:
        #     set_batch_status(cursor, month, '200', session_id=session_id)
        #     conn.commit()
        #     logger.info(f"📦 Batch '{month}' status updated to '200' (exported).")

    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        raise
    finally:
        conn.close()
        close_logger(logger)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("month", help="Batch month in YYYY-MM format")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without running AppleScript")
    parser.add_argument("--session-id", help="Pipeline session ID")
    args = parser.parse_args()

    main(month=args.month, dry_run=args.dry_run, session_id=args.session_id)