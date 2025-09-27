import sqlite3
import os
import sys
from datetime import datetime
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import logging
from utils.logger import setup_logger, close_logger
from constants import LOG_PATH, STAGING_ROOT
from db.connections import get_connection, get_cursor, commit, close as close_conn

MODULE_TAG = 'generate_batches'

def main_process(logger):
    conn = get_connection()
    cursor = get_cursor()

    try:
        logger.info("Starting simplified batch generation process...")

        # Query existing months from month_batches
        cursor.execute("SELECT month FROM month_batches;")
        existing_months = {row[0] for row in cursor.fetchall()}

        # Query months with unprocessed imports (status_code IS NULL) from assets joined with imports, including count of distinct assets
        cursor.execute("""
            SELECT a.month, COUNT(DISTINCT a.asset_id) as asset_count
            FROM assets a
            JOIN imports i ON a.import_id = i.import_uuid
            WHERE i.status_code IS NULL
            GROUP BY a.month
        """)
        unprocessed_months = {row[0]: row[1] for row in cursor.fetchall()}

        for month, count in unprocessed_months.items():
            logger.info(f"Unprocessed month: {month}, distinct imports count: {count}")

        # Determine months needing new batch entries
        new_months = set(unprocessed_months.keys()) - existing_months

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for month in sorted(new_months):
            asset_count = unprocessed_months[month]
            logger.info(f"Inserting batch for month {month} with {asset_count} distinct imports")
            cursor.execute('''
                INSERT INTO month_batches (month, batch_number, assets_count, status_code, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (month, 1, asset_count, '000', now, now))
            logger.info(f"Inserted new batch for month {month}")

        conn.commit()
        logger.info("Batch insertion completed successfully.")
    except Exception as e:
        logger.error(f"Error during batch generation: {e}")
        raise
    finally:
        conn.close()
        logger.info("Closed connection to Media Organizer DB.")

if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

    try:
        main_process(logger)
        logger.info("Script completed successfully.")
    except Exception as e:
        logger.error(f"Script failed: {e}")
    finally:
        close_logger(logger)