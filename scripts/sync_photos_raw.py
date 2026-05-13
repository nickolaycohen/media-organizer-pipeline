import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import sqlite3
import logging
from constants import BASE_DIR, MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH, LOG_PATH
from utils.logger import setup_logger, close_logger

MODULE_TAG = 'sync_photos_raw'

def sync_metadata(logger):
    if not os.path.exists(APPLE_PHOTOS_DB_COPY_PATH):
        logger.error(f"Apple Photos database not found at {APPLE_PHOTOS_DB_COPY_PATH}")
        return

    conn_media = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH, timeout=30)
    conn_media.execute("PRAGMA journal_mode=WAL;")
    conn_media.execute("PRAGMA busy_timeout = 30000;")
    cursor_media = conn_media.cursor()

    try:
        logger.info("Connected to Media Organizer DB.")

        # Check db_updates.raw_synced flag
        cursor_media.execute("SELECT raw_synced FROM db_updates ORDER BY id DESC LIMIT 1")
        row = cursor_media.fetchone()
        if row and row[0] == 1:
            logger.info("Raw sync flag is already set. Skipping raw assets sync.")
            conn_media.close()
            return

        # if not needs_refresh:
        #     logger.info("No new copy_all_media_db update detected since last sync. Skipping metadata sync.")
        #     return

        # Attach the Apple Photos database
        cursor_media.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_COPY_PATH}' AS photos_db;")
        logger.info("Attached Photos.sqlite database.")

        # Refresh local copies of heavy tables. ZIMPORTSESSION is optional in some Photos versions.
        for table in ["ZASSET", "ZADDITIONALASSETATTRIBUTES", "ZEXTENDEDATTRIBUTES", "ZIMPORTSESSION"]:
            cursor_media.execute("SELECT name FROM photos_db.sqlite_master WHERE type='table' AND name=?", (table,))
            if cursor_media.fetchone():
                logger.info(f"Copying {table} from Apple Photos...")
                cursor_media.execute(f"DROP TABLE IF EXISTS main.{table};")
                cursor_media.execute(f"CREATE TABLE main.{table} AS SELECT * FROM photos_db.{table};")
                conn_media.commit()
            else:
                if table == "ZIMPORTSESSION":
                    logger.warning(f"Optional table {table} not found in Apple Photos DB. Skipping.")
                else:
                    raise sqlite3.OperationalError(f"Required table {table} not found in photos_db.")

        logger.info("Copied tables successfully.")

        # Insert sync timestamp into metadata_sync_log
        cursor_media.execute("INSERT INTO metadata_sync_log (synced_at_utc) VALUES (datetime('now'));")
        conn_media.commit()

        # Detach Photos DB
        cursor_media.execute("DETACH DATABASE photos_db;")
        logger.info("Detached Photos.sqlite database.")

        # After successful sync, update the raw_synced flag
        cursor_media.execute("UPDATE db_updates SET raw_synced = 1")
        conn_media.commit()
        logger.info("Raw sync flag updated to 1 after successful metadata sync.")
        # TODO - This sync process should have a schema modification logic to 
        # recognize schema changes on Photos Database and possibly stop any further
        # executions until schema is updated 

    except Exception as e:
        logger.error(f"Error during metadata sync: {e}")
        raise
    finally:
        conn_media.close()
        logger.info("Closed connection to Media Organizer DB.")

if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

    try:
        sync_metadata(logger)
        logger.info("Photos metadata sync completed successfully.")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)
    finally:
        close_logger(logger=logger)