import sys
import os
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import sqlite3
import logging
from constants import BASE_DIR, MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_PATH, LOG_PATH, MAX_RETRIES, RETRY_DELAY
from utils.logger import setup_logger, close_logger

MODULE_TAG = 'sync_photos_raw'

def sync_metadata(logger):
    if not os.path.exists(APPLE_PHOTOS_DB_PATH):
        logger.error(f"Apple Photos database not found at {APPLE_PHOTOS_DB_PATH}")
        return

    for attempt in range(1, MAX_RETRIES + 1):
        conn_media = None
        try:
            conn_media = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH, timeout=30, uri=True)
            conn_media.execute("PRAGMA journal_mode=WAL;")
            conn_media.execute("PRAGMA busy_timeout = 30000;")
            cursor_media = conn_media.cursor()

            logger.info(f"Connected to Media Organizer DB (Attempt {attempt}/{MAX_RETRIES}).")

            # Check db_updates.raw_synced flag
            cursor_media.execute("SELECT raw_synced FROM db_updates ORDER BY id DESC LIMIT 1")
            row = cursor_media.fetchone()
            if row and row[0] == 1:
                logger.info("Raw sync flag is already set. Skipping raw assets sync.")
                return

            # Attach the Apple Photos database in read-only mode
            cursor_media.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
            logger.info("Attached Photos.sqlite database read-only.")

            # Verify attached database integrity
            # logger.info("Verifying attached database integrity (this may take a while)...")
            # cursor_media.execute("PRAGMA photos_db.quick_check;")
            # integrity_res = cursor_media.fetchone()
            # if integrity_res and integrity_res[0] != 'ok':
            #     raise sqlite3.DatabaseError(f"Attached Photos DB copy is malformed: {integrity_res[0]}")

            # Refresh local copies of heavy tables.
            for table in ["ZASSET", "ZADDITIONALASSETATTRIBUTES", "ZEXTENDEDATTRIBUTES", "ZIMPORTSESSION"]:
                cursor_media.execute("SELECT name FROM photos_db.sqlite_master WHERE type='table' AND name=?", (table,))
                if cursor_media.fetchone():
                    # Check if local table exists and has Z_PK as primary key
                    cursor_media.execute("SELECT name FROM main.sqlite_master WHERE type='table' AND name=?", (table,))
                    local_exists = cursor_media.fetchone() is not None
                    
                    has_pk = False
                    if local_exists:
                        cursor_media.execute(f"PRAGMA main.table_info({table});")
                        columns = cursor_media.fetchall()
                        for col in columns:
                            # col[1] is name, col[5] is pk flag (1 or 0)
                            if col[1] == "Z_PK" and col[5] == 1:
                                has_pk = True
                                break
                    
                    if not local_exists or not has_pk:
                        logger.info(f"Creating/upgrading local table main.{table} with primary key schema...")
                        # Drop if exists (since it doesn't have PK)
                        cursor_media.execute(f"DROP TABLE IF EXISTS main.{table};")
                        # Get creation SQL from source DB
                        cursor_media.execute("SELECT sql FROM photos_db.sqlite_master WHERE type='table' AND name=?", (table,))
                        create_sql = cursor_media.fetchone()[0]
                        # Execute creation SQL in main
                        cursor_media.execute(create_sql)
                        
                        # Populate table initially
                        logger.info(f"Performing initial full sync for main.{table}...")
                        cursor_media.execute(f"INSERT INTO main.{table} SELECT * FROM photos_db.{table};")
                        conn_media.commit()
                    else:
                        # Table exists and has PK, perform incremental sync (upsert and delete)
                        logger.info(f"Performing incremental sync for main.{table}...")
                        
                        # 1. Upsert new/modified rows
                        cursor_media.execute(f"""
                            INSERT OR REPLACE INTO main.{table}
                            SELECT src.* FROM photos_db.{table} src
                            LEFT JOIN main.{table} dest ON dest.Z_PK = src.Z_PK
                            WHERE dest.Z_PK IS NULL OR src.Z_OPT > dest.Z_OPT;
                        """)
                        inserted_updated = cursor_media.rowcount
                        
                        # 2. Delete removed rows
                        cursor_media.execute(f"""
                            DELETE FROM main.{table}
                            WHERE Z_PK NOT IN (SELECT Z_PK FROM photos_db.{table});
                        """)
                        deleted = cursor_media.rowcount
                        
                        logger.info(f"Incremental sync for {table} completed: upserted {inserted_updated} rows, deleted {deleted} rows.")
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
            return

        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"❌ Error during metadata sync after {MAX_RETRIES} attempts: {e}")
                raise
        finally:
            if conn_media:
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