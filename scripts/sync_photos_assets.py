import sys, os
import logging
import sqlite3
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger, close_logger
from constants import LOG_PATH, APPLE_PHOTOS_DB_PATH, MEDIA_ORGANIZER_DB_PATH

MODULE_TAG = 'sync_photos_assets'

def sync_assets(logger):
    logger.info("Photos assets sync started.")

    media_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    logger.info(f"Connected to Media Organizer DB: {MEDIA_ORGANIZER_DB_PATH}")
    media_cursor = media_conn.cursor()

    logger.info("Fetching existing uploaded or imported assets...")
    media_cursor.execute("SELECT asset_id FROM assets WHERE uploaded_to_google = 1 OR score_imported_at_utc IS NOT NULL")
    uploaded_assets = set(row[0] for row in media_cursor.fetchall())

    media_cursor.execute("SELECT MAX(import_id) FROM assets")
    latest_import_id = media_cursor.fetchone()[0] or 0
    logger.info(f"Latest import session ID in DB: {latest_import_id}")

    logger.info("Fetching assets from Apple Photos DB...")
    media_cursor.execute("""
        SELECT 
            a.ZUUID, 
            a.ZOVERALLAESTHETICSCORE, 
            aaa.ZORIGINALFILENAME, 
            datetime(a.ZDATECREATED + 978307200, 'unixepoch'),
            datetime(a.ZADDEDDATE + 978307200, 'unixepoch'),
            a.ZIMPORTSESSION,
            strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) as month
        FROM ZASSET a
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        WHERE a.ZOVERALLAESTHETICSCORE IS NOT NULL
        AND a.ZIMPORTSESSION > ?
    """, (latest_import_id,))
    results = media_cursor.fetchall()

    logger.info(f"Fetched {len(results)} assets from Photos DB.")

    inserted_count = 0
    skipped_count = 0
    ignored_count = 0
    assets_to_insert = []

    for row in results:
        asset_id, score, filename, date_created, imported_date, import_id, month = row

        # Skip if already uploaded or already imported (score_imported_at_utc is not null)
        if asset_id in uploaded_assets:
            ignored_count += 1
            logger.debug(f"Ignored asset already in DB or uploaded: {asset_id}")
            continue
        
        assets_to_insert.append((asset_id, score, filename, date_created, imported_date, import_id, month))
        inserted_count += 1

        if inserted_count % 10000 == 0:
            percentage = (inserted_count / len(results)) * 100
            logger.info(f"Progress: {inserted_count}/{len(results)} assets inserted ({percentage:.2f}% complete).")

    # Perform bulk insert for remaining assets
    if assets_to_insert:
        media_cursor.executemany("""
            INSERT INTO assets (
                asset_id, 
                aesthetic_score, 
                original_filename, 
                date_created_utc,
                imported_date_utc,
                import_id,
                month
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_filename, month) DO NOTHING
        """, assets_to_insert)

    media_conn.commit()

    logger.info(f"✅ Inserted {inserted_count} new asset records into Media Organizer DB.")
    logger.info(f"ℹ️ Ignored {ignored_count} assets already uploaded or previously imported.")
    logger.info(f"ℹ️ Skipped {skipped_count} assets due to insertion conflict.")

    media_conn.close()
    logger.info("Connection to Media Organizer DB closed.")

if __name__ == "__main__":
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    try:
        sync_assets(logger)
        logger.info("Photos assets sync completed successfully.")
    except Exception as e:
        logger.error(f"Photo assets sync failed: {e}")
    finally:
        close_logger(logger=logger)