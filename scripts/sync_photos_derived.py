import sys, os
import logging
import sqlite3
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger, close_logger
from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH
from db.connections import get_connection, get_cursor, commit, close as close_conn

MODULE_TAG = 'sync_photos_derived'

def sync_assets(media_cursor, logger):
    logger.info("Photos assets sync started.")

    # media_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    # logger.info(f"Connected to Media Organizer DB: {MEDIA_ORGANIZER_DB_PATH}")
    # media_cursor = media_conn.cursor()

    # Attach the Apple Photos database
    media_cursor.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_COPY_PATH}' AS photos_db;")
    logger.info("Attached Photos.sqlite database.")


    # We are fetching all assets that exist in ZASSET table,
    # but might not be in assets table and also do not have 
    # a default /Apple Photos/ assigned aestetic score
    # or might have aestetic score reeveluated
    # ideally this should be a refresh of the assets table with 
    # what exists in ZASSET table
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
        JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        WHERE a.ZIMPORTSESSION > ?
    """, (latest_import_id,))
    results = media_cursor.fetchall()

    logger.info(f"Fetched {len(results)} assets from Photos DB.")

    inserted_count = 0
    assets_to_insert = []

    for row in results:
        asset_id, score, filename, date_created, imported_date, import_id, month = row
        assets_to_insert.append((asset_id, score, filename, date_created, imported_date, import_id, month))

    if assets_to_insert:
        logger.info(f"Preparing to insert/update {len(assets_to_insert)} asset records...")
        media_cursor.executemany("""
            INSERT INTO assets (
                asset_id, 
                aesthetic_score, 
                original_filename, 
                date_created_utc,
                imported_date_utc,
                import_id,
                month,
                score_imported_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(original_filename, month) DO UPDATE SET
                asset_id = excluded.asset_id,
                aesthetic_score = excluded.aesthetic_score,
                score_imported_at_utc = datetime('now'),
                updated_at_utc = datetime('now')
            WHERE aesthetic_score IS NULL OR aesthetic_score != excluded.aesthetic_score OR asset_id != excluded.asset_id;
        """, assets_to_insert)
        inserted_count = media_cursor.rowcount
        commit()

    logger.info(f"✅ Inserted or updated {inserted_count} asset records in Media Organizer DB.")

    # Clear and repopulate smart_albums
    logger.info("Clearing existing smart_albums entries...")
    media_cursor.execute("DELETE FROM smart_albums;")

    logger.info("Populating smart_albums from Apple Photos DB...")
    media_cursor.execute('''
        INSERT INTO smart_albums (album_pk, album_name, parent_folder_pk, parent_folder_name)
        SELECT 
            a.Z_PK, 
            a.ZTITLE, 
            a.ZPARENTFOLDER, 
            p.ZTITLE
        FROM photos_db.ZGENERICALBUM a
        LEFT JOIN photos_db.ZGENERICALBUM p ON a.ZPARENTFOLDER = p.Z_PK
        WHERE a.ZKIND = 1507
        AND (p.ZTITLE = 'MonthlyExports' OR a.ZPARENTFOLDER = 72235)
        ORDER BY a.ZTITLE;
    ''')

    commit()

    logger.info("Smart albums synced successfully.")

    # Incrementally insert new import session records into imports table
    logger.info("Fetching latest import_uuid from imports table for incremental insert...")
    media_cursor.execute("SELECT MAX(import_uuid) FROM imports")
    max_import_uuid_row = media_cursor.fetchone()
    max_import_uuid = max_import_uuid_row[0] if max_import_uuid_row and max_import_uuid_row[0] is not None else 0

    logger.info(f"Latest import_uuid in imports table: {max_import_uuid}")

    logger.info("Inserting new import session records from ZIMPORTSESSION...")
    media_cursor.execute('''
        INSERT INTO imports (import_uuid, import_name, import_timestamp_utc, album, assets_count)
        SELECT
            ZIMPORTSESSION,  -- used as a stand-in for import_uuid
            datetime((z.ZADDEDDATE + 978307200), 'unixepoch') || ' UTC - ' || ea.ZCAMERAMAKE || '-' || ea.ZCAMERAMODEL,
            datetime((z.ZADDEDDATE + 978307200), 'unixepoch'),
            NULL,
            COUNT(z.Z_ENT)
        FROM photos_db.ZASSET z
        LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = z.Z_PK
        WHERE z.ZIMPORTSESSION IS NOT NULL AND z.ZIMPORTSESSION > ?
        GROUP BY z.ZIMPORTSESSION
        ORDER BY z.ZIMPORTSESSION DESC, z.ZADDEDDATE DESC;
    ''', (max_import_uuid,))
    commit()

    logger.info("New import sessions inserted successfully.")

    # Drop and recreate the photos_assets_view
    logger.info("Dropping and recreating photos_assets_view...")
    media_cursor.execute("DROP VIEW IF EXISTS main.photos_assets_view;")

    media_cursor.execute('''
        CREATE VIEW main.photos_assets_view AS
        SELECT 
            a.ZUUID AS uuid,
            a.ZFILENAME AS filename,
            aaa.ZORIGINALFILENAME AS original_filename,
            a.ZIMPORTSESSION AS import_id,
            datetime(a.ZDATECREATED + 978307200, 'unixepoch') AS creation_datetime_utc,
            datetime(a.ZADDEDDATE + 978307200, 'unixepoch') AS import_datetime_utc
        FROM main.ZASSET a
        LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK;
    ''')
    commit()

    logger.info("View photos_assets_view recreated successfully.")

    # Now update the db_updates.derived_synced flag
    media_cursor.execute("UPDATE db_updates SET derived_synced = 1")
    commit()

if __name__ == "__main__":
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

    # Check db_updates.derived_synced flag before running sync_assets
    # import sqlite3
    conn = get_connection()
    media_cursor = get_cursor()
    media_cursor.execute("SELECT derived_synced FROM db_updates ORDER BY id DESC LIMIT 1")
    row = media_cursor.fetchone()
    
    if row and row[0] == 1:
        logger.info("Derived sync flag is already set. Skipping derived assets sync.")
        close_conn()
        close_logger(logger=logger)
        sys.exit(0)

    try:
        sync_assets(media_cursor, logger)
        logger.info("Photos assets sync completed successfully.")
    except Exception as e:
        logger.error(f"Photo assets sync failed: {e}")
        sys.exit(1)
    finally:
        close_conn()
        close_logger(logger=logger)