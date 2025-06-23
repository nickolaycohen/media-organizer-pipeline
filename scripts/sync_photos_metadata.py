import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import sqlite3
import logging
from constants import BASE_DIR, MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_PATH, LOG_PATH
from utils.logger import setup_logger, close_logger

MODULE_TAG = 'sync_metadata'

def sync_metadata(logger):
    if not os.path.exists(APPLE_PHOTOS_DB_PATH):
        logger.error(f"Apple Photos database not found at {APPLE_PHOTOS_DB_PATH}")
        return

    conn_media = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor_media = conn_media.cursor()

    try:
        logger.info("Connected to Media Organizer DB.")
        
        # Attach the Apple Photos database
        cursor_media.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_PATH}' AS photos_db;")
        logger.info("Attached Photos.sqlite database.")

        # Drop local copies if they exist
        logger.info("Dropping old local copies of ZASSET, ZIMPORTSESSION and ZADDITIONALASSETATTRIBUTES if exist...")
        cursor_media.execute("DROP TABLE IF EXISTS main.ZASSET;")
        cursor_media.execute("DROP TABLE IF EXISTS main.ZIMPORTSESSION;")
        cursor_media.execute("DROP TABLE IF EXISTS main.ZADDITIONALASSETATTRIBUTES;")
        conn_media.commit()

        # Create fresh local copies
        logger.info("Copying ZASSET from Apple Photos...")
        cursor_media.execute("CREATE TABLE main.ZASSET AS SELECT * FROM photos_db.ZASSET;")
        
        logger.info("Copying ZADDITIONALASSETATTRIBUTES from Apple Photos...")
        cursor_media.execute("CREATE TABLE main.ZADDITIONALASSETATTRIBUTES AS SELECT * FROM photos_db.ZADDITIONALASSETATTRIBUTES;")
        conn_media.commit()

        logger.info("Copied tables successfully.")

        # Drop and recreate the photos_assets_view
        logger.info("Dropping and recreating photos_assets_view...")
        cursor_media.execute("DROP VIEW IF EXISTS main.photos_assets_view;")
        
        cursor_media.execute('''
            CREATE VIEW main.photos_assets_view AS
            SELECT 
                a.ZUUID AS uuid,
                a.ZFILENAME AS filename,
                aaa.ZORIGINALFILENAME AS original_filename,
                a.ZIMPORTSESSION AS import_id,
                datetime((a.ZADDEDDATE + 978303599.796), 'unixepoch', 'localtime') as import_datetime
            FROM main.ZASSET a
            LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK;
        ''')
        conn_media.commit()

        logger.info("View photos_assets_view recreated successfully.")

        # Clear and repopulate smart_albums
        logger.info("Clearing existing smart_albums entries...")
        cursor_media.execute("DELETE FROM smart_albums;")
        
        logger.info("Populating smart_albums from Apple Photos DB...")
        cursor_media.execute('''
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
        conn_media.commit()
        
        logger.info("Smart albums synced successfully.")

        # Clear and repopulate imports table
        logger.info("Clearing existing import session entries...")
        cursor_media.execute("DELETE FROM imports;")

        logger.info("Populating imports table from ZIMPORTSESSION...")
        cursor_media.execute('''
            INSERT INTO imports (import_uuid, import_name, import_timestamp_utc, album, assets_count)
            SELECT
                ZIMPORTSESSION,  -- used as a stand-in for import_uuid
                datetime((z.ZADDEDDATE + 978307200), 'unixepoch') || ' UTC - ' || ea.ZCAMERAMAKE || '-' || ea.ZCAMERAMODEL,
                datetime((z.ZADDEDDATE + 978307200), 'unixepoch'),
                NULL,
                COUNT(z.Z_ENT)
            FROM photos_db.ZASSET z
            LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = z.Z_PK
            WHERE z.ZIMPORTSESSION IS NOT NULL
            GROUP BY z.ZIMPORTSESSION
            ORDER BY z.ZIMPORTSESSION DESC, z.ZADDEDDATE DESC;
        ''')
        conn_media.commit()

        logger.info("Import sessions synced successfully.")

        # Detach Photos DB
        cursor_media.execute("DETACH DATABASE photos_db;")
        logger.info("Detached Photos.sqlite database.")

    except Exception as e:
        logger.error(f"Error during metadata sync: {e}")
        raise
    finally:
        conn_media.close()
        logger.info("Closed connection to Media Organizer DB.")

if __name__ == '__main__':
    logger = setup_logger(LOG_PATH, MODULE_TAG)
    try:
        sync_metadata(logger)
        logger.info("Photos metadata sync completed successfully.")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
    finally:
        close_logger(logger=logger)