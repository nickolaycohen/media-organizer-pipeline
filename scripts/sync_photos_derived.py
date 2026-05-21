import sys, os
import logging
import sqlite3
import argparse
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

    # --- Schema Migration: Ensure assets table uses asset_id as PRIMARY KEY ---
    media_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='assets'")
    a_row = media_cursor.fetchone()

    media_cursor.execute("PRAGMA table_info(assets)")
    is_asset_id_pk = any(col[1] == 'asset_id' and col[5] == 1 for col in media_cursor.fetchall())

    if a_row and (not is_asset_id_pk or "UNIQUE" in a_row[0]):
        logger.info("Legacy UNIQUE constraint detected in 'assets' table schema. Migrating assets table...")
        
        media_cursor.execute("ALTER TABLE assets RENAME TO assets_old")
        
        media_cursor.execute("""
            CREATE TABLE assets (
                asset_id TEXT PRIMARY KEY,
                original_filename TEXT,
                month TEXT,
                date_created_utc TEXT,
                imported_date_utc TEXT,
                import_id TEXT,
                aesthetic_score REAL,
                google_favorite INTEGER DEFAULT 0,
                ignore_continuity_check INTEGER DEFAULT 0,
                file_hash TEXT,
                uploaded_to_google INTEGER DEFAULT 0,
                score_imported_at_utc TEXT,
                created_at_utc TEXT,
                updated_at_utc TEXT
            )
        """)
        
        # Determine columns that exist in the old table to safely copy data
        media_cursor.execute("PRAGMA table_info(assets_old)")
        existing_cols = {row[1] for row in media_cursor.fetchall()}
        target_cols = [
            "asset_id", "original_filename", "month", "date_created_utc", "imported_date_utc",
            "import_id", "aesthetic_score", "google_favorite", "ignore_continuity_check",
            "file_hash", "uploaded_to_google", "score_imported_at_utc", "created_at_utc", "updated_at_utc"
        ]
        valid_cols = [c for c in target_cols if c in existing_cols]
        col_str = ", ".join(valid_cols)
        
        media_cursor.execute(f"INSERT INTO assets ({col_str}) SELECT {col_str} FROM assets_old")
        media_cursor.execute("DROP TABLE assets_old")
        media_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_asset_id ON assets(asset_id)")
        commit()
        logger.info("Migration of 'assets' table completed successfully.")

    # --- Schema Migration: Ensure imports table is clean of legacy constraints ---
    media_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='imports'")
    i_row = media_cursor.fetchone()

    media_cursor.execute("PRAGMA index_list(imports)")
    has_new_idx = any(idx[1] == 'idx_imports_uuid_model' for idx in media_cursor.fetchall())

    if i_row and (not has_new_idx or "import_timestamp_utc" in i_row[0]):
        logger.info("Legacy UNIQUE constraint detected in 'imports' table. Migrating...")
        media_cursor.execute("ALTER TABLE imports RENAME TO imports_old")
        media_cursor.execute("""
            CREATE TABLE imports (
                import_uuid TEXT,
                import_name TEXT,
                import_timestamp_utc TEXT,
                album TEXT,
                assets_count INTEGER,
                camera_make TEXT,
                camera_model TEXT,
                min_filename TEXT,
                max_filename TEXT,
                min_date TEXT,
                max_date TEXT,
                months_detected TEXT,
                execution_id TEXT,
                status_code TEXT,
                sequencing_confirmed INTEGER DEFAULT 0
            )
        """)
        media_cursor.execute("""
            INSERT INTO imports (
                import_uuid, import_name, import_timestamp_utc, album, assets_count, 
                camera_make, camera_model, min_filename, max_filename, min_date, 
                max_date, months_detected, sequencing_confirmed
            )
            SELECT 
                import_uuid, import_name, import_timestamp_utc, album, assets_count, 
                camera_make, camera_model, min_filename, max_filename, min_date, 
                max_date, months_detected, sequencing_confirmed
            FROM imports_old
        """)
        media_cursor.execute("DROP TABLE imports_old")
        media_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_imports_uuid_model ON imports(import_uuid, camera_model)")
        commit()
        logger.info("Migration of 'imports' table completed successfully.")

    # Create unique index to support UPSERT on (import_uuid, camera_model) if not handled by migration
    media_cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_imports_uuid_model ON imports(import_uuid, camera_model)")

    # We are fetching all assets that exist in ZASSET table,
    # but might not be in assets table and also do not have 
    # a default /Apple Photos/ assigned aestetic score
    # or might have aestetic score reeveluated
    # ideally this should be a refresh of the assets table with 
    # what exists in ZASSET table
    logger.info("Syncing asset metadata (detecting new assets and updating aesthetic scores)...")
    media_cursor.execute("""
        SELECT 
            a.ZUUID, 
            a.ZOVERALLAESTHETICSCORE, 
            aaa.ZORIGINALFILENAME, 
            datetime(a.ZDATECREATED + 978307200, 'unixepoch'),
            datetime(a.ZADDEDDATE + 978307200, 'unixepoch'),
            a.ZIMPORTSESSION as import_id,
            strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) as month,
            COALESCE(ea.ZCAMERAMODEL, 'Unknown') as camera_model
        FROM ZASSET a
        JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
        WHERE a.ZIMPORTSESSION IS NOT NULL
    """)
    results = media_cursor.fetchall()

    logger.info(f"Fetched {len(results)} assets from Photos DB.")

    inserted_count = 0
    assets_to_insert = []

    for row in results:
        asset_id, score, filename, date_created, imported_date, import_id, month, _ = row
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
                score_imported_at_utc,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(asset_id) DO UPDATE SET
                asset_id = excluded.asset_id,
                aesthetic_score = excluded.aesthetic_score,
                date_created_utc = excluded.date_created_utc,
                imported_date_utc = excluded.imported_date_utc,
                import_id = excluded.import_id,
                score_imported_at_utc = datetime('now'),
                updated_at_utc = datetime('now')
            WHERE asset_id IS NOT excluded.asset_id
               OR ROUND(COALESCE(aesthetic_score, -1.0), 6) IS NOT ROUND(COALESCE(excluded.aesthetic_score, -1.0), 6)
               OR COALESCE(CAST(import_id AS TEXT), '') IS NOT COALESCE(CAST(excluded.import_id AS TEXT), '')
               OR COALESCE(date_created_utc, '') IS NOT COALESCE(excluded.date_created_utc, '');
        """, assets_to_insert)
        inserted_count = media_cursor.rowcount
        commit()

    logger.info(f"✅ Inserted or updated {inserted_count} asset records in Media Organizer DB.")

    # Purge assets from local 'assets' table that no longer exist in ZASSET
    logger.info("Purging orphaned assets that no longer exist in Apple Photos...")
    media_cursor.execute("""
        DELETE FROM assets 
        WHERE asset_id NOT IN (SELECT ZUUID FROM photos_db.ZASSET)
    """)
    purged_count = media_cursor.rowcount
    if purged_count > 0:
        logger.info(f"🗑️ Purged {purged_count} orphaned asset records.")
        commit()

    # Purge import sessions from local 'imports' table that no longer have corresponding assets in ZASSET
    logger.info("Purging orphaned import sessions that no longer exist in Apple Photos...")
    media_cursor.execute("""
        DELETE FROM imports
        WHERE (import_uuid, camera_model) NOT IN (
            SELECT DISTINCT
                a.ZIMPORTSESSION,
                COALESCE(ea.ZCAMERAMODEL, 'Unknown')
            FROM photos_db.ZASSET a
            LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
            WHERE a.ZIMPORTSESSION IS NOT NULL
        )
    """)
    purged_imports_count = media_cursor.rowcount
    if purged_imports_count > 0:
        logger.info(f"🗑️ Purged {purged_imports_count} orphaned import session records.")
        commit()

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

    logger.info("Syncing import session records (inserting new and updating existing)...")
    media_cursor.execute('''
        INSERT INTO imports (import_uuid, import_name, import_timestamp_utc, album, assets_count, camera_make, camera_model, min_filename, max_filename, min_date, max_date, months_detected)
        SELECT
            z.ZIMPORTSESSION,
            COALESCE(ea.ZCAMERAMODEL, 'Unknown') || ' (Session ' || z.ZIMPORTSESSION || ')',
            MIN(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
            NULL,
            COUNT(z.Z_ENT),
            COALESCE(ea.ZCAMERAMAKE, 'Unknown'),
            COALESCE(ea.ZCAMERAMODEL, 'Unknown'),
            MIN(aaa.ZORIGINALFILENAME),
            MAX(aaa.ZORIGINALFILENAME),
            MIN(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
            MAX(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
            GROUP_CONCAT(DISTINCT strftime('%Y-%m', datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')))
        FROM photos_db.ZASSET z
        LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = z.Z_PK
        LEFT JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = z.Z_PK
        WHERE z.ZIMPORTSESSION IS NOT NULL
        GROUP BY z.ZIMPORTSESSION, ea.ZCAMERAMAKE, ea.ZCAMERAMODEL
        ORDER BY z.ZIMPORTSESSION DESC
        ON CONFLICT(import_uuid, camera_model) DO UPDATE SET
            assets_count = excluded.assets_count,
            min_filename = excluded.min_filename,
            max_filename = excluded.max_filename,
            min_date = excluded.min_date,
            max_date = excluded.max_date,
            months_detected = excluded.months_detected
        WHERE assets_count != excluded.assets_count 
           OR min_filename != excluded.min_filename 
           OR max_filename != excluded.max_filename
           OR min_date != excluded.min_date
           OR max_date != excluded.max_date
           OR months_detected != excluded.months_detected;
    ''')
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Force sync even if derived_synced flag is set")
    args = parser.parse_args()

    logger = setup_logger(LOG_PATH, MODULE_TAG)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

    conn = get_connection()
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000")
    media_cursor = get_cursor()

    if not args.force:
        media_cursor.execute("SELECT derived_synced FROM db_updates ORDER BY id DESC LIMIT 1")
        row = media_cursor.fetchone()
        if row and row[0] == 1:
            logger.info("Derived sync flag is already set. Skipping derived assets sync (use --force to override).")
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