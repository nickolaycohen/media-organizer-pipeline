import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH
from constants import LOG_PATH
from utils.logger import setup_logger
from storage_manager.init_schema import init_schema
from storage_manager.migrations import get_migration_status, apply_pending_migrations

MODULE_TAG = "storage_manager"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def main():
    logger.info(f"🗂  Checking Storage Status at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Early integrity check for Media Organizer DB
    cursor.execute("PRAGMA quick_check;")
    integrity_result = cursor.fetchone()
    if integrity_result and integrity_result[0] != 'ok':
        logger.error(f"❌ Media Organizer DB is malformed: {integrity_result[0]}")
        logger.error(f"Location: {DB_PATH}")
        logger.error("🚨 Corruption detected! A common fix is to delete this file and let the pipeline recreate it.")
        conn.close()
        sys.exit(1)
    logger.info("✅ Media Organizer DB integrity check passed.")

    # Ensure the assets table has the ignore_continuity_check column
    cursor.execute("PRAGMA table_info(assets)")
    columns = [row[1] for row in cursor.fetchall()]
    if "ignore_continuity_check" not in columns:
        logger.info("Adding 'ignore_continuity_check' column to assets table.")
        cursor.execute("ALTER TABLE assets ADD COLUMN ignore_continuity_check INTEGER DEFAULT 0")

    if "MomentsAlbumName" not in columns:
        logger.info("Adding 'MomentsAlbumName' column to assets table.")
        cursor.execute("ALTER TABLE assets ADD COLUMN MomentsAlbumName TEXT")

    # Drop the old unique index on (original_filename, month) if it exists
    cursor.execute("DROP INDEX IF EXISTS idx_assets_filename_month")
    logger.info("Dropped old unique index 'idx_assets_filename_month' if it existed.")

    # Ensure assets are uniquely identified by their ZUUID to prevent filename collision fighting
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_asset_id ON assets(asset_id)")

    # Ensure the transition from favorites_pulled (550) to ranked (600) is defined as a pipeline step
    cursor.execute("""
        INSERT INTO batch_status (code, preceding_code, short_label, full_description, transition_type, script_name, pipeline_stage)
        VALUES ('600', '550', 'ranked', 'Assets ranked and exported for human review', 'pipeline', 'rank_assets_by_score.py {month}', '3.4')
        ON CONFLICT(code) DO UPDATE SET 
            preceding_code = excluded.preceding_code,
            transition_type = excluded.transition_type
    """)

    # Ensure the transition from ranked (600) to cleaned (650) is defined
    cursor.execute("""
        INSERT INTO batch_status (code, preceding_code, short_label, full_description, transition_type, script_name, pipeline_stage)
        VALUES ('650', '600', 'cleaned', 'Manual cleanup of Google Photos assets to free storage', 'pipeline', 'delete_google_assets.py {month}', '4.2')
        ON CONFLICT(code) DO UPDATE SET 
            preceding_code = excluded.preceding_code,
            transition_type = excluded.transition_type
    """)

    get_migration_status(cursor)
    conn.commit()
    if "--migrate" in sys.argv:
        apply_pending_migrations(cursor, conn)
    conn.close()


if __name__ == "__main__":
    main()