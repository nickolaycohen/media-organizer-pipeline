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

    # --- Add early integrity check for Media Organizer DB ---
    # cursor.execute("PRAGMA quick_check;")
    # integrity_result = cursor.fetchone()
    # if integrity_result and integrity_result[0] != 'ok':
    #     logger.error(f"❌ Media Organizer DB is malformed: {integrity_result[0]}")
    #     logger.error(f"Location: {DB_PATH}")
    #     logger.error("🚨 Corruption detected! A common fix is to delete this file and let the pipeline recreate it.")
    #     conn.close()
    #     sys.exit(1)
    # logger.info("✅ Media Organizer DB integrity check passed.")

    # Ensure the assets table has the ignore_continuity_check column
    cursor.execute("PRAGMA table_info(assets)")
    columns = [row[1] for row in cursor.fetchall()]
    if "ignore_continuity_check" not in columns:
        logger.info("Adding 'ignore_continuity_check' column to assets table.")
        cursor.execute("ALTER TABLE assets ADD COLUMN ignore_continuity_check INTEGER DEFAULT 0")

    # Drop the old unique index on (original_filename, month) if it exists
    cursor.execute("DROP INDEX IF EXISTS idx_assets_filename_month")
    logger.info("Dropped old unique index 'idx_assets_filename_month' if it existed.")

    # Ensure assets are uniquely identified by their ZUUID to prevent filename collision fighting
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_asset_id ON assets(asset_id)")

    get_migration_status(cursor)
    conn.commit()
    if "--migrate" in sys.argv:
        apply_pending_migrations(cursor, conn)
    conn.close()


if __name__ == "__main__":
    main()