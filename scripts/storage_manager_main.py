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

    # Ensure the assets table has the ignore_continuity_check column
    cursor.execute("PRAGMA table_info(assets)")
    columns = [row[1] for row in cursor.fetchall()]
    if "ignore_continuity_check" not in columns:
        logger.info("Adding 'ignore_continuity_check' column to assets table.")
        cursor.execute("ALTER TABLE assets ADD COLUMN ignore_continuity_check INTEGER DEFAULT 0")

    get_migration_status(cursor)
    conn.commit()
    if "--migrate" in sys.argv:
        apply_pending_migrations(cursor, conn)
    conn.close()


if __name__ == "__main__":
    main()