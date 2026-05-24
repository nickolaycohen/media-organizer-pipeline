import os
import shutil
import logging
import sqlite3
from datetime import datetime

from constants import MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH, APPLE_PHOTOS_DB_MARKER

logging.basicConfig(level=logging.INFO, format="%(asctime)s [copy_all_media_photos_db] - %(message)s")

def read_marker():
    if not os.path.exists(APPLE_PHOTOS_DB_MARKER):
        return 0
    with open(APPLE_PHOTOS_DB_MARKER, "r") as f:
        return float(f.read().strip())

def write_marker(src_time):
    with open(APPLE_PHOTOS_DB_MARKER, "w") as f:
        f.write(str(src_time))

def main():
    if not os.path.exists(APPLE_PHOTOS_DB_PATH):
        logging.error(f"Source DB not found: {APPLE_PHOTOS_DB_PATH}")
        return 1
    if not os.path.exists(os.path.dirname(APPLE_PHOTOS_DB_COPY_PATH)):
        logging.error(f"Destination folder missing: {os.path.dirname(APPLE_PHOTOS_DB_COPY_PATH)}")
        return 1

    src_time = os.path.getmtime(APPLE_PHOTOS_DB_PATH)
    # Check the modification time of the existing copy to be more resilient
    dest_time = os.path.getmtime(APPLE_PHOTOS_DB_COPY_PATH) if os.path.exists(APPLE_PHOTOS_DB_COPY_PATH) else 0

    last_copied = read_marker()
    if src_time > last_copied or src_time != dest_time:
        logging.info(f"Copying newer DB from {APPLE_PHOTOS_DB_PATH} to {APPLE_PHOTOS_DB_COPY_PATH}")
        shutil.copy2(APPLE_PHOTOS_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH)
        write_marker(src_time)
        logging.info("✅ Copy complete.")

        # Record the update in the media organizer DB
        conn = None
        try:
            conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS db_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    update_type TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                    notes TEXT
                )
            """)
            cursor.execute("""
                INSERT INTO db_updates (update_type, notes)
                VALUES (?, ?)
            """, ("copy_all_media_photos_db", f"Copied from {APPLE_PHOTOS_DB_PATH}"))
            conn.commit()
            logging.info("📒 Recorded DB update in db_updates table.")
        except Exception as e:
            logging.error(f"Failed to record DB update: {e}")
        finally:
            if conn:
                conn.close()
    else:
        logging.info("No copy needed. Destination DB is up-to-date.")
    return 0

if __name__ == "__main__":
    exit(main())