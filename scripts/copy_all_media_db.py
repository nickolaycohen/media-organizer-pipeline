import os
import shutil
import logging
import sqlite3
from datetime import datetime

SOURCE = "/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite"
DEST = "/Volumes/Macintosh HD/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite"
MARKER = DEST + ".lastcopy"
from constants import MEDIA_ORGANIZER_DB_PATH 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [copy_all_media_db] - %(message)s")

def read_marker():
    if not os.path.exists(MARKER):
        return 0
    with open(MARKER, "r") as f:
        return float(f.read().strip())

def write_marker(src_time):
    with open(MARKER, "w") as f:
        f.write(str(src_time))

def main():
    if not os.path.exists(SOURCE):
        logging.error(f"Source DB not found: {SOURCE}")
        return 1
    if not os.path.exists(os.path.dirname(DEST)):
        logging.error(f"Destination folder missing: {os.path.dirname(DEST)}")
        return 1

    src_time = os.path.getmtime(SOURCE)

    last_copied = read_marker()
    if src_time > last_copied:
        logging.info(f"Copying newer DB from {SOURCE} to {DEST}")
        shutil.copy2(SOURCE, DEST)
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
            """, ("copy_all_media_db", f"Copied from {SOURCE}"))
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