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

    src_wal_path = APPLE_PHOTOS_DB_PATH + "-wal"
    src_time = os.path.getmtime(APPLE_PHOTOS_DB_PATH)
    if os.path.exists(src_wal_path):
        src_time = max(src_time, os.path.getmtime(src_wal_path))

    # Check the modification time of the existing copy to be more resilient
    dest_time = os.path.getmtime(APPLE_PHOTOS_DB_COPY_PATH) if os.path.exists(APPLE_PHOTOS_DB_COPY_PATH) else 0

    last_copied = read_marker()
    if src_time > last_copied or src_time != dest_time:
        logging.info(f"Copying newer DB from {APPLE_PHOTOS_DB_PATH} to {APPLE_PHOTOS_DB_COPY_PATH} via SQLite backup API...")
        
        # Clean up any stale destination WAL/SHM files to avoid conflict
        for suffix in ["-wal", "-shm"]:
            stale_file = APPLE_PHOTOS_DB_COPY_PATH + suffix
            if os.path.exists(stale_file):
                try:
                    os.remove(stale_file)
                except Exception:
                    pass

        try:
            src_conn = sqlite3.connect(APPLE_PHOTOS_DB_PATH)
            src_conn.execute("PRAGMA busy_timeout = 30000;")
            
            dest_conn = sqlite3.connect(APPLE_PHOTOS_DB_COPY_PATH)
            dest_conn.execute("PRAGMA busy_timeout = 30000;")
            
            with dest_conn:
                src_conn.backup(dest_conn)
                
            dest_conn.close()
            src_conn.close()
            
            # Update modification time to match source
            os.utime(APPLE_PHOTOS_DB_COPY_PATH, (src_time, src_time))
            write_marker(src_time)
            logging.info("✅ Copy complete.")
        except Exception as e:
            logging.error(f"❌ Backup failed: {e}")
            return 1

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