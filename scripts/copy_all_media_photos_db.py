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

def perform_direct_copy_and_repair(src_path, dest_path, src_time):
    logging.info("Falling back to direct file copy...")
    try:
        # 1. Direct copy of main DB file
        shutil.copy2(src_path, dest_path)
        logging.info(f"Copied main DB file to {dest_path}")
        
        # 2. Direct copy of WAL and SHM if present
        for suffix in ["-wal", "-shm"]:
            src_file = src_path + suffix
            dest_file = dest_path + suffix
            if os.path.exists(src_file):
                shutil.copy2(src_file, dest_file)
                logging.info(f"Copied {suffix} file to {dest_file}")
            elif os.path.exists(dest_file):
                try:
                    os.remove(dest_file)
                except Exception:
                    pass
    except Exception as e:
        logging.error(f"Failed to copy DB files directly: {e}")
        return False
        
    # 3. Open copied DB, check integrity and repair
    logging.info("Checking integrity of direct copy...")
    conn = None
    try:
        conn = sqlite3.connect(dest_path)
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA integrity_check;")
        errors = [row[0] for row in cursor.fetchall()]
        
        if len(errors) == 1 and errors[0] == "ok":
            logging.info("✅ Direct copy is clean. No repair needed.")
            conn.close()
            return True
            
        logging.warning(f"Integrity check found {len(errors)} issues. Attempting to repair index issues...")
        reindexed = set()
        for err in errors:
            for word in err.split():
                if "INDEX" in word or "idx" in word or word.startswith("ATRANSACTION_Z"):
                    clean_word = word.strip(".,;()\"'")
                    reindexed.add(clean_word)
                    
        if reindexed:
            logging.info(f"Rebuilding {len(reindexed)} indexes: {reindexed}")
            for index in reindexed:
                logging.info(f"Running REINDEX {index}...")
                cursor.execute(f"REINDEX {index};")
            conn.commit()
            
            # Verify again
            logging.info("Verifying integrity post-repair...")
            cursor.execute("PRAGMA integrity_check;")
            post_errors = [row[0] for row in cursor.fetchall()]
            if len(post_errors) == 1 and post_errors[0] == "ok":
                logging.info("✅ Copy is now healthy after REINDEX.")
                conn.close()
                return True
            else:
                logging.error(f"❌ Copy still has integrity issues: {post_errors[:10]}")
                conn.close()
                return False
        else:
            logging.error(f"❌ Direct copy has non-index errors that cannot be repaired automatically: {errors[:10]}")
            conn.close()
            return False
    except Exception as e:
        logging.error(f"Error during integrity check/repair: {e}")
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return False

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
            logging.warning(f"❌ SQLite backup API failed: {e}")
            fallback_success = perform_direct_copy_and_repair(APPLE_PHOTOS_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH, src_time)
            if fallback_success:
                try:
                    os.utime(APPLE_PHOTOS_DB_COPY_PATH, (src_time, src_time))
                except Exception as utime_err:
                    logging.warning(f"Failed to update modification time: {utime_err}")
                write_marker(src_time)
                logging.info("✅ Direct copy and repair complete.")
            else:
                logging.error("❌ Direct copy and repair fallback failed.")
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