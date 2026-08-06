import os
import shutil
import logging
import sqlite3
import sys
import subprocess
import time
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

def perform_direct_copy_and_repair(dest_path):
    logging.info("Checking integrity of database copy...")
    conn = None
    try:
        conn = sqlite3.connect(dest_path)
        conn.execute("PRAGMA mmap_size = 0;")
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA quick_check;")
        errors = [row[0] for row in cursor.fetchall()]
        
        if len(errors) == 1 and errors[0] == "ok":
            logging.info("✅ Copy is clean. No repair needed.")
            conn.close()
            return True
            
        logging.warning(f"Integrity check found {len(errors)} issues. Attempting to repair index issues...")
        reindexed = set()
        has_non_index_error = False
        for err in errors:
            is_index_error = False
            for word in err.split():
                if "INDEX" in word or "idx" in word or word.startswith("ATRANSACTION_Z"):
                    clean_word = word.strip(".,;()\"'")
                    reindexed.add(clean_word)
                    is_index_error = True
            if not is_index_error:
                has_non_index_error = True
                     
        if reindexed and not has_non_index_error:
            logging.info(f"Rebuilding {len(reindexed)} indexes: {reindexed}")
            for index in reindexed:
                logging.info(f"Running REINDEX {index}...")
                cursor.execute(f"REINDEX {index};")
            conn.commit()
            
            # Verify again
            logging.info("Verifying integrity post-repair...")
            cursor.execute("PRAGMA quick_check;")
            post_errors = [row[0] for row in cursor.fetchall()]
            if len(post_errors) == 1 and post_errors[0] == "ok":
                logging.info("✅ Copy is now healthy after REINDEX.")
                conn.close()
                return True
            else:
                logging.error(f"❌ Copy still has integrity issues: {post_errors[:10]}")
                conn.close()
        else:
            logging.error(f"❌ Copy has non-index/malformed errors: {errors[:10]}")
            conn.close()
            
    except Exception as e:
        logging.error(f"Error during integrity check/repair: {e}")
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # If we get here, the copy is malformed. Let's attempt automatic recovery using sqlite3 .recover
    logging.info("Attempting automatic database recovery using sqlite3 .recover...")
    recovered_path = dest_path + ".recovered"
    if os.path.exists(recovered_path):
        try:
            os.remove(recovered_path)
        except Exception:
            pass

    try:
        # Run sqlite3 dest_path ".recover" | sqlite3 recovered_path
        p1 = subprocess.Popen(["sqlite3", dest_path, ".recover"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p2 = subprocess.Popen(["sqlite3", recovered_path], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p1.stdout.close() # Allow p1 to receive a SIGPIPE if p2 exits
        stdout2, stderr2 = p2.communicate()
        stderr1 = p1.communicate()[1]

        if p1.returncode != 0 or p2.returncode != 0:
            logging.error(f"SQLite recovery failed: recover return code {p1.returncode} (stderr: {stderr1}), import return code {p2.returncode} (stderr: {stderr2})")
            return False

        if not os.path.exists(recovered_path) or os.path.getsize(recovered_path) == 0:
            logging.error("SQLite recovery produced an empty or missing database file.")
            return False

        # Verify integrity of recovered DB
        logging.info("Verifying integrity of recovered database...")
        conn = sqlite3.connect(recovered_path)
        conn.execute("PRAGMA mmap_size = 0;")
        cursor = conn.cursor()
        cursor.execute("PRAGMA quick_check;")
        rec_errors = [row[0] for row in cursor.fetchall()]
        conn.close()

        if len(rec_errors) == 1 and rec_errors[0] == "ok":
            logging.info("✅ Recovered database is healthy. Replacing original copy...")
            
            # Clean up the original corrupt DB and any associated WAL/SHM files
            for suffix in ["", "-wal", "-shm"]:
                f_to_remove = dest_path + suffix
                if os.path.exists(f_to_remove):
                    try:
                        os.remove(f_to_remove)
                    except Exception as err:
                        logging.warning(f"Could not remove stale file {f_to_remove}: {err}")
            
            # Move recovered DB to dest_path
            shutil.move(recovered_path, dest_path)
            logging.info("✅ Successfully replaced copied database with recovered database.")
            return True
        else:
            logging.error(f"❌ Recovered database still has integrity issues: {rec_errors[:10]}")
            return False

    except Exception as e:
        logging.error(f"Failed to perform sqlite3 recovery: {e}")
        if os.path.exists(recovered_path):
            try:
                os.remove(recovered_path)
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
    dest_exists = os.path.exists(APPLE_PHOTOS_DB_COPY_PATH)
    dest_time = os.path.getmtime(APPLE_PHOTOS_DB_COPY_PATH) if dest_exists else 0

    last_copied = read_marker()
    
    # We use a 2.0-second tolerance threshold for timestamp comparison to account
    # for different filesystem precisions (e.g. FAT32/exFAT vs APFS) and float representation.
    copy_needed = (
        not dest_exists
        or last_copied == 0
        or src_time > (last_copied + 2.0)
        or abs(src_time - dest_time) > 2.0
    )

    if copy_needed:
        max_copy_attempts = 3
        copy_success = False
        
        for attempt in range(1, max_copy_attempts + 1):
            logging.info(f"Copying DB from {APPLE_PHOTOS_DB_PATH} to {APPLE_PHOTOS_DB_COPY_PATH} (Attempt {attempt}/{max_copy_attempts})...")
            
            # Clean up any stale destination files to avoid conflict
            for suffix in ["", "-wal", "-shm"]:
                stale_file = APPLE_PHOTOS_DB_COPY_PATH + suffix
                if os.path.exists(stale_file):
                    try:
                        os.remove(stale_file)
                    except Exception:
                        pass

            try:
                # 1. Direct copy of main DB file (filesystem-level read only, no SQLite connections or locks)
                shutil.copy2(APPLE_PHOTOS_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH)
                logging.info(f"Copied main DB file to {APPLE_PHOTOS_DB_COPY_PATH}")
                
                # 2. Direct copy of WAL if present (avoid copying the -shm index as it is a memory-mapped index)
                if os.path.exists(src_wal_path):
                    dest_wal_path = APPLE_PHOTOS_DB_COPY_PATH + "-wal"
                    shutil.copy2(src_wal_path, dest_wal_path)
                    logging.info(f"Copied WAL file to {dest_wal_path}")

                # 3. Verify physical integrity and repair index issues locally on the destination SSD copy
                success = perform_direct_copy_and_repair(APPLE_PHOTOS_DB_COPY_PATH)
                if success:
                    copy_success = True
                    break
                else:
                    logging.warning(f"⚠️ Copy verification/repair failed on attempt {attempt}.")
            except Exception as e:
                logging.warning(f"⚠️ Direct copy failed on attempt {attempt}: {e}")
            
            if attempt < max_copy_attempts:
                logging.info("Waiting 5 seconds before retrying...")
                time.sleep(5)
        
        if not copy_success:
            logging.error("❌ All copy attempts failed to produce a healthy database copy.")
            return 1

        # Update modification time to match source
        try:
            os.utime(APPLE_PHOTOS_DB_COPY_PATH, (src_time, src_time))
        except Exception as utime_err:
            logging.warning(f"Failed to update modification time: {utime_err}")
        write_marker(src_time)
        logging.info("✅ Copy and verification complete.")

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