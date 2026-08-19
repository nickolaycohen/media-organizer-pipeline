import os
import sys
import gzip
import shutil
import sqlite3
from datetime import datetime

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import MEDIA_ORGANIZER_DB_PATH, BG_SERVICE_PID_PATH

def restore_database(backup_path):
    print(f"[{datetime.now().isoformat()}] Starting database restore process...")

    if not backup_path:
        print("Error: Please provide the path to the backup file.")
        print("Usage: python3 scripts/restore_database.py backups/media_organizer_backup_YYYYMMDD_HHMMSS.db.gz")
        sys.exit(1)

    if not os.path.exists(backup_path):
        print(f"Error: Backup file not found at {backup_path}")
        sys.exit(1)

    # 1. Check if the background sync service is running
    if os.path.exists(BG_SERVICE_PID_PATH):
        try:
            with open(BG_SERVICE_PID_PATH, 'r') as f:
                pid = int(f.read().strip())
            # Check if PID is active
            os.kill(pid, 0)
            print(f"⚠️ Warning: The background sync service is currently running (PID {pid}).")
            print("Please stop the service before performing a restore. You can run:")
            print(f"  kill {pid}")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            # PID file exists but process is dead, we can proceed
            pass
        except Exception as e:
            print(f"Warning: Failed to check service PID status: {e}")

    # Confirm restore with the user
    confirm = input(f"Are you sure you want to restore {backup_path} and overwrite the current database? (y/N): ")
    if confirm.lower() != 'y':
        print("Restore cancelled.")
        return

    # Paths for active DB files
    wal_path = MEDIA_ORGANIZER_DB_PATH + "-wal"
    shm_path = MEDIA_ORGANIZER_DB_PATH + "-shm"

    try:
        # 2. Safely remove active WAL and SHM files to prevent corruption or old page recovery
        print("Cleaning up active database and SQLite journal files...")
        for path in [MEDIA_ORGANIZER_DB_PATH, wal_path, shm_path]:
            if os.path.exists(path):
                os.remove(path)
                print(f"Removed: {os.path.basename(path)}")

        # 3. Decompress the gzip backup directly into media_organizer.db
        print(f"Decompressing {backup_path} into {MEDIA_ORGANIZER_DB_PATH}...")
        with gzip.open(backup_path, 'rb') as f_in:
            with open(MEDIA_ORGANIZER_DB_PATH, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # 4. Verify integrity of the restored database
        print("Verifying integrity of the restored database...")
        conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA integrity_check;")
        res = cursor.fetchone()
        conn.close()

        if res and res[0] == 'ok':
            print("✅ Database integrity check passed.")
            print(f"🎉 Database successfully restored from {backup_path}!")
        else:
            print(f"❌ Warning: Restored database integrity check failed: {res}")

    except Exception as e:
        print(f"❌ Restore failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    restore_database(path_arg)
