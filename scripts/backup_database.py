import os
import sys
import time
import sqlite3
import gzip
import shutil
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import BASE_DIR, MEDIA_ORGANIZER_DB_PATH

BACKUPS_DIR = os.path.abspath(os.path.join(BASE_DIR, "../backups"))
BACKUP_RETENTION_COUNT = 7

def perform_backup():
    print(f"[{datetime.now().isoformat()}] Starting database backup process...")
    
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        print(f"Error: Database file not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    # 1. Create backups directory if it doesn't exist
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    
    # Generate filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_backup_path = os.path.join(BACKUPS_DIR, f"media_organizer_temp_{timestamp}.db")
    final_gzip_path = os.path.join(BACKUPS_DIR, f"media_organizer_backup_{timestamp}.db.gz")
    
    src_conn = None
    dest_conn = None
    try:
        # 2. Open source database connection (in read-only mode to prevent any locks/writes)
        src_conn = sqlite3.connect(f"file:{MEDIA_ORGANIZER_DB_PATH}?mode=ro", uri=True)
        
        # 3. Open temporary destination connection
        dest_conn = sqlite3.connect(temp_backup_path)
        
        # 4. Perform SQLite online backup
        print("Safely copying database pages (SQLite Online Backup)...")
        with dest_conn:
            src_conn.backup(dest_conn)
            
        print("Database copy complete. Closing connections...")
        dest_conn.close()
        src_conn.close()
        
        # 5. Compress the backup copy
        print(f"Compressing database backup to {final_gzip_path}...")
        with open(temp_backup_path, 'rb') as f_in:
            with gzip.open(final_gzip_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        # Clean up temporary raw copy
        os.remove(temp_backup_path)
        
        # Get final compressed size
        compressed_size_mb = os.path.getsize(final_gzip_path) / (1024 * 1024)
        print(f"✅ Backup created successfully: {final_gzip_path} ({compressed_size_mb:.2f} MB)")
        
        # 6. Apply rotation policy (keep only last N backups)
        rotate_backups()
        
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_backup_path):
            try:
                os.remove(temp_backup_path)
            except Exception:
                pass
        sys.exit(1)

def rotate_backups():
    print(f"Applying backup retention policy (keeping last {BACKUP_RETENTION_COUNT} backups)...")
    
    # List all gzip backup files in backups dir
    files = []
    for entry in os.scandir(BACKUPS_DIR):
        if entry.is_file() and entry.name.startswith("media_organizer_backup_") and entry.name.endswith(".db.gz"):
            files.append(entry)
            
    # Sort files by name (which has YYYYMMDD_HHMMSS timestamp and sorts chronologically)
    files.sort(key=lambda x: x.name)
    
    # Delete oldest if count exceeds limit
    if len(files) > BACKUP_RETENTION_COUNT:
        to_delete = files[:-BACKUP_RETENTION_COUNT]
        for f in to_delete:
            try:
                os.remove(f.path)
                print(f"Deleted old backup file: {f.name}")
            except Exception as e:
                print(f"Warning: Failed to delete old backup {f.name}: {e}")
    else:
        print("No old backups to prune.")

if __name__ == "__main__":
    perform_backup()
