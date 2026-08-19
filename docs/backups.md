# Database Backup Guide

This project includes a safe database backup solution at [`scripts/backup_database.py`](file:///Users/nickolaycohen/dev/media-organizer-pipeline/scripts/backup_database.py) to archive and protect your metadata database (`media_organizer.db`).

## How It Works
- **Hot Backup**: Uses SQLite's Online Backup API to copy pages dynamically without holding read or write locks on the active database.
- **Compression**: Compresses the database copy to `gzip` format (e.g. `.db.gz`), reducing file size by ~30% (from 190MB down to ~135MB).
- **Retention**: Automatically keeps only the last **7 daily backups** to save disk space.
- **Location**: Backups are written to the [`backups/`](file:///Users/nickolaycohen/dev/media-organizer-pipeline/backups/) folder in the project root.

---

## Scheduling the Backup

To automate the backup, you can schedule it to run daily using macOS `launchd` or `cron`.

### Option A: macOS LaunchAgent (Recommended)

1. Create a plist file at `~/Library/LaunchAgents/com.mediaorganizer.backup.plist`:
   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
   <plist version="1.0">
   <dict>
       <key>Label</key>
       <string>com.mediaorganizer.backup</string>
       <key>ProgramArguments</key>
       <array>
           <string>/usr/bin/python3</string>
           <string>/Users/nickolaycohen/dev/media-organizer-pipeline/scripts/backup_database.py</string>
       </array>
       <key>StartCalendarInterval</key>
       <dict>
           <key>Hour</key>
           <integer>2</integer>
           <key>Minute</key>
           <integer>0</integer>
       </dict>
       <key>StandardOutPath</key>
       <string>/Users/nickolaycohen/dev/media-organizer-pipeline/logs/backup.log</string>
       <key>StandardErrorPath</key>
       <string>/Users/nickolaycohen/dev/media-organizer-pipeline/logs/backup_error.log</string>
   </dict>
   </plist>
   ```

2. Load the LaunchAgent:
   ```bash
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.mediaorganizer.backup.plist
   ```

This will run the backup automatically every day at 2:00 AM.

---

### Option B: Crontab

1. Open your crontab editor:
   ```bash
   crontab -e
   ```

2. Add a line to execute the script daily at 2 AM (saving output to a log file):
   ```text
   0 2 * * * /usr/bin/python3 /Users/nickolaycohen/dev/media-organizer-pipeline/scripts/backup_database.py >> /Users/nickolaycohen/dev/media-organizer-pipeline/logs/backup.log 2>&1
   ```

---

## Syncing with Time Machine and Cloud Providers (iDrive, Dropbox, iCloud)

### 1. Time Machine
Time Machine automatically backs up the `/Users/nickolaycohen/dev/media-organizer-pipeline/backups/` directory as part of your system backup. Because the backup files inside this folder are completed, static `.db.gz` archives, Time Machine will copy them safely without any open-file lock issues.

### 2. iDrive / Cloud Backups
To back up to iDrive, iCloud Drive, or other cloud storage services:
- **iDrive**: Open your iDrive application on macOS, select **Backup** > **Classic File Backup**, and check the box next to `/Users/nickolaycohen/dev/media-organizer-pipeline/backups/` folder.
- **iCloud Drive / Google Drive**: You can symlink the `backups` folder directly to your local cloud drive folder:
  ```bash
  ln -s /Users/nickolaycohen/dev/media-organizer-pipeline/backups ~/Library/Mobile\ Documents/com~apple~CloudDocs/MediaOrganizerBackups
  ```

---

## Restoring the Database

A helper script has been provided to safely restore a database backup without corruption.

### Safe Restore Steps:
1. **Stop the Background Service**: Make sure the sync service is stopped to prevent concurrent writes during restore.
2. **Clean SQLite Journal Files**: Old WAL (`-wal`) and SHM (`-shm`) cache files must be deleted so SQLite doesn't attempt to recover invalid transactions on top of the newly restored DB.
3. **Decompress Backup**: Restores the database file safely.

### How to Run:
Pass the path of the `.db.gz` backup file to [`restore_database.py`](file:///Users/nickolaycohen/dev/media-organizer-pipeline/scripts/restore_database.py):

```bash
python3 scripts/restore_database.py backups/media_organizer_backup_YYYYMMDD_HHMMSS.db.gz
```

The script will prompt for confirmation, clean the active files, decompress the backup, and run a `PRAGMA integrity_check` on the database to ensure a successful restore.
