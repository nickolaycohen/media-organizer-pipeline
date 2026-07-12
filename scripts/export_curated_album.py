import os
import sys
import sqlite3
import subprocess
import argparse

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH, CURATED_LACIE_DIR

MODULE_TAG = "export_curated_album"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def run_applescript(moment_name, dest_dir):
    applescript_code = f'''
    property topFolderName : "Media Organizer on LaCie"
    property midFolderName : "Curated"

    on run argv
        set albumName to item 1 of argv
        set destinationFolderPath to item 2 of argv
        
        tell application "Photos"
            -- Find Top Folder
            set topFolder to missing value
            repeat with f in folders
                if name of f is equal to topFolderName then
                    set topFolder to f
                    exit repeat
                end if
            end repeat
            if topFolder is missing value then
                return "ERROR: Top folder not found"
            end if
            
            -- Find Mid Folder
            set midFolder to missing value
            repeat with f in folders of topFolder
                if name of f is equal to midFolderName then
                    set midFolder to f
                    exit repeat
                end if
            end repeat
            if midFolder is missing value then
                return "ERROR: Mid folder Curated not found"
            end if
            
            -- Find Album
            set targetAlbum to missing value
            repeat with a in albums of midFolder
                if name of a is equal to albumName then
                    set targetAlbum to a
                    exit repeat
                end if
            end repeat
            if targetAlbum is missing value then
                return "ERROR: Album not found"
            end if
            
            set mediaItems to media items of targetAlbum
            set totalCount to count of mediaItems
            if totalCount is 0 then
                return "ERROR: Album is empty"
            end if
            
            -- Create directory
            do shell script "mkdir -p " & quoted form of destinationFolderPath
            set destFolder to POSIX file destinationFolderPath as alias
            
            set exportedIds to {{}}
            set itemsToExport to {{}}
            
            repeat with thisItem in mediaItems
                set itemId to id of thisItem
                set itemName to filename of thisItem
                set itemPath to destinationFolderPath & "/" & itemName
                
                set fileCheckCmd to "test -f " & quoted form of itemPath & " && echo exists || echo missing"
                set fileStatus to do shell script fileCheckCmd
                
                copy itemId to end of exportedIds
                
                if fileStatus is "missing" then
                    copy thisItem to end of itemsToExport
                end if
            end repeat
            
            if (count of itemsToExport) > 0 then
                export itemsToExport to destFolder with using originals
            end if
            
            set AppleScript's text item delimiters to ","
            set idsString to exportedIds as string
            set AppleScript's text item delimiters to ""
            return idsString
        end tell
    end run
    '''
    
    logger.info(f"Running AppleScript to export curated album '{moment_name}' to {dest_dir}...")
    process = subprocess.Popen(['osascript', '-e', applescript_code, moment_name, dest_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if stderr:
        logger.warning(f"AppleScript Warning/Stderr: {stderr.strip()}")
    return stdout.strip()

def main():
    parser = argparse.ArgumentParser(description="Export curated photos from Apple Photos to LaCie and record them in the database.")
    parser.add_argument("moment_name", help="Name of the curated album/moment to export")
    args = parser.parse_args()

    moment_name = args.moment_name
    dest_dir = os.path.join(CURATED_LACIE_DIR, moment_name)

    # Run the export AppleScript
    result = run_applescript(moment_name, dest_dir)

    if not result:
        logger.error("❌ AppleScript returned empty output.")
        sys.exit(1)
    if result.startswith("ERROR:"):
        logger.error(f"❌ AppleScript Error: {result}")
        sys.exit(1)

    # Parse exported asset IDs
    raw_ids = [item.strip() for item in result.split(",") if item.strip()]
    asset_ids = []
    for rid in raw_ids:
        # Extract UUID (part before first slash, e.g. "8E0CE138-0096-4A73-A338-709B5AD8A758/L0/001")
        uuid = rid.split("/")[0]
        asset_ids.append(uuid)

    logger.info(f"✅ Successfully verified/exported {len(asset_ids)} items to {dest_dir}")

    # Record in database
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    try:
        # 1. Update moment_exports with curation_stage = 'curated'
        export_data = [(aid, moment_name, 'curated') for aid in asset_ids]
        cursor.executemany("""
            INSERT OR REPLACE INTO moment_exports (asset_id, album_name, curation_stage, exported_at_utc)
            VALUES (?, ?, ?, datetime('now'))
        """, export_data)

        # 2. Update assets table with curated_album name
        for aid in asset_ids:
            cursor.execute("""
                UPDATE assets 
                SET curated_album = ? 
                WHERE asset_id = ?
            """, (moment_name, aid))

        # 3. Update curated_moments tracking
        cursor.execute("""
            INSERT INTO curated_moments (moment_name, curated_count, photos_curated_exists, last_curated_sync, memory_stage)
            VALUES (?, ?, 1, datetime('now'), 'M400')
            ON CONFLICT(moment_name) DO UPDATE SET
                curated_count = excluded.curated_count,
                photos_curated_exists = 1,
                last_curated_sync = excluded.last_curated_sync,
                memory_stage = CASE WHEN memory_stage = 'M500' THEN 'M500' ELSE 'M400' END
        """, (moment_name, len(asset_ids)))

        conn.commit()
        logger.info("✅ Database records updated successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to update database records: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
