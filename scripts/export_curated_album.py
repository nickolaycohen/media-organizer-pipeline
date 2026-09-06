import os
import sys
import sqlite3
import subprocess
import argparse

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH, CURATED_LACIE_DIR, APPLE_SCRIPT_LOG_PATH

MODULE_TAG = "export_curated_album"
logger = setup_logger(LOG_PATH, MODULE_TAG)
as_logger = setup_logger(APPLE_SCRIPT_LOG_PATH, "applescript_worker", include_console=False)

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
            
            -- Find Album (check flat album first, then check folder of the same name)
            set targetAlbum to missing value
            if exists album albumName of midFolder then
                set targetAlbum to album albumName of midFolder
            else if exists folder albumName of midFolder then
                set nestedFolder to folder albumName of midFolder
                if exists album albumName of nestedFolder then
                    set targetAlbum to album albumName of nestedFolder
                end if
            end if
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
            
            set results to {{}}
            set itemsToExport to {{}}
            
            repeat with thisItem in mediaItems
                set itemId to id of thisItem
                set itemName to filename of thisItem
                set itemPath to destinationFolderPath & "/" & itemName
                
                set fileCheckCmd to "test -f " & quoted form of itemPath & " && echo exists || echo missing"
                set fileStatus to do shell script fileCheckCmd
                
                copy (itemId & "|" & itemName) to end of results
                
                if fileStatus is "missing" then
                    copy thisItem to end of itemsToExport
                end if
            end repeat
            
            if (count of itemsToExport) > 0 then
                export itemsToExport to destFolder with using originals
            end if
            
            set oldDelims to AppleScript's text item delimiters
            set AppleScript's text item delimiters to "\\n"
            set resultsString to results as string
            set AppleScript's text item delimiters to oldDelims
            return resultsString
        end tell
    end run
    '''
    
    as_logger.info(f"--- START APPLESCRIPT EXECUTION ({moment_name}) ---\n{applescript_code}\n--- END SCRIPT CONTENT ---")
    process = subprocess.Popen(['osascript', '-e', applescript_code, moment_name, dest_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if stderr:
        as_logger.error(f"AppleScript Error ({moment_name}):\n{stderr}")
    if stdout:
        as_logger.info(f"AppleScript Output ({moment_name}):\n{stdout}")
    as_logger.info("--- FINISHED EXECUTION ---\n")
    return stdout.strip()

def get_skipped_publishing_uuids():
    from constants import APPLE_PHOTOS_DB_PATH
    skipped_uuids = set()
    if os.path.exists(APPLE_PHOTOS_DB_PATH):
        try:
            conn_photos = sqlite3.connect(f"file:{APPLE_PHOTOS_DB_PATH}?mode=ro", uri=True)
            c = conn_photos.cursor()
            c.execute("""
                SELECT DISTINCT z.ZUUID
                FROM ZASSET z
                JOIN Z_30ASSETS aa ON aa.Z_3ASSETS = z.Z_PK
                JOIN ZGENERICALBUM ga ON ga.Z_PK = aa.Z_30ALBUMS
                WHERE LOWER(ga.ZTITLE) IN ('skippublishing', 'ignore')
                  AND ga.ZTRASHEDSTATE = 0 AND z.ZTRASHEDSTATE = 0
            """)
            skipped_uuids = set(r[0] for r in c.fetchall() if r[0])
            conn_photos.close()
        except Exception as e:
            logger.warning(f"Could not load skipped assets from Photos DB: {e}")
    return skipped_uuids

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

    # Parse exported asset IDs and filenames
    lines = [line.strip() for line in result.split("\n") if line.strip()]
    raw_asset_ids = []
    raw_expected_filenames = []
    for line in lines:
        if '|' in line:
            rid, fname = line.split('|', 1)
            # Extract UUID (part before first slash, e.g. "8E0CE138-0096-4A73-A338-709B5AD8A758/L0/001")
            uuid = rid.split("/")[0]
            raw_asset_ids.append(uuid)
            raw_expected_filenames.append(fname)

    # Filter out assets in SkipPublishing or Ignore albums
    skipped_uuids = get_skipped_publishing_uuids()
    valid_items = [(aid, fname) for aid, fname in zip(raw_asset_ids, raw_expected_filenames) if aid not in skipped_uuids]
    asset_ids = [item[0] for item in valid_items]
    expected_filenames = [item[1] for item in valid_items]

    if len(raw_asset_ids) != len(asset_ids):
        logger.info(f"🚫 Excluded {len(raw_asset_ids) - len(asset_ids)} assets that are in SkipPublishing / Ignore.")

    logger.info(f"✅ Successfully verified/exported {len(asset_ids)} items to {dest_dir}")

    # Prune extra files in dest_dir that do not correspond to the curated album assets (or are skipped)
    expected_bases = set(os.path.splitext(f)[0].lower() for f in expected_filenames)
    if os.path.exists(dest_dir):
        try:
            pruned_count = 0
            for f in os.listdir(dest_dir):
                file_path = os.path.join(dest_dir, f)
                if os.path.isfile(file_path) and not f.startswith('.'):
                    f_base = os.path.splitext(f)[0].lower()
                    if f_base not in expected_bases:
                        os.remove(file_path)
                        logger.info(f"🗑️ Pruned extra/skipped file from curated directory: {f}")
                        pruned_count += 1
            if pruned_count > 0:
                logger.info(f"✅ Pruned {pruned_count} extra/stale/skipped files from {dest_dir}")
        except Exception as e:
            logger.warning(f"Failed to prune extra files in {dest_dir}: {e}")

    # Record in database
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    try:
        # 1. Clean up any previous moment_exports for this album that are skipped
        if skipped_uuids:
            cursor.executemany("""
                DELETE FROM moment_exports
                WHERE (album_name = ? OR album_name = ?) AND asset_id = ?
            """, [(moment_name, moment_name.strip(), sid) for sid in skipped_uuids])

        # 2. Update moment_exports with curation_stage = 'curated' for valid items
        export_data = [(aid, moment_name, 'curated') for aid in asset_ids]
        cursor.executemany("""
            INSERT OR REPLACE INTO moment_exports (asset_id, album_name, curation_stage, exported_at_utc)
            VALUES (?, ?, ?, datetime('now'))
        """, export_data)

        # 3. Update assets table with curated_album name
        for aid in asset_ids:
            cursor.execute("""
                UPDATE assets 
                SET curated_album = ? 
                WHERE asset_id = ?
            """, (moment_name, aid))

        # 4. Clear curated_album for any skipped assets
        if skipped_uuids:
            cursor.executemany("""
                UPDATE assets
                SET curated_album = NULL
                WHERE curated_album = ? AND asset_id = ?
            """, [(moment_name, sid) for sid in skipped_uuids])

        # 5. Update curated_moments tracking
        cursor.execute("""
            INSERT INTO curated_moments (moment_name, curated_count, photos_curated_exists, last_curated_sync, memory_stage)
            VALUES (?, ?, 1, datetime('now'), 'M400')
            ON CONFLICT(moment_name) DO UPDATE SET
                curated_count = excluded.curated_count,
                photos_curated_exists = 1,
                last_curated_sync = excluded.last_curated_sync,
                memory_stage = CASE WHEN memory_stage IN ('M500', 'M450') THEN memory_stage ELSE 'M400' END
        """, (moment_name, len(asset_ids)))

        conn.commit()
        logger.info("✅ Database records updated successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to update database records: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

    # Run the deduplication utility on the exported curated folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dedup_script = os.path.join(script_dir, "deduplicate_assets.py")
    logger.info(f"🔄 Running deduplication on curated export directory: {dest_dir}")
    try:
        subprocess.run([sys.executable, dedup_script, dest_dir], check=True)
        logger.info("✅ Deduplication of curated album complete.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Deduplication failed: {e}")

if __name__ == "__main__":
    main()
