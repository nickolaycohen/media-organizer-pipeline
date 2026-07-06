import os
import sys
import sqlite3
import subprocess
from pathlib import Path

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH, LOG_PATH, APPLE_SCRIPT_LOG_PATH, MOMENTS_EXPORT_DIR

MODULE_TAG = "apple_moments_sync"
logger = setup_logger(LOG_PATH, MODULE_TAG)
as_logger = setup_logger(APPLE_SCRIPT_LOG_PATH, "applescript_worker")

def run_applescript(script_content):
    as_logger.info(f"--- START APPLESCRIPT EXECUTION ---\n{script_content}\n--- END SCRIPT CONTENT ---")
    process = subprocess.Popen(['osascript', '-e', script_content], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    if stderr:
        as_logger.error(f"AppleScript Error:\n{stderr}")
        logger.error(f"AppleScript reported an error. See {APPLE_SCRIPT_LOG_PATH} for details.")
    if stdout:
        as_logger.info(f"AppleScript Output:\n{stdout}")
    as_logger.info("--- FINISHED EXECUTION ---\n")
    return stdout

def main():
    logger.info("🚀 Organizing suggested Moments into Apple Photos albums...")
    
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Query assets that are in a batch that is at least ranked (600)
    # and have a suggested Moment name assigned by the pipeline.
    query = """
        SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, me.asset_id
        FROM ranked_assets_view v
        JOIN month_batches mb ON v.month = mb.month
        LEFT JOIN moment_exports me ON v.asset_id = me.asset_id
        WHERE mb.status_code >= '600'
        ORDER BY v.score_normalized DESC;
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        logger.warning("No assets found in ranked batches. Ensure you have batches at stage 600.")
        conn.close()
        return

    # Establish the cutoff threshold: the score of the first asset without a Moment
    threshold_score = None
    for r in rows:
        if not r[1]: # moment_name is index 1
            threshold_score = r[2] # score_normalized is index 2
            logger.info(f"🛑 Found first asset without a Moment. Cutoff threshold established at score: {r[2]:.4f}")
            break

    if threshold_score is None:
        logger.warning("No assets without Moments found to establish a cutoff threshold.")
        conn.close()
        return

    # Check filesystem folder existence for each unique album name
    album_folder_exists = {}
    for r in rows:
        album_name = r[1]
        if album_name and album_name not in album_folder_exists:
            safe_moment_name = "".join([c for c in album_name if c.isalnum() or c in (' ', '-', '_')]).strip()
            folder_path = os.path.join(MOMENTS_EXPORT_DIR, safe_moment_name)
            album_folder_exists[album_name] = os.path.exists(folder_path)

    # Filter list: assets with a Moment, score higher than threshold.
    # An asset should be exported if it has not been recorded as exported in DB (r[3] is None) OR
    # the filesystem folder for its target Moment does not exist.
    export_list = []
    reexport_albums = set()
    for r in rows:
        asset_id, album_name, score, db_exported_id = r
        if score > threshold_score and album_name:
            folder_exists = album_folder_exists.get(album_name, False)
            if db_exported_id is None or not folder_exists:
                export_list.append(r)
                if db_exported_id is not None and not folder_exists:
                    reexport_albums.add(album_name)

    if not export_list:
        logger.warning(f"No new assets found to organize. All candidates above {threshold_score:.4f} are already recorded as exported and their folders exist on the filesystem.")
        conn.close()
        return

    if reexport_albums:
        logger.info(f"Re-exporting assets for albums because their folders do not exist on the filesystem: {', '.join(reexport_albums)}")

    # Group asset IDs by suggested Moment name
    album_map = {}
    for asset_id, album_name, score, is_exported in export_list:
        if album_name not in album_map:
            album_map[album_name] = []
        album_map[album_name].append(asset_id)

    logger.info(f"Syncing {len(export_list)} assets across {len(album_map)} albums to Apple Photos...")

    for album_name, asset_ids in album_map.items():
        safe_album_name = album_name.replace('"', '\\"')
        ids_string = ",".join([f'"{aid}"' for aid in asset_ids])
        
        # AppleScript to create hierarchy and add assets by ID (no re-import)
        script = f'''
        property debugLogPath : "/Users/nickolaycohen/dev/media-organizer-pipeline/logs/applescript_execution.log"
        tell application "Photos"
            set topFolderName to "Media Organizer on LaCie"
            set subFolderName to "MomentExport"
            set targetAlbumName to "{safe_album_name}"

            if not (exists folder topFolderName) then
                make new folder named topFolderName
            end if
            set topFolder to folder topFolderName
            
            if not (exists folder subFolderName of topFolder) then
                make new folder named subFolderName at topFolder
            end if
            set exportFolder to folder subFolderName of topFolder
            
            if not (exists album targetAlbumName of exportFolder) then
                make new album named targetAlbumName at exportFolder
            end if
            set targetAlbum to album targetAlbumName of exportFolder
            
            set assetIds to {{{ids_string}}}
            set assetsToAdd to {{}}
            repeat with anId in assetIds
                try
                    set foundItems to {{}}
                    try
                        -- Attempt fast exact UUID lookup using the standard Apple Photos local identifier suffix
                        set end of foundItems to media item id (anId & "/L0/001")
                    on error
                        -- Fallback to slow substring search if direct lookup fails
                        set foundItems to (media items whose id contains anId)
                    end try
                    
                    if (count of foundItems) > 0 then
                        copy item 1 of foundItems to end of assetsToAdd
            			logMessage("Copying: " & anId)
                    end if
                end try
            end repeat
            
            if (count of assetsToAdd) > 0 then
                add assetsToAdd to targetAlbum
            end if
        end tell

        -- ========================================
        -- Helper Functions
        -- ========================================
        on logMessage(messageText)
            do shell script "echo " & quoted form of messageText & " >> " & quoted form of debugLogPath
        end logMessage

        '''
        run_applescript(script)
        logger.info(f"  ✅ Sync complete for album: {album_name} ({len(asset_ids)} assets)")
        
        # Record the export in the database
        export_data = [(aid, album_name) for aid in asset_ids]
        cursor.executemany("""
            INSERT OR REPLACE INTO moment_exports (asset_id, album_name, exported_at_utc)
            VALUES (?, ?, datetime('now'))
        """, export_data)
        conn.commit()

    logger.info("🏁 Apple Photos Moments sync finished.")
    conn.close()

if __name__ == "__main__":
    main()
