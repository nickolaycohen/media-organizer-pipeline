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

def get_moment_photos_assets(album_name):
    safe_album_name = album_name.replace('"', '\\"')
    script = f'''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        set resultsCurated to {{}}
        set resultsToBeCurated to {{}}
        
        if exists folder topFolderName then
            set topFolder to folder topFolderName
            
            -- Check Curated folder
            if exists folder "Curated" of topFolder then
                set midFolder to folder "Curated" of topFolder
                if exists album "{safe_album_name}" of midFolder then
                    set curatedAlbum to album "{safe_album_name}" of midFolder
                    set curatedItems to media items of curatedAlbum
                    repeat with cItem in curatedItems
                        copy id of cItem to end of resultsCurated
                    end repeat
                else if exists folder "{safe_album_name}" of midFolder then
                    set nestedFolder to folder "{safe_album_name}" of midFolder
                    set nestedAlbums to albums of nestedFolder
                    repeat with nAlb in nestedAlbums
                        set curatedItems to media items of nAlb
                        repeat with cItem in curatedItems
                            copy id of cItem to end of resultsCurated
                        end repeat
                    end repeat
                end if
            end if
            
            -- Check ToBeCurated folder
            if exists folder "ToBeCurated" of topFolder then
                set midFolder to folder "ToBeCurated" of topFolder
                if exists album "{safe_album_name}" of midFolder then
                    set curatedAlbum to album "{safe_album_name}" of midFolder
                    set curatedItems to media items of curatedAlbum
                    repeat with cItem in curatedItems
                        copy id of cItem to end of resultsToBeCurated
                    end repeat
                else if exists folder "{safe_album_name}" of midFolder then
                    set nestedFolder to folder "{safe_album_name}" of midFolder
                    set nestedAlbums to albums of nestedFolder
                    repeat with nAlb in nestedAlbums
                        set curatedItems to media items of nAlb
                        repeat with cItem in curatedItems
                            copy id of cItem to end of resultsToBeCurated
                        end repeat
                    end repeat
                end if
            end if
        end if
        
        set oldDelims to AppleScript's text item delimiters
        set AppleScript's text item delimiters to "\\n"
        set curatedStr to resultsCurated as string
        set toBeCuratedStr to resultsToBeCurated as string
        set AppleScript's text item delimiters to oldDelims
        
        return curatedStr & "===SEPARATOR===" & toBeCuratedStr
    end tell
    '''
    stdout = run_applescript(script)
    curated_uuids = set()
    to_be_curated_uuids = set()
    if stdout:
        parts = stdout.split("===SEPARATOR===")
        if len(parts) >= 1 and parts[0].strip():
            curated_uuids = {line.strip().split('/')[0] for line in parts[0].strip().split('\n') if line.strip()}
        if len(parts) >= 2 and parts[1].strip():
            to_be_curated_uuids = {line.strip().split('/')[0] for line in parts[1].strip().split('\n') if line.strip()}
    return curated_uuids, to_be_curated_uuids

def get_skip_publishing_asset_ids():
    script = '''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        set results to {}
        
        if exists folder topFolderName then
            set topFolder to folder topFolderName
            set skipAlbum to missing value
            if exists album "SkipPublishing" of topFolder then
                set skipAlbum to album "SkipPublishing" of topFolder
            else
                set flds to folders of topFolder
                repeat with f in flds
                    if exists album "SkipPublishing" of f then
                        set skipAlbum to album "SkipPublishing" of f
                        exit repeat
                    end if
                end repeat
            end if
            
            if skipAlbum is not missing value then
                set skipItems to media items of skipAlbum
                repeat with sItem in skipItems
                    copy id of sItem to end of results
                end repeat
            end if
        end if
        
        set oldDelims to AppleScript's text item delimiters
        set AppleScript's text item delimiters to "\\n"
        set resultsString to results as string
        set AppleScript's text item delimiters to oldDelims
        return resultsString
    end tell
    '''
    stdout = run_applescript(script)
    if stdout:
        return [line.strip() for line in stdout.strip().split('\n') if line.strip()]
    return []

def cleanup_empty_albums_and_folders(conn):
    logger.info("🧹 Cleaning up empty or mismatched albums and folders under ToBeCurated in Apple Photos...")
    script = '''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        set subFolderName to "ToBeCurated"
        set momentsFolderName to "Moments"
        set momentsNames to {}
        set deletedAlbums to {}
        
        if exists folder topFolderName then
            set topFolder to folder topFolderName
            if exists folder momentsFolderName of topFolder then
                set momentsFolder to folder momentsFolderName of topFolder
                set momentsNames to my getMomentsNames(momentsFolder)
            end if
            if exists folder subFolderName of topFolder then
                set exportFolder to folder subFolderName of topFolder
                set deletedAlbums to my cleanFolder(exportFolder, true, momentsNames)
            end if
        end if
        
        set oldDelims to AppleScript's text item delimiters
        set AppleScript's text item delimiters to "\\n"
        set deletedStr to deletedAlbums as string
        set AppleScript's text item delimiters to oldDelims
        return deletedStr
    end tell

    on getMomentsNames(targetFolder)
        set resList to {}
        tell application "Photos"
            set subFolders to folders of targetFolder
            repeat with subFld in subFolders
                copy name of subFld to end of resList
                set subNames to my getMomentsNames(subFld)
                set resList to resList & subNames
            end repeat
            set subAlbums to albums of targetFolder
            repeat with subAlb in subAlbums
                copy name of subAlb to end of resList
            end repeat
        end tell
        return resList
    end getMomentsNames

    on cleanFolder(targetFolder, isRoot, momentsNames)
        set deletedList to {}
        tell application "Photos"
            -- 1. Recursively clean subfolders
            set subFolders to folders of targetFolder
            repeat with subFld in subFolders
                set subFldName to name of subFld
                if not (my isSkipPublishing(subFldName)) then
                    if not (momentsNames contains subFldName) then
                        copy subFldName to end of deletedList
                        try
                            delete subFld
                        end try
                    else
                        set subDeleted to my cleanFolder(subFld, false, momentsNames)
                        set deletedList to deletedList & subDeleted
                    end if
                end if
            end repeat
            
            -- 2. Clean empty or non-moment albums inside this folder
            set subAlbums to albums of targetFolder
            repeat with subAlb in subAlbums
                set subAlbName to name of subAlb
                if not (my isSkipPublishing(subAlbName)) then
                    set isMismatched to not (momentsNames contains subAlbName)
                    set isEmpty to (count of media items of subAlb) is 0
                    if isMismatched or isEmpty then
                        copy subAlbName to end of deletedList
                        try
                            delete subAlb
                        end try
                    end if
                end if
            end repeat
            
            -- 3. If it's not the root folder, check if it is empty and delete it
            if not isRoot then
                set currentName to name of targetFolder
                if not (my isSkipPublishing(currentName)) then
                    if (count of albums of targetFolder) is 0 and (count of folders of targetFolder) is 0 then
                        try
                            delete targetFolder
                        end try
                    end if
                end if
            end if
        end tell
        return deletedList
    end cleanFolder

    on isSkipPublishing(nameStr)
        return (nameStr is "SkipPublishing")
    end isSkipPublishing
    '''
    stdout = run_applescript(script)
    deleted_albums = []
    if stdout:
        deleted_albums = [line.strip() for line in stdout.strip().split('\n') if line.strip()]
        
    if deleted_albums:
        logger.info(f"🗑️ Deleted {len(deleted_albums)} empty/mismatched albums or folders from Apple Photos: {', '.join(deleted_albums)}")
        cursor = conn.cursor()
        for album_name in deleted_albums:
            cursor.execute("""
                DELETE FROM moment_exports
                WHERE album_name = ? AND curation_stage = 'to_be_curated'
            """, (album_name,))
        conn.commit()
        logger.info("✅ Database updated: removed 'to_be_curated' export records for deleted albums.")
    else:
        logger.info("✨ No empty or mismatched albums/folders to delete in Apple Photos.")

def cleanup_empty_filesystem_dirs(root_dir):
    logger.info(f"🧹 Cleaning up empty filesystem directories under {root_dir}...")
    if not os.path.isdir(root_dir):
        return
    deleted_dirs = []
    # Bottom-up walk to ensure nested folders are processed and deleted before parent folders
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Skip any path that is or is inside SkipPublishing folder tree
        path_parts = [p.lower() for p in dirpath.split(os.sep)]
        if "skippublishing" in path_parts:
            continue

        for dirname in dirnames:
            if dirname.lower() == "skippublishing":
                continue
            full_path = os.path.join(dirpath, dirname)
            try:
                contents = os.listdir(full_path)
                real_files = [f for f in contents if not f.startswith('.')]
                if not real_files:
                    # Clean up hidden files so rmdir works
                    for f in contents:
                        os.remove(os.path.join(full_path, f))
                    os.rmdir(full_path)
                    deleted_dirs.append(full_path)
            except Exception as e:
                logger.error(f"Failed to delete directory {full_path}: {e}")
    if deleted_dirs:
        logger.info(f"🧹 Cleaned up {len(deleted_dirs)} empty filesystem directories: {', '.join(deleted_dirs)}")


def main():

    logger.info("🚀 Organizing suggested Moments into Apple Photos albums...")
    
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Fetch all skipped asset IDs from the global SkipPublishing album
    skip_publishing_ids = get_skip_publishing_asset_ids()
    global_skipped_publishing_uuids = {sid.split('/')[0] for sid in skip_publishing_ids if sid}
    if global_skipped_publishing_uuids:
        logger.info(f"Loaded {len(global_skipped_publishing_uuids)} globally skipped assets from Apple Photos 'SkipPublishing' album.")

    # Query assets that are in a batch that is at least ranked (600)
    # and have a suggested Moment name assigned by the pipeline.
    query = """
        SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, me.asset_id
        FROM ranked_assets_view v
        JOIN month_batches mb ON v.month = mb.month
        LEFT JOIN moment_exports me ON v.asset_id = me.asset_id AND me.curation_stage = 'to_be_curated'
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

    # Fetch all curated moment names from DB to determine which moments are active in curation
    cursor.execute("SELECT moment_name FROM curated_moments")
    curated_moments = {row[0] for row in cursor.fetchall()}

    # Group all qualified assets by suggested Moment name
    album_candidates = {}
    for r in rows:
        asset_id, album_name, score, db_exported_id = r
        if score > threshold_score and album_name:
            if album_name not in album_candidates:
                album_candidates[album_name] = []
            album_candidates[album_name].append(r)

    if not album_candidates:
        logger.warning(f"No assets found above the cutoff threshold of {threshold_score:.4f}.")
        conn.close()
        return

    logger.info(f"Analyzing {len(album_candidates)} potential albums for synchronization...")

    albums_to_sync = []
    # Dict to cache computed expected assets and curated UUIDs so we don't have to re-evaluate them
    sync_jobs = {}

    for album_name, candidates in sorted(album_candidates.items()):
        # Query Photos for Curated and ToBeCurated contents in one AppleScript call
        curated_uuids, to_be_curated_uuids = get_moment_photos_assets(album_name)

        # Fetch previously exported curated assets from the database to identify explicitly rejected/skipped items
        cursor.execute("SELECT asset_id FROM moment_exports WHERE album_name = ? AND curation_stage = 'curated'", (album_name,))
        previously_curated_uuids = {row[0] for row in cursor.fetchall()}

        min_curated_score = None
        if curated_uuids:
            placeholders = ",".join(["?"] * len(curated_uuids))
            cursor.execute(f"SELECT score_normalized FROM ranked_assets_view WHERE asset_id IN ({placeholders})", list(curated_uuids))
            scores = [row[0] for row in cursor.fetchall() if row[0] is not None]
            if scores:
                min_curated_score = min(scores)

        # Expected assets in ToBeCurated for this album
        expected_uuids = set()
        skipped_already_curated = 0
        skipped_rejected = 0

        for asset_id, moment_name, score, db_exported_id in candidates:
            if asset_id in curated_uuids:
                skipped_already_curated += 1
                continue
            if asset_id in global_skipped_publishing_uuids:
                skipped_rejected += 1
                continue
            # Only skip if the asset was previously curated and is now deleted/skipped from the curated folder
            if min_curated_score is not None and score > min_curated_score and asset_id in previously_curated_uuids:
                skipped_rejected += 1
                continue
            expected_uuids.add(asset_id)

        # Determine sync trigger
        folder_exists = album_folder_exists.get(album_name, False)
        is_being_curated = album_name in curated_moments
        content_mismatch = (expected_uuids != to_be_curated_uuids)
        missing_db_exports = any(db_exported_id is None for asset_id, moment_name, score, db_exported_id in candidates if asset_id in expected_uuids)

        if not folder_exists or is_being_curated or content_mismatch or missing_db_exports:
            albums_to_sync.append(album_name)
            sync_jobs[album_name] = {
                'expected_uuids': expected_uuids,
                'skipped_already_curated': skipped_already_curated,
                'skipped_rejected': skipped_rejected,
                'min_curated_score': min_curated_score,
                'folder_exists': folder_exists
            }

    if not albums_to_sync:
        logger.info("🏁 Apple Photos is fully in sync. No albums require updates.")
    else:
        # Log albums that are being re-exported because their folder is missing on the filesystem
        reexport_albums = [name for name, job in sync_jobs.items() if not job['folder_exists']]
        if reexport_albums:
            logger.info(f"Re-exporting assets for albums because their folders do not exist on the filesystem: {', '.join(reexport_albums)}")

        logger.info(f"Syncing {len(albums_to_sync)} albums to Apple Photos...")

        for album_name in albums_to_sync:
            job = sync_jobs[album_name]
            expected_uuids = job['expected_uuids']
            skipped_already_curated = job['skipped_already_curated']
            skipped_rejected = job['skipped_rejected']
            min_curated_score = job['min_curated_score']

            if skipped_already_curated > 0 or skipped_rejected > 0:
                filter_msg = f"  🧹 Filtered '{album_name}': skipped {skipped_already_curated} already-curated assets, and {skipped_rejected} rejected assets"
                if min_curated_score is not None:
                    filter_msg += f" (score > {min_curated_score:.4f})"
                filter_msg += "."
                logger.info(filter_msg)

            safe_album_name = album_name.replace('"', '\\"')
            sorted_expected_uuids = sorted(list(expected_uuids))
            ids_string = ",".join([f'"{aid}"' for aid in sorted_expected_uuids])
            
            # AppleScript to create hierarchy and add assets by ID (no re-import)
            script = f'''
            property debugLogPath : "/Users/nickolaycohen/dev/media-organizer-pipeline/logs/applescript_execution.log"
            tell application "Photos"
                set topFolderName to "Media Organizer on LaCie"
                set subFolderName to "ToBeCurated"
                set targetAlbumName to "{safe_album_name}"

                if not (exists folder topFolderName) then
                    make new folder named topFolderName
                end if
                set topFolder to folder topFolderName
                
                if not (exists folder subFolderName of topFolder) then
                    make new folder named subFolderName at topFolder
                end if
                set exportFolder to folder subFolderName of topFolder
                
                if exists album targetAlbumName of exportFolder then
                    set oldAlbum to album targetAlbumName of exportFolder
                    delete album id (id of oldAlbum)
                end if
                make new album named targetAlbumName at exportFolder
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
                            my logMessage("Copying: " & anId)
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
            logger.info(f"  ✅ Sync complete for album: {album_name} ({len(sorted_expected_uuids)} assets)")
            
            # Record the export in the database
            export_data = [(aid, album_name, 'to_be_curated') for aid in sorted_expected_uuids]
            cursor.executemany("""
                INSERT OR REPLACE INTO moment_exports (asset_id, album_name, curation_stage, exported_at_utc)
                VALUES (?, ?, ?, datetime('now'))
            """, export_data)
            conn.commit()

    # Clean up empty albums/folders in Apple Photos and filesystem
    cleanup_empty_albums_and_folders(conn)
    cleanup_empty_filesystem_dirs(MOMENTS_EXPORT_DIR)

    logger.info("🏁 Apple Photos Moments sync finished.")
    conn.close()

if __name__ == "__main__":
    main()

