#!/usr/bin/env python3
"""
Reconstruct Apple Photos Album Structure and Members from Media Organizer Database Backup.

In case of catastrophic failure of Apple Photos, this script uses the Media Organizer
database (media_organizer.db) as an authoritative backup to reconstruct the entire
folder and album hierarchy under 'Media Organizer on LaCie' and restore all member assets.

Usage:
    # 1. Take a fresh backup snapshot of Apple Photos albums and members into media_organizer.db
    python3 scripts/reconstruct_apple_albums.py --backup

    # 2. Preview what would be reconstructed without making changes (Dry Run)
    python3 scripts/reconstruct_apple_albums.py --recreate --dry-run

    # 3. Reconstruct all folders, albums, and member assets in Apple Photos
    python3 scripts/reconstruct_apple_albums.py --recreate

    # 4. Reconstruct only a specific subfolder (e.g. Moments or Curated)
    python3 scripts/reconstruct_apple_albums.py --recreate --folder "Moments"

    # 5. Reconstruct only a single specific album
    python3 scripts/reconstruct_apple_albums.py --recreate --album "2026 - Venice"

    # 6. Verify existing Apple Photos structure against the database backup
    python3 scripts/reconstruct_apple_albums.py --verify
"""

import os
import sys
import time
import argparse
import sqlite3
import subprocess
from datetime import datetime, timezone

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    APPLE_PHOTOS_DB_PATH,
    APPLE_PHOTOS_DB_COPY_PATH,
    LOG_PATH,
    APPLE_SCRIPT_LOG_PATH
)

MODULE_TAG = "reconstruct_apple_albums"
logger = setup_logger(LOG_PATH, MODULE_TAG)
as_logger = setup_logger(APPLE_SCRIPT_LOG_PATH, "applescript_reconstruct", include_console=False)

ROOT_FOLDER_NAME = "Media Organizer on LaCie"


def run_applescript(script_content):
    """Executes AppleScript code via osascript and returns stdout."""
    as_logger.info(f"--- START APPLESCRIPT EXECUTION ---\n{script_content}\n--- END SCRIPT CONTENT ---")
    process = subprocess.Popen(['osascript', '-e', script_content], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    if stderr:
        as_logger.error(f"AppleScript Error:\n{stderr}")
    if stdout:
        as_logger.info(f"AppleScript Output:\n{stdout}")
    as_logger.info("--- FINISHED EXECUTION ---\n")
    return stdout, stderr


def get_source_photos_db_path():
    """Finds available Photos.sqlite path (live library or safe background copy)."""
    if os.path.exists(APPLE_PHOTOS_DB_PATH):
        return APPLE_PHOTOS_DB_PATH
    if os.path.exists(APPLE_PHOTOS_DB_COPY_PATH):
        return APPLE_PHOTOS_DB_COPY_PATH
    return None


def sync_album_backup_from_cursor(cursor, logger):
    """
    Core sync logic: populates media_organizer_albums and media_organizer_album_assets
    from the attached photos_db database.
    """
    # Ensure tables exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS media_organizer_albums (
            album_pk            INTEGER PRIMARY KEY,
            album_name          TEXT NOT NULL,
            parent_folder_pk    INTEGER,
            parent_folder_name  TEXT,
            full_path           TEXT NOT NULL,
            is_folder           INTEGER NOT NULL,
            depth               INTEGER NOT NULL,
            zkind               INTEGER NOT NULL,
            asset_count         INTEGER DEFAULT 0,
            synced_at_utc       TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS media_organizer_album_assets (
            album_pk            INTEGER NOT NULL,
            asset_id            TEXT NOT NULL,
            original_filename   TEXT,
            date_created_utc    TEXT,
            synced_at_utc       TEXT NOT NULL,
            PRIMARY KEY (album_pk, asset_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_albums_parent ON media_organizer_albums(parent_folder_pk)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_albums_path ON media_organizer_albums(full_path)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_album_assets_asset_id ON media_organizer_album_assets(asset_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_album_assets_album_pk ON media_organizer_album_assets(album_pk)")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sync_start = time.time()

    # 1. Fetch hierarchical album tree
    query_tree = """
    WITH RECURSIVE album_tree AS (
        SELECT 
            a.Z_PK, 
            a.ZTITLE, 
            a.ZPARENTFOLDER, 
            a.ZKIND, 
            1 as depth, 
            CAST(a.ZTITLE AS TEXT) as full_path
        FROM photos_db.ZGENERICALBUM a
        WHERE a.ZTITLE = ? AND a.ZTRASHEDSTATE = 0
        
        UNION ALL
        
        SELECT 
            ch.Z_PK, 
            ch.ZTITLE, 
            ch.ZPARENTFOLDER, 
            ch.ZKIND, 
            t.depth + 1, 
            t.full_path || '/' || ch.ZTITLE
        FROM photos_db.ZGENERICALBUM ch
        JOIN album_tree t ON ch.ZPARENTFOLDER = t.Z_PK
        WHERE ch.ZTRASHEDSTATE = 0
    )
    SELECT 
        t.Z_PK,
        t.ZTITLE,
        t.ZPARENTFOLDER,
        p.ZTITLE as parent_title,
        t.full_path,
        CASE WHEN t.ZKIND = 4000 THEN 1 ELSE 0 END as is_folder,
        t.depth,
        t.ZKIND,
        (SELECT count(*) FROM photos_db.Z_30ASSETS aa WHERE aa.Z_30ALBUMS = t.Z_PK) as asset_count,
        ? as synced_at_utc
    FROM album_tree t
    LEFT JOIN photos_db.ZGENERICALBUM p ON t.ZPARENTFOLDER = p.Z_PK
    ORDER BY t.depth, is_folder DESC, t.full_path;
    """
    cursor.execute(query_tree, (ROOT_FOLDER_NAME, now_utc))
    albums_data = cursor.fetchall()

    if not albums_data:
        logger.warning(f"⚠️ No albums found under '{ROOT_FOLDER_NAME}' in source database.")
        return False

    # Replace albums backup
    cursor.execute("DELETE FROM media_organizer_albums")
    cursor.executemany("""
        INSERT INTO media_organizer_albums (
            album_pk, album_name, parent_folder_pk, parent_folder_name,
            full_path, is_folder, depth, zkind, asset_count, synced_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, albums_data)

    # 2. Fetch all asset memberships for user albums (zkind = 2)
    query_assets = """
    WITH RECURSIVE album_tree AS (
        SELECT 
            a.Z_PK, 
            a.ZTITLE, 
            a.ZPARENTFOLDER, 
            a.ZKIND, 
            1 as depth, 
            CAST(a.ZTITLE AS TEXT) as full_path
        FROM photos_db.ZGENERICALBUM a
        WHERE a.ZTITLE = ? AND a.ZTRASHEDSTATE = 0
        
        UNION ALL
        
        SELECT 
            ch.Z_PK, 
            ch.ZTITLE, 
            ch.ZPARENTFOLDER, 
            ch.ZKIND, 
            t.depth + 1, 
            t.full_path || '/' || ch.ZTITLE
        FROM photos_db.ZGENERICALBUM ch
        JOIN album_tree t ON ch.ZPARENTFOLDER = t.Z_PK
        WHERE ch.ZTRASHEDSTATE = 0
    )
    SELECT 
        t.Z_PK as album_pk,
        z.ZUUID as asset_id,
        aaa.ZORIGINALFILENAME as original_filename,
        datetime(z.ZDATECREATED + 978307200, 'unixepoch') as date_created_utc,
        ? as synced_at_utc
    FROM album_tree t
    JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = t.Z_PK
    JOIN photos_db.ZASSET z ON z.Z_PK = aa.Z_3ASSETS
    LEFT JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = z.Z_PK
    WHERE z.ZTRASHEDSTATE = 0 AND t.ZKIND = 2;
    """
    cursor.execute(query_assets, (ROOT_FOLDER_NAME, now_utc))
    assets_data = cursor.fetchall()

    # Replace assets backup
    cursor.execute("DELETE FROM media_organizer_album_assets")
    cursor.executemany("""
        INSERT INTO media_organizer_album_assets (
            album_pk, asset_id, original_filename, date_created_utc, synced_at_utc
        ) VALUES (?, ?, ?, ?, ?)
    """, assets_data)

    folder_count = sum(1 for a in albums_data if a[5] == 1)
    album_count = sum(1 for a in albums_data if a[5] == 0 and a[7] == 2)
    smart_album_count = sum(1 for a in albums_data if a[7] == 1507)
    elapsed = time.time() - sync_start

    logger.info(
        f"✅ Backup complete in {elapsed:.2f}s: "
        f"{folder_count} folders, {album_count} user albums ({smart_album_count} smart albums), "
        f"and {len(assets_data)} member asset associations backed up to media_organizer.db."
    )
    return True


def sync_album_backup(conn, logger):
    """
    Backs up all folders, albums, and asset memberships under 'Media Organizer on LaCie'
    from the Apple Photos SQLite database into media_organizer.db tables:
    - media_organizer_albums
    - media_organizer_album_assets
    """
    photos_path = get_source_photos_db_path()
    if not photos_path:
        logger.error(f"❌ Neither live Apple Photos DB ({APPLE_PHOTOS_DB_PATH}) nor local copy ({APPLE_PHOTOS_DB_COPY_PATH}) was found.")
        return False

    cursor = conn.cursor()
    logger.info(f"🔄 Starting album backup snapshot from Photos DB: {photos_path}")

    # Attach source photos DB
    is_attached = False
    try:
        cursor.execute("ATTACH DATABASE ? AS photos_db;", (photos_path,))
        is_attached = True
    except Exception as e:
        logger.error(f"❌ Failed to attach Photos database: {e}")
        return False

    try:
        success = sync_album_backup_from_cursor(cursor, logger)
        if success:
            conn.commit()
        else:
            conn.rollback()
        return success
    except Exception as e:
        logger.error(f"❌ Error during album backup sync: {e}")
        conn.rollback()
        return False
    finally:
        if is_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db;")
            except Exception:
                pass


def load_backed_up_albums(cursor, filter_folder=None, filter_album=None):
    """
    Loads albums from media_organizer_albums and media_organizer_album_assets.
    Returns:
        folders: list of folder records ordered by depth ASC
        albums: list of album records (zkind == 2) ordered by depth ASC
        smart_albums: list of smart album records (zkind == 1507)
    """
    cursor.execute("""
        SELECT album_pk, album_name, parent_folder_pk, parent_folder_name,
               full_path, is_folder, depth, zkind, asset_count, synced_at_utc
        FROM media_organizer_albums
        ORDER BY depth ASC, is_folder DESC, full_path ASC
    """)
    all_rows = cursor.fetchall()

    folders = []
    albums = []
    smart_albums = []

    for r in all_rows:
        # Columns: 0: pk, 1: name, 2: parent_pk, 3: parent_name, 4: full_path, 5: is_folder, 6: depth, 7: zkind, 8: asset_count
        full_path = r[4]
        name = r[1]
        is_folder = r[5]
        zkind = r[7]

        # Apply filters
        if filter_folder:
            # Check if filter_folder is in the path segments
            parts = full_path.split("/")
            if filter_folder.lower() not in [p.lower() for p in parts]:
                continue
        if filter_album:
            if name.lower() != filter_album.lower():
                continue

        if is_folder == 1:
            folders.append(r)
        elif zkind == 2:
            albums.append(r)
        elif zkind == 1507:
            smart_albums.append(r)

    return folders, albums, smart_albums


def get_parent_path_list(full_path):
    """
    Extracts parent folder path segments as a list of strings.
    E.g. 'Media Organizer on LaCie/Apple Photos/YYYY-MM-DD/2025-04-09'
    returns ['Media Organizer on LaCie', 'Apple Photos', 'YYYY-MM-DD']
    """
    parts = full_path.strip("/").split("/")
    return parts[:-1]


def applescript_escape(text):
    """Escapes backslashes and double quotes for AppleScript string literals."""
    if text is None:
        return ""
    return text.replace('\\', '\\\\').replace('"', '\\"')


def ensure_folder_in_photos(folder_path_list):
    """
    Ensures that a nested folder path exists in Apple Photos.
    Returns True if successfully ensured/created.
    """
    escaped_segments = [f'"{applescript_escape(p)}"' for p in folder_path_list]
    segments_str = "{" + ", ".join(escaped_segments) + "}"

    script = f'''
    on ensureFolderChain(pathList)
        tell application "Photos"
            set curFolder to missing value
            repeat with i from 1 to count of pathList
                set fName to item i of pathList
                if i is 1 then
                    if not (exists folder fName) then
                        make new folder named fName
                    end if
                    set curFolder to folder fName
                else
                    if not (exists folder fName of curFolder) then
                        make new folder named fName at curFolder
                    end if
                    set curFolder to folder fName of curFolder
                end if
            end repeat
            return "OK"
        end tell
    end ensureFolderChain

    return ensureFolderChain({segments_str})
    '''
    stdout, stderr = run_applescript(script)
    return "OK" in stdout


def ensure_album_in_photos(parent_path_list, album_name):
    """
    Ensures that an album exists inside the specified parent folder chain.
    """
    escaped_segments = [f'"{applescript_escape(p)}"' for p in parent_path_list]
    segments_str = "{" + ", ".join(escaped_segments) + "}"
    escaped_album_name = applescript_escape(album_name)

    script = f'''
    on ensureFolderChain(pathList)
        tell application "Photos"
            set curFolder to missing value
            repeat with i from 1 to count of pathList
                set fName to item i of pathList
                if i is 1 then
                    if not (exists folder fName) then
                        make new folder named fName
                    end if
                    set curFolder to folder fName
                else
                    if not (exists folder fName of curFolder) then
                        make new folder named fName at curFolder
                    end if
                    set curFolder to folder fName of curFolder
                end if
            end repeat
            return curFolder
        end tell
    end ensureFolderChain

    set parentFld to ensureFolderChain({segments_str})
    tell application "Photos"
        set albName to "{escaped_album_name}"
        if not (exists album albName of parentFld) then
            make new album named albName at parentFld
        end if
        return "OK"
    end tell
    '''
    stdout, stderr = run_applescript(script)
    return "OK" in stdout


def get_existing_album_asset_ids(parent_path_list, album_name):
    """
    Returns the set of asset UUIDs currently present in the Apple Photos album.
    Returns (set of UUIDs, status_str) where status_str is 'OK', 'MISSING_FOLDER', or 'MISSING_ALBUM'.
    """
    escaped_segments = [f'"{applescript_escape(p)}"' for p in parent_path_list]
    segments_str = "{" + ", ".join(escaped_segments) + "}"
    escaped_album_name = applescript_escape(album_name)

    script = f'''
    on findFolderChain(pathList)
        tell application "Photos"
            set curFolder to missing value
            repeat with i from 1 to count of pathList
                set fName to item i of pathList
                if i is 1 then
                    if not (exists folder fName) then
                        return missing value
                    end if
                    set curFolder to folder fName
                else
                    if not (exists folder fName of curFolder) then
                        return missing value
                    end if
                    set curFolder to folder fName of curFolder
                end if
            end repeat
            return curFolder
        end tell
    end findFolderChain

    set parentFld to findFolderChain({segments_str})
    if parentFld is missing value then
        return "MISSING_FOLDER"
    end if
    tell application "Photos"
        set albName to "{escaped_album_name}"
        if not (exists album albName of parentFld) then
            return "MISSING_ALBUM"
        end if
        set targetAlb to album albName of parentFld
        set idList to (id of media items of targetAlb)
        set oldDelims to AppleScript's text item delimiters
        set AppleScript's text item delimiters to ","
        set res to idList as string
        set AppleScript's text item delimiters to oldDelims
        return res
    end tell
    '''
    stdout, stderr = run_applescript(script)
    if not stdout or not stdout.strip():
        return set(), "EMPTY"
    stdout_trimmed = stdout.strip()
    if stdout_trimmed in ("MISSING_FOLDER", "MISSING_ALBUM"):
        return set(), stdout_trimmed

    uuids = set()
    for raw_id in stdout_trimmed.split(","):
        raw_id = raw_id.strip()
        if raw_id:
            uuid = raw_id.split("/")[0]
            uuids.add(uuid)
    return uuids, "OK"


def add_assets_to_album_batch(parent_path_list, album_name, asset_ids):
    """
    Adds a batch of asset UUIDs to the specified album in Apple Photos.
    Returns the number of assets successfully added.
    """
    if not asset_ids:
        return 0

    escaped_segments = [f'"{applescript_escape(p)}"' for p in parent_path_list]
    segments_str = "{" + ", ".join(escaped_segments) + "}"
    escaped_album_name = applescript_escape(album_name)
    escaped_ids = [f'"{applescript_escape(aid)}"' for aid in asset_ids]
    ids_str = "{" + ", ".join(escaped_ids) + "}"

    script = f'''
    on ensureFolderChain(pathList)
        tell application "Photos"
            set curFolder to missing value
            repeat with i from 1 to count of pathList
                set fName to item i of pathList
                if i is 1 then
                    if not (exists folder fName) then
                        make new folder named fName
                    end if
                    set curFolder to folder fName
                else
                    if not (exists folder fName of curFolder) then
                        make new folder named fName at curFolder
                    end if
                    set curFolder to folder fName of curFolder
                end if
            end repeat
            return curFolder
        end tell
    end ensureFolderChain

    set parentFld to ensureFolderChain({segments_str})
    tell application "Photos"
        set albName to "{escaped_album_name}"
        if not (exists album albName of parentFld) then
            make new album named albName at parentFld
        end if
        set targetAlb to album albName of parentFld
        
        set assetIds to {ids_str}
        set assetsToAdd to {{}}
        repeat with anId in assetIds
            try
                set end of assetsToAdd to media item id (anId & "/L0/001")
            on error
                try
                    set foundItems to (media items whose id contains anId)
                    if (count of foundItems) > 0 then
                        set end of assetsToAdd to item 1 of foundItems
                    end if
                end try
            end try
        end repeat
        
        if (count of assetsToAdd) > 0 then
            add assetsToAdd to targetAlb
        end if
        return (count of assetsToAdd) as string
    end tell
    '''
    stdout, stderr = run_applescript(script)
    if stdout and stdout.strip().isdigit():
        return int(stdout.strip())
    return 0


def verify_apple_photos_structure(cursor, filter_folder=None, filter_album=None):
    """
    Compares the backup in media_organizer.db with the active state in Apple Photos.
    Reports missing folders, missing albums, and asset count mismatches.
    """
    logger.info("🔍 Verifying Apple Photos structure against Media Organizer DB backup...")
    folders, albums, smart_albums = load_backed_up_albums(cursor, filter_folder, filter_album)

    print("\n" + "=" * 80)
    print(f"📊 VERIFICATION REPORT: Apple Photos vs Media Organizer DB Backup")
    print("=" * 80)

    # 1. Verify Folders
    missing_folders = []
    for f in folders:
        path_list = f[4].strip("/").split("/")
        # Check folder existence via AppleScript
        escaped_segments = [f'"{applescript_escape(p)}"' for p in path_list]
        segments_str = "{" + ", ".join(escaped_segments) + "}"
        script = f'''
        tell application "Photos"
            set pathList to {segments_str}
            set curFolder to missing value
            repeat with i from 1 to count of pathList
                set fName to item i of pathList
                if i is 1 then
                    if not (exists folder fName) then
                        return "MISSING"
                    end if
                    set curFolder to folder fName
                else
                    if not (exists folder fName of curFolder) then
                        return "MISSING"
                    end if
                    set curFolder to folder fName of curFolder
                end if
            end repeat
            return "EXISTS"
        end tell
        '''
        stdout, _ = run_applescript(script)
        if "EXISTS" not in stdout:
            missing_folders.append(f[4])

    print(f"📁 Folders: {len(folders) - len(missing_folders)}/{len(folders)} exist in Apple Photos.")
    if missing_folders:
        print(f"   ⚠️ Missing Folders ({len(missing_folders)}):")
        for mf in missing_folders[:10]:
            print(f"      - {mf}")
        if len(missing_folders) > 10:
            print(f"      ... and {len(missing_folders) - 10} more.")

    # 2. Verify Albums and Asset Counts
    missing_albums = []
    count_mismatches = []
    matched_albums = 0

    print(f"\n📂 Checking {len(albums)} User Albums...")
    for i, a in enumerate(albums, 1):
        album_pk = a[0]
        album_name = a[1]
        full_path = a[4]
        expected_count = a[8]
        parent_path_list = get_parent_path_list(full_path)

        existing_uuids, status = get_existing_album_asset_ids(parent_path_list, album_name)
        if status in ("MISSING_FOLDER", "MISSING_ALBUM"):
            missing_albums.append(full_path)
            continue

        actual_count = len(existing_uuids)
        if actual_count != expected_count:
            count_mismatches.append((full_path, expected_count, actual_count))
        else:
            matched_albums += 1

    print(f"📁 Albums: {matched_albums}/{len(albums)} fully match expected asset counts.")
    if missing_albums:
        print(f"   ❌ Missing Albums ({len(missing_albums)}):")
        for ma in missing_albums[:10]:
            print(f"      - {ma}")
        if len(missing_albums) > 10:
            print(f"      ... and {len(missing_albums) - 10} more.")

    if count_mismatches:
        print(f"   ⚠️ Asset Count Mismatches ({len(count_mismatches)}):")
        for cm in count_mismatches[:10]:
            print(f"      - {cm[0]}: Expected {cm[1]}, Found {cm[2]} in Apple Photos")
        if len(count_mismatches) > 10:
            print(f"      ... and {len(count_mismatches) - 10} more.")

    if smart_albums:
        print(f"\n💡 Note: {len(smart_albums)} Smart Albums are tracked in the database backup.")
        print("   AppleScript does not support creating Smart Albums; they must be created manually.")

    print("=" * 80 + "\n")


def reconstruct_apple_albums(cursor, filter_folder=None, filter_album=None, dry_run=False, batch_size=300):
    """
    Main reconstruction routine. Recreates folders, albums, and populates members from backup.
    """
    folders, albums, smart_albums = load_backed_up_albums(cursor, filter_folder, filter_album)

    if not folders and not albums:
        logger.warning("⚠️ No albums or folders found in media_organizer.db backup table.")
        logger.warning("💡 Run with '--backup' first to populate the backup snapshot from Photos DB.")
        return

    total_assets_to_restore = sum(a[8] for a in albums)

    print("\n" + "=" * 80)
    print(f"🚀 RECONSTRUCTION PLAN {'[DRY RUN]' if dry_run else ''}")
    print("=" * 80)
    print(f"📁 Folders to verify/create: {len(folders)}")
    print(f"📂 Albums to reconstruct:   {len(albums)}")
    print(f"🖼️  Total member assets:     {total_assets_to_restore}")
    if smart_albums:
        print(f"💡 Smart Albums (reference):{len(smart_albums)}")
    print("=" * 80 + "\n")

    if dry_run:
        print("--- Folder Hierarchy ---")
        for f in folders:
            print(f"  [Folder] Depth {f[6]}: {f[4]}")

        print("\n--- Albums and Members ---")
        for a in albums:
            print(f"  [Album]  Depth {a[6]}: {a[4]} ({a[8]} assets)")

        if smart_albums:
            print("\n--- Smart Albums (Manual Reference) ---")
            for sa in smart_albums:
                print(f"  [Smart]  Depth {sa[6]}: {sa[4]}")

        print("\n✨ Dry run complete. No changes were made in Apple Photos.")
        return

    # Check that Photos app is running
    stdout, _ = run_applescript('tell application "System Events" to return (name of processes) contains "Photos"')
    if "true" not in stdout:
        logger.info("📱 Apple Photos is not running. Launching Photos app...")
        run_applescript('tell application "Photos" to activate')
        time.sleep(3)

    # Phase 1: Recreate Folders (ordered by depth)
    logger.info("📁 Phase 1/3: Recreating folder structure in Apple Photos...")
    created_folders = 0
    for f in folders:
        path_list = f[4].strip("/").split("/")
        logger.info(f"  📁 Ensuring folder: {' / '.join(path_list)}")
        if ensure_folder_in_photos(path_list):
            created_folders += 1
        else:
            logger.error(f"  ❌ Failed to ensure folder: {' / '.join(path_list)}")

    logger.info(f"✅ Phase 1 complete: {created_folders}/{len(folders)} folders verified/created.")

    # Phase 2 & 3: Recreate Albums and Populate Assets
    logger.info(f"📂 Phase 2 & 3: Recreating {len(albums)} albums and populating member assets...")
    total_added_assets = 0
    total_already_present = 0
    failed_albums = []

    for i, a in enumerate(albums, 1):
        album_pk = a[0]
        album_name = a[1]
        full_path = a[4]
        expected_count = a[8]
        parent_path_list = get_parent_path_list(full_path)

        logger.info(f"\n[{i}/{len(albums)}] Processing album: {album_name}")
        logger.info(f"  Location: {' / '.join(parent_path_list)}")
        logger.info(f"  Expected members: {expected_count}")

        # Ensure album exists
        if not ensure_album_in_photos(parent_path_list, album_name):
            logger.error(f"  ❌ Failed to create album '{album_name}' at {' / '.join(parent_path_list)}")
            failed_albums.append(full_path)
            continue

        # Fetch expected asset IDs from database
        cursor.execute("""
            SELECT asset_id, original_filename
            FROM media_organizer_album_assets
            WHERE album_pk = ?
            ORDER BY date_created_utc ASC, original_filename ASC
        """, (album_pk,))
        db_members = cursor.fetchall()
        expected_asset_ids = [m[0] for m in db_members]

        if not expected_asset_ids:
            logger.info("  ℹ️ Album has 0 members in backup. Empty album created.")
            continue

        # Check existing assets in Apple Photos album to avoid duplicate work
        existing_uuids, _ = get_existing_album_asset_ids(parent_path_list, album_name)
        already_present_count = len(set(expected_asset_ids).intersection(existing_uuids))
        total_already_present += already_present_count

        missing_asset_ids = [aid for aid in expected_asset_ids if aid not in existing_uuids]

        if not missing_asset_ids:
            logger.info(f"  ✨ All {already_present_count} assets already present in album.")
            continue

        logger.info(f"  📸 Adding {len(missing_asset_ids)} missing asset(s) (Already present: {already_present_count})...")

        # Chunk missing assets into batches
        album_added_count = 0
        for chunk_idx in range(0, len(missing_asset_ids), batch_size):
            chunk = missing_asset_ids[chunk_idx:chunk_idx + batch_size]
            batch_num = (chunk_idx // batch_size) + 1
            total_batches = ((len(missing_asset_ids) - 1) // batch_size) + 1

            added = add_assets_to_album_batch(parent_path_list, album_name, chunk)
            album_added_count += added
            total_added_assets += added

            logger.info(f"    Batch {batch_num}/{total_batches}: added {added}/{len(chunk)} assets.")

        logger.info(f"  ✅ Finished album '{album_name}': {album_added_count} added, {already_present_count} already present.")

    # Summary Report
    print("\n" + "=" * 80)
    print("🏁 RECONSTRUCTION SUMMARY")
    print("=" * 80)
    print(f"📁 Folders verified/created: {created_folders}/{len(folders)}")
    print(f"📂 Albums reconstructed:     {len(albums) - len(failed_albums)}/{len(albums)}")
    print(f"🖼️  Assets added:            {total_added_assets}")
    print(f"✨ Assets already present:   {total_already_present}")
    if failed_albums:
        print(f"❌ Failed Albums ({len(failed_albums)}):")
        for fa in failed_albums:
            print(f"   - {fa}")
    print("=" * 80 + "\n")
    logger.info("🏁 Apple Photos album reconstruction process finished.")


def main():
    parser = argparse.ArgumentParser(
        description="Reconstruct Apple Photos albums structure and members from Media Organizer DB backup."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--recreate", "--restore",
        action="store_true",
        help="Recreate folders and albums and restore member assets in Apple Photos"
    )
    group.add_argument(
        "--backup", "--sync",
        action="store_true",
        help="Snapshot/backup current Apple Photos album structure & members into media_organizer.db"
    )
    group.add_argument(
        "--verify",
        action="store_true",
        help="Verify Apple Photos folders, albums, and member counts against DB backup"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview folders and albums to be created without modifying Apple Photos (use with --recreate)"
    )
    parser.add_argument(
        "--folder",
        type=str,
        default=None,
        help="Filter reconstruction/verification to a specific subfolder (e.g. 'Moments', 'Curated', 'Retired')"
    )
    parser.add_argument(
        "--album",
        type=str,
        default=None,
        help="Filter reconstruction/verification to a specific album name"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=300,
        help="Number of assets to add in a single AppleScript call (default: 300)"
    )

    args = parser.parse_args()

    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"❌ Media Organizer database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)

    try:
        if args.backup:
            success = sync_album_backup(conn, logger)
            sys.exit(0 if success else 1)

        cursor = conn.cursor()

        # Check if backup table exists and has records
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='media_organizer_albums'")
        if cursor.fetchone()[0] == 0:
            logger.error("❌ 'media_organizer_albums' table does not exist. Please run migrations first.")
            sys.exit(1)

        cursor.execute("SELECT count(*) FROM media_organizer_albums")
        count = cursor.fetchone()[0]
        if count == 0 and not args.dry_run:
            logger.warning("⚠️ No albums are currently backed up in media_organizer.db.")
            logger.info("🔄 Running initial backup snapshot from Photos DB now...")
            if not sync_album_backup(conn, logger):
                logger.error("❌ Failed to create initial album backup.")
                sys.exit(1)

        if args.verify:
            verify_apple_photos_structure(cursor, filter_folder=args.folder, filter_album=args.album)
        elif args.recreate:
            reconstruct_apple_albums(
                cursor,
                filter_folder=args.folder,
                filter_album=args.album,
                dry_run=args.dry_run,
                batch_size=args.batch_size
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
