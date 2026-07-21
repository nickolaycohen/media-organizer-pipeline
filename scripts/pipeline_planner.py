import sys
import os
import subprocess
import re
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from utils.logger import setup_logger
from constants import LOG_PATH, STAGING_ROOT
from utils.utils import get_full_transition_path, human_readable_size
from google_photos import check_google_quota, authenticate, get_all_favorites
import argparse
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH, LOG_PATH, GOOGLE_PHOTOS_READONLY_SCOPES, GOOGLE_DRIVE_READ_ONLY_SCOPES, PLANNER_REQUIRED_SCOPES
from constants import ACTIVE_CAMERA_MODELS
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_stage_transitions, get_batch_statuses, get_latest_import_and_month
import requests
from datetime import timezone, datetime, timedelta
 

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}

logger = setup_logger(LOG_PATH, "pipeline_planner")
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

def set_planned_month(cursor, month):
    cursor.execute("DELETE FROM planned_execution")
    cursor.execute("INSERT INTO planned_execution (planned_month, active) VALUES (?, 1)", (month,))

def should_run_sync_metadata(cursor):
    """
    Determine whether the sync_photos_metadata.py step should run.
    It should run if the last successful sync is older than the Apple Photos DB modification time.
    """
    # Get last successful sync time from a hypothetical table sync_status
    cursor.execute("""
        SELECT MAX(executed_at_utc)
        FROM pipeline_executions
        WHERE (label = '0.3 Sync Metadata' OR label = '0.3 Sync assets' OR label = '0.3 Sync metadata') AND status = 'success'
    """)
    last_sync = cursor.fetchone()
    last_sync_time = last_sync[0] if last_sync else None

    # Get Apple Photos DB modification time
    # APPLE_PHOTOS_DB_COPY_PATH = os.path.expanduser("~/Pictures/Photos Library.photoslibrary/database/Photos.sqlite")
    if not os.path.exists(APPLE_PHOTOS_DB_COPY_PATH):
        return True  # If DB does not exist, better to run sync

    # Check if necessary columns exist in imports table
    try:
        cursor.execute("SELECT min_date, max_date, months_detected FROM imports LIMIT 1")
    except sqlite3.OperationalError:
        logger.info("Schema mismatch detected in 'imports' table. Resetting sync flags to force metadata sync.")
        cursor.execute("UPDATE db_updates SET raw_synced = 0, derived_synced = 0")
        cursor.connection.commit()
        return True

    # Check if ranked_assets_view exists
    try:
        cursor.execute("SELECT score_normalized FROM ranked_assets_view LIMIT 1")
    except sqlite3.OperationalError:
        logger.info("Missing 'ranked_assets_view' detected. Resetting sync flags to force metadata sync.")
        cursor.execute("UPDATE db_updates SET raw_synced = 0, derived_synced = 0")
        cursor.connection.commit()
        return True

    db_mod_time = os.path.getmtime(APPLE_PHOTOS_DB_COPY_PATH)

    if last_sync_time is None:
        return True

    last_sync_timestamp = datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").timestamp()

    return db_mod_time > last_sync_timestamp

# Helper to run bootstrap steps
def run_bootstrap_steps(auto_apply, logger):
    """
    Run the bootstrap steps: copy_all_media_db.py, storage_status.py, sync_photos_metadata.py.
    """
    steps = [
        ("0.0 Copy all media DB", "copy_all_media_photos_db.py", []),
        ("0.1 Run storage manager", "storage_manager_main.py", ["--migrate"]),
        ("0.2 Sync assets", "sync_photos_raw.py", []),
        ("0.3 Sync metadata", "sync_photos_derived.py", ["--force"]),
        ("1.0 Generate Batches", "generate_month_batches.py", [])
    ]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for step_name, script_file, step_args in steps:
        script_path = os.path.join(script_dir, script_file)
        logger.info(f"🔧 Running bootstrap step: {step_name} ({script_file})")
        try:
            if script_file in ["sync_photos_raw.py", "sync_photos_derived.py", "sync_photos_metadata.py"]:
                # Isolate the check so the connection is definitely closed before the subprocess starts
                tmp_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
                tmp_conn.execute("PRAGMA journal_mode=WAL;")
                tmp_conn.execute("PRAGMA busy_timeout = 30000;")
                tmp_cursor = tmp_conn.cursor()
                should_sync = should_run_sync_metadata(tmp_cursor)
                tmp_conn.close()
                
                if not should_sync:
                    logger.info(f"Skipping {script_file} as sync is up to date.")
                    continue
            subprocess.run(["python3", script_path] + step_args, check=True)
            logger.info(f"✅ Completed: {step_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error in bootstrap step {step_name}: {e}")
            if script_file == "storage_status.py":
                logger.error("Storage status check failed. Exiting planner due to migration failure.")
                sys.exit(1)
            else:
                sys.exit(1)

def prompt_asset_level_triage(cursor, conn, import_uuids, camera_model, camera_make, month):
    """
    Prompts the user to ignore assets one by one for a given import/month/camera.
    """
    placeholders = ','.join(['?' for _ in import_uuids])
    cursor.execute(f"""
        SELECT a.original_filename, a.date_created_utc, a.asset_id
        FROM assets a
        JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        WHERE a.import_id IN ({placeholders})
          AND a.month = ?
          AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = ?
          AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = ?
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
        ORDER BY a.date_created_utc
    """, import_uuids + [month, camera_model, camera_make])
    
    assets = cursor.fetchall()
    if not assets:
        print(f"No active assets found to triage for {camera_make} {camera_model} in {month}.")
        return

    print(f"\n--- Asset-level Triage for {camera_make} {camera_model} ({month}) ---")
    ignored_any = False
    for fname, dt, asset_id in assets:
        choice = input(f"  Ignore {fname} ({dt})? [y/N]: ").strip().lower()
        if choice == 'y':
            cursor.execute("UPDATE assets SET ignore_continuity_check = 1 WHERE asset_id = ?", (asset_id,))
            ignored_any = True
            print(f"  ✅ Asset {fname} ignored.")
    
    if ignored_any:
        conn.commit()
        print("\nTriaging complete. Please re-run the planner to see updated reasonability metrics.")
        sys.exit(0)

def check_active_sources_import_status(cursor, conn, month, auto_apply):
    """
    Checks if all active camera models have imported assets for the proposed month.
    Prompts user if any active source is missing.
    """
    if not ACTIVE_CAMERA_MODELS:
        logger.info("No active camera models configured. Skipping active source check.")
        return True

    months_to_check = [month]

    try:
        cursor.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_COPY_PATH}' AS photos_db;")
        logger.debug("Attached Photos.sqlite database for active source check.")

        for month_str in months_to_check:
            source_metadata = []
            # We use conditional aggregation (CASE WHEN) to get the range for the target month 
            # while still being able to group by camera model.
            query = """
                SELECT 
                    xa.ZCAMERAMODEL,
                    xa.ZCAMERAMAKE,
                    COUNT(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN 1 END) AS assets_in_month,
                    MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN aaa.ZORIGINALFILENAME END) AS min_filename,
                    MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN aaa.ZORIGINALFILENAME END) AS max_filename,
                    MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime') END) AS min_date,
                    MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime') END) AS max_date,
                    GROUP_CONCAT(DISTINCT CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? 
                                               THEN a.ZIMPORTSESSION END) AS involved_import_ids
                FROM photos_db.ZASSET a
                JOIN photos_db.ZEXTENDEDATTRIBUTES xa ON xa.ZASSET = a.Z_PK
                JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                JOIN imports i ON i.import_uuid = a.ZIMPORTSESSION 
                              AND i.camera_model = xa.ZCAMERAMODEL
                LEFT JOIN assets loc ON loc.asset_id = a.ZUUID
                WHERE a.ZTRASHEDSTATE = 0
                  AND (loc.ignore_continuity_check = 0 OR loc.ignore_continuity_check IS NULL)
                  AND xa.ZCAMERAMODEL IN ({})
                  AND a.ZDATECREATED >= (strftime('%s', date(?, 'start of month', '-12 month')) - 978307200)
                  AND a.ZDATECREATED < (strftime('%s', date(?, 'start of month', '+2 month')) - 978307200)
                GROUP BY xa.ZCAMERAMODEL, xa.ZCAMERAMAKE
            """.format(','.join(['?' for _ in ACTIVE_CAMERA_MODELS]))

            cursor.execute(query, [month_str] * 6 + ACTIVE_CAMERA_MODELS + [month_str + "-01", month_str + "-01"])
            results = cursor.fetchall()
            found_models = set()

            for row in results:
                model, make, count, f_min, f_max, d_min, d_max, involved_import_ids = row
                num_min = None
                num_max = None
                gap_info = ""
                if count > 0:
                    found_models.add(model)

                    # Reasonability check: parse numeric part from filenames (ignoring extensions)
                    # We only attempt this if the filename looks like a standard sequential pattern (Prefix + Digits)
                    seq_pattern = r'^([a-zA-Z_-]+)(\d+)$'
                    if f_min:
                        m = re.match(seq_pattern, os.path.splitext(f_min)[0])
                        if m: num_min = int(m.group(2))
                    if f_max:
                        m = re.match(seq_pattern, os.path.splitext(f_max)[0])
                        if m: num_max = int(m.group(2))

                    if num_min is not None and num_max is not None:
                        # We use abs because string MIN/MAX might flip if sequence is not zero-padded
                        expected_range = abs(num_max - num_min) + 1
                        if expected_range > count:
                            gap_info = f" | ⚠️ Reasonability: {expected_range} expected vs {count} found (gap of {expected_range - count})"

                # Continuity check with previous month's confirmed imports
                continuity_info = ""
                previous_month = (datetime.strptime(month_str, '%Y-%m').replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
                cursor.execute("""
                    SELECT MAX(max_filename), MAX(max_date)
                    FROM imports
                    WHERE camera_model = ? AND months_detected LIKE ? AND sequencing_confirmed = 1
                """, (model, f'%{previous_month}%')) # Use LIKE for months_detected as it's comma-separated
                prev_month_data = cursor.fetchone()
                prev_max_filename, prev_max_date = prev_month_data if prev_month_data else (None, None)

                if prev_max_filename and num_min is not None:
                    prev_num_max = None
                    prev_nums = re.findall(r'(\d+)', os.path.splitext(prev_max_filename)[0])
                    if prev_nums: prev_num_max = int(prev_nums[-1])

                    if prev_num_max is not None and num_min > prev_num_max + 1:
                        continuity_info += f" | ⚠️ Filename gap from {previous_month}: {prev_max_filename} -> {f_min}"
                    elif prev_num_max is not None and num_min <= prev_num_max:
                        continuity_info += f" | ⚠️ Filename overlap/reset from {previous_month}: {prev_max_filename} -> {f_min}"
                
                if prev_max_date and d_min:
                    # Simple date string comparison for YYYY-MM-DD HH:MM:SS
                    if d_min < prev_max_date:
                        continuity_info += f" | ⚠️ Date overlap from {previous_month}: {prev_max_date} -> {d_min}"

                logger.info(f"📸 Month: {month_str} | Source: {model:20} | Count: {count:4} | Files: {f_min} -> {f_max} | Dates: {d_min} to {d_max}{gap_info}{continuity_info}")
                source_metadata.append({
                    'row': row,
                    'gap_info': gap_info,
                    'continuity_info': continuity_info
                })

            missing_models = set(ACTIVE_CAMERA_MODELS) - found_models

            if missing_models:
                logger.warning(f"⚠️ Missing imports for active camera models in {month_str}: {', '.join(missing_models)}")
                if not auto_apply:
                    proceed_input = input(f"Some active sources are missing imports for {month_str}. Do you want to proceed? [y/N]: ")
                    if proceed_input.strip().lower() != 'y':
                        logger.info("Operation aborted by user due to missing active source imports.")
                        sys.exit(0)
                else:
                    logger.error(f"❌ Auto-apply aborted: Missing active source imports for {month_str}. Manual intervention required.")
                    sys.exit(1)

            # Per-source sequencing confirmation
            if not auto_apply:
                for entry in source_metadata:
                    row = entry['row']
                    gap_info = entry['gap_info']
                    continuity_info = entry['continuity_info']
                    model, make, count, f_min, f_max, d_min, d_max, involved_import_ids = row

                    if count == 0 or not involved_import_ids:
                        continue
                    
                    # Extract individual import IDs from the concatenated string
                    import_id_list = involved_import_ids.split(',')
                    placeholders = ','.join(['?' for _ in import_id_list])
                    
                    # Check which of these involved imports are still unconfirmed in our local table
                    cursor.execute("""
                        SELECT COUNT(*) FROM imports
                        WHERE import_uuid IN ({}) AND camera_model = ?
                          AND (sequencing_confirmed = 0 OR sequencing_confirmed IS NULL)
                    """.format(placeholders), import_id_list + [model])
                    unconfirmed_count = cursor.fetchone()[0]

                    #   TODO: Before the promt we should check confirmed months for each source in comparison to months in the past or in the future relative to the proposed month
                    if unconfirmed_count > 0:
                        # Determine naming pattern to filter context to relevant conventions
                        pattern = "*"
                        if f_min:
                            # Only use a prefix filter if it looks like a standard sequence (Prefix + Digits)
                            stem = os.path.splitext(f_min)[0]
                            m = re.match(r'^([a-zA-Z_-]+)\d+$', stem)
                            if m:
                                pattern = m.group(1) + "*"

                        # Fetch global boundaries for this model before and after the current month
                        cursor.execute("""
                            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
                            FROM imports
                            WHERE camera_model = ? AND max_date < ? AND min_filename GLOB ?
                        """, (model, f"{month_str}-01 00:00:00", pattern))
                        b = cursor.fetchone()
                        before_str = f"  Before:  {b[0]} -> {b[1]} ({b[2]} to {b[3]})" if b and b[1] else "  Before:  None"

                        cursor.execute("""
                            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
                            FROM imports
                            WHERE camera_model = ? AND min_date >= date(?, 'start of month', '+1 month') AND min_filename GLOB ?
                        """, (model, f"{month_str}-01", pattern))
                        a = cursor.fetchone()
                        after_str = f"  After:   {a[0]} -> {a[1]} ({a[2]} to {a[3]})" if a and a[0] else "  After:   None"

                        print(f"Verifying {model} for {month_str}:")
                        print(before_str)
                        choice = input(
                            f"  Current: {f_min} -> {f_max} ({d_min} to {d_max}){gap_info}{continuity_info}\n"
                            f"{after_str}\n"
                            f"Mark as reasonable? [I/n]: "
                        ).strip().upper()

                        if choice == 'I':
                            for import_uuid in import_id_list:
                                # Calculate metadata specific to this individual import_uuid for the month
                                cursor.execute("""
                                    SELECT 
                                        MIN(aaa.ZORIGINALFILENAME),
                                        MAX(aaa.ZORIGINALFILENAME),
                                        MIN(datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
                                        MAX(datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime'))
                                    FROM photos_db.ZASSET a
                                    JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                                    LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
                                    LEFT JOIN assets loc ON loc.asset_id = a.ZUUID
                                    WHERE a.ZIMPORTSESSION = ?
                                      AND (loc.ignore_continuity_check = 0 OR loc.ignore_continuity_check IS NULL)
                                      AND strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ?
                                      AND COALESCE(ea.ZCAMERAMODEL, 'Unknown') = ?
                                      AND COALESCE(ea.ZCAMERAMAKE, 'Unknown') = ?
                                """, (import_uuid, month_str, model, make or "Unknown"))
                                res = cursor.fetchone()
                                if res:
                                    s_f_min, s_f_max, s_d_min, s_d_max = res
                                    cursor.execute("""
                                        UPDATE imports 
                                        SET sequencing_confirmed = 1,
                                            min_filename = COALESCE(min_filename, ?),
                                            max_filename = COALESCE(max_filename, ?),
                                            min_date = COALESCE(min_date, ?),
                                            max_date = COALESCE(max_date, ?)
                                    WHERE import_uuid = ? AND camera_model = ?
                                """, (s_f_min, s_f_max, s_d_min, s_d_max, import_uuid, model))
                            conn.commit()
                            logger.info(f"✅ Marked involved imports for {model} in {month_str} as reasonable and updated metadata individually.")
                        else:
                            print(f"\n❌ Reasonability rejected for {model} in {month_str}. Listing involved assets:")
                            cursor.execute(f"""
                                SELECT aaa.ZORIGINALFILENAME, datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime'), a.ZUUID
                                FROM photos_db.ZASSET a
                                JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                                LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
                                WHERE a.ZIMPORTSESSION IN ({placeholders})
                                  AND COALESCE(ea.ZCAMERAMODEL, 'Unknown') = ?
                                  AND COALESCE(ea.ZCAMERAMAKE, 'Unknown') = ?
                                  AND strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ?
                                ORDER BY aaa.ZORIGINALFILENAME
                            """, import_id_list + [model, make or "Unknown", month_str])
                            for fname, dt, uuid in cursor.fetchall():
                                print(f"  - {fname} ({dt}) [UUID: {uuid}]")
                            logger.error("Execution halted by user. Source data needs fixing.")
                            sys.exit(1)
    finally:
        cursor.execute("DETACH DATABASE photos_db;")
        logger.debug("Detached Photos.sqlite database.")
    return True

def check_favorites_count(cursor, month, check_remote=False, all_favs=None, creds=None, verbose=True):
    """
    Checks for favorites in local DB or optionally Google Photos API.
    Used to verify readiness for manual transitions or pull/ranking steps.
    """
    cursor.execute("SELECT original_filename FROM assets WHERE month = ? AND google_favorite = 1", (month,))
    local_fav_names = [row[0] for row in cursor.fetchall()]
    local_count = len(local_fav_names)
    
    if local_count > 0 or not check_remote:
        if verbose:
            logger.info(f"📊 Favorites check for {month}: Found {local_count} starred assets in local database.")
        return local_count, "local", local_fav_names
        
    try:
        if verbose: logger.info(f"🌐 Local database has 0 favorites for {month}. Calling Google Photos API to verify curation status...")
        if all_favs is None:
            if creds is None:
                creds = authenticate(scopes=GOOGLE_PHOTOS_READONLY_SCOPES)
            all_favs = get_all_favorites(creds)
        else:
            if verbose: logger.info(f"Using {len(all_favs)} cached favorites from current session.")
        if verbose: logger.info(f"✅ API Response: {len(all_favs)} total favorites retrieved from account.")
        
        cursor.execute("SELECT original_filename, date_created_utc FROM assets WHERE month = ?", (month,))
        local_assets = cursor.fetchall()
        
        fav_signatures = set()
        for f in all_favs:
            fname = f.get('filename')
            q_time = f.get('mediaMetadata', {}).get('creationTime', '')
            if fname and q_time:
                ts = q_time.replace('T', ' ').split('.')[0]
                fav_signatures.add((fname, ts))
        
        remote_count = 0
        matched_files = []
        for fname, ts in local_assets:
            if (fname, ts) in fav_signatures:
                remote_count += 1
                matched_files.append(fname)
        if matched_files and verbose:
            logger.info(f"✨ Successfully matched remote favorites: {matched_files}")
        if verbose:
            logger.info(f"📊 Cross-reference result for {month}: Found {remote_count} assets matching global favorites list.")
        return remote_count, "remote", matched_files
    except Exception as e:
        logger.warning(f"Could not verify remote favorites: {e}")
        return 0, "error", []

def verify_sequencing_for_planned_month(cursor, conn, month, auto_apply):
    """
    Checks if imports associated with the planned month have sequencing confirmed.
    Prompts the user if confirmation is missing.
    """
    cursor.execute("""
        SELECT DISTINCT i.import_uuid, i.camera_model, i.camera_make
        FROM imports i
        JOIN ZASSET za ON za.ZIMPORTSESSION = i.import_uuid
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        LEFT JOIN assets a ON a.asset_id = za.ZUUID
        WHERE a.month = ?
          AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(i.camera_model, 'Unknown')
          AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(i.camera_make, 'Unknown')
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
          AND (i.sequencing_confirmed = 0 OR i.sequencing_confirmed IS NULL)
    """, (month,))
    unconfirmed = cursor.fetchall()

    if not unconfirmed:
        return True

    logger.info(f"🧐 Found {len(unconfirmed)} import sessions for {month} requiring sequencing confirmation.")
    for uuid, model, make in unconfirmed:
        # Recalculate metrics based on non-ignored assets matching this specific import row's camera
        cursor.execute("""
            SELECT MIN(aaa.ZORIGINALFILENAME), MAX(aaa.ZORIGINALFILENAME), COUNT(za.Z_PK),
                   MIN(datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
                   MAX(datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime'))
            FROM ZASSET za
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
            LEFT JOIN assets a ON a.asset_id = za.ZUUID
            WHERE za.ZIMPORTSESSION = ?
              AND a.month = ?
              AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(?, 'Unknown')
              AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(?, 'Unknown')
              AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
        """, (uuid, month, model, make))
        
        f_min, f_max, count, d_min, d_max = cursor.fetchone()

        if not count:
            continue

        if not model:
            model = "Unknown Model"

        # Reasonability check: parse numeric part from filenames
        seq_pattern = r'^([a-zA-Z_-]+)(\d+)$'
        num_min = None
        if f_min:
            m = re.match(seq_pattern, os.path.splitext(f_min)[0])
            if m: num_min = int(m.group(2))
            
        num_max = None
        if f_max:
            m = re.match(seq_pattern, os.path.splitext(f_max)[0])
            if m: num_max = int(m.group(2))

        gap_str = ""
        if num_min is not None and num_max is not None:
            expected = abs(num_max - num_min) + 1
            gap = expected - count if expected > count else 0
            if gap > 0:
                gap_str = f" | ⚠️ Gap detected: {gap} items"
        logger.info(f"   - Session {uuid} ({model}): {f_min} -> {f_max} ({d_min} to {d_max}) ({count} files){gap_str}")

        if auto_apply:
            continue

        # Determine naming pattern to filter context to relevant conventions
        pattern = "*"
        if f_min:
            # Only use a prefix filter if it looks like a standard sequence (Prefix + Digits)
            stem = os.path.splitext(f_min)[0]
            m = re.match(r'^([a-zA-Z_-]+)\d+$', stem)
            if m:
                pattern = m.group(1) + "*"

        # Fetch global boundaries for this model before and after the current month
        cursor.execute("""
            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
            FROM imports
            WHERE camera_model = ? AND max_date < ? AND min_filename GLOB ?
        """, (model, f"{month}-01 00:00:00", pattern))
        b = cursor.fetchone()
        before_str = f"  Before:  {b[0]} -> {b[1]} ({b[2]} to {b[3]})" if b and b[1] else "  Before:  None"

        cursor.execute("""
            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
            FROM imports
            WHERE camera_model = ? AND min_date >= date(?, 'start of month', '+1 month') AND min_filename GLOB ?
        """, (model, f"{month}-01", pattern))
        a = cursor.fetchone()
        after_str = f"  After:   {a[0]} -> {a[1]} ({a[2]} to {a[3]})" if a and a[0] else "  After:   None"

        print(f"Verifying {model} session {uuid} for {month}:")
        print(before_str)
        choice = input(
            f"  Current: {f_min} -> {f_max} ({d_min} to {d_max}) ({count} files){gap_str}\n"
            f"{after_str}\n"
            f"Mark as reasonable? [I/n]: "
        ).strip().upper()

        if choice == 'I':
            # Calculate missing metadata from assets table
            cursor.execute("""
                SELECT MIN(original_filename), MAX(original_filename), MIN(date_created_utc), MAX(date_created_utc)
                FROM assets WHERE import_id = ?
            """, (uuid,))
            calc_f_min, calc_f_max, calc_d_min, calc_d_max = cursor.fetchone()

            cursor.execute("""
                UPDATE imports 
                SET sequencing_confirmed = 1,
                    min_filename = COALESCE(min_filename, ?),
                    max_filename = COALESCE(max_filename, ?),
                    min_date = COALESCE(min_date, ?),
                    max_date = COALESCE(max_date, ?)
                WHERE import_uuid = ? AND camera_model = ?
            """, (calc_f_min, calc_f_max, calc_d_min, calc_d_max, uuid, model))
            conn.commit()
            logger.info(f"✅ Marked import {uuid} for {model} as reasonable and updated metadata.")
        else:
            print(f"\n❌ Reasonability rejected for {model} (session {uuid}). Listing active assets:")
            cursor.execute("""
                SELECT aaa.ZORIGINALFILENAME, datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime'), za.ZUUID
                FROM ZASSET za
                JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                LEFT JOIN assets a ON a.asset_id = za.ZUUID
                WHERE za.ZIMPORTSESSION = ?
                  AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(?, 'Unknown')
                  AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(?, 'Unknown')
                  AND a.month = ?
                  AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
                ORDER BY aaa.ZORIGINALFILENAME
            """, (uuid, model, make, month))
            for fname, dt, asset_uuid in cursor.fetchall():
                print(f"  - {fname} ({dt}) [UUID: {asset_uuid}]")

            asset_choice = input(f"\nWould you like to triage assets one by one for {model} (session {uuid}) to ignore specific items? [y/N]: ").strip().lower()
            if asset_choice == 'y':
                prompt_asset_level_triage(cursor, conn, [uuid], model or "Unknown", make or "Unknown", month)

            logger.error("Execution halted by user. Source data needs fixing.")
            sys.exit(1)

    if auto_apply:
        return True

    # Final check: are there any remaining unconfirmed sessions for this month?
    cursor.execute("""
        SELECT COUNT(*)
        FROM assets a
        JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        JOIN imports i ON i.import_uuid = a.import_id 
          AND COALESCE(i.camera_model, 'Unknown') = COALESCE(zea.ZCAMERAMODEL, 'Unknown')
        WHERE a.month = ? 
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
          AND (i.sequencing_confirmed = 0 OR i.sequencing_confirmed IS NULL)
    """, (month,))
    remaining = cursor.fetchone()[0]
    return remaining == 0


def display_summary(transitions, batches, cursor, remote_favs_cache=None):
    print("\n=== 📊 Stage Transitions ===")
    for code, prev, desc, ttype, label in transitions:
        print(f"{prev} ➜ {code}: {desc} (Type: {ttype})")

    print("\n=== 📦 Batch Statuses ===")
    for month, status in batches:
        print(f"Month: {month}, Status: {status}")

    if remote_favs_cache:
        # Build a lookup of (filename, timestamp) -> local batch month
        cursor.execute("SELECT original_filename, date_created_utc, month FROM assets")
        local_mapping = {(row[0], row[1]): row[2] for row in cursor.fetchall()}
        
        fav_counts = {}
        for item in remote_favs_cache:
            fname = item.get('filename')
            creation_time = item.get('mediaMetadata', {}).get('creationTime')
            
            if creation_time:
                # Convert Google format '2026-04-18T23:00:00Z' to local format '2026-04-18 23:00:00'
                ts = creation_time.replace('T', ' ').split('.')[0]
                
                # Group by the local batch month if the asset is recognized, 
                # otherwise fallback to Google's raw month metadata.
                month_key = local_mapping.get((fname, ts), creation_time[:7])
                fav_counts[month_key] = fav_counts.get(month_key, 0) + 1
        
        if fav_counts:
            print("\n=== ⭐ Remote Favorites Matched to Local Batches ===")
            for month in sorted(fav_counts.keys(), reverse=True):
                print(f"Month: {month}, Favorites: {fav_counts[month]}")

def run_memory_publishing_flow(cursor, conn):
    logger.info("🎨 Starting Memory Feature & Publishing session...")
    from constants import CURATED_LACIE_DIR, TO_BE_CURATED_DIR
    import math

    # Create threshold_history table if it doesn't exist
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS threshold_history (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at_utc    TEXT NOT NULL DEFAULT (datetime('now')),
                threshold_score     REAL NOT NULL,
                notes               TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not initialize threshold_history table: {e}")

    # Fetch historical minimum threshold
    historical_min = 0.0
    try:
        cursor.execute("SELECT MIN(threshold_score) FROM threshold_history WHERE threshold_score > 0.0")
        row = cursor.fetchone()
        if row and row[0] is not None:
            historical_min = row[0]
            logger.info(f"Loaded historical minimum threshold from DB: {historical_min:.4f}")
    except Exception as e:
        logger.warning(f"Could not fetch historical minimum threshold: {e}")
    
    while True:
        # Clear/rollback any open transactions to get a fresh snapshot of the database
        try:
            conn.rollback()
        except Exception:
            pass

        # Try attaching Apple Photos DB copy to fetch Apple's auto-generated moments and filter ignored items
        photos_db_attached = False
        try:
            from constants import APPLE_PHOTOS_DB_COPY_PATH
            # Pre-open direct connection to force SQLite recovery/WAL resolution on the copy
            try:
                temp_conn = sqlite3.connect(APPLE_PHOTOS_DB_COPY_PATH, timeout=5)
                temp_conn.execute("SELECT 1;")  # quick query to force file access/recovery
                temp_conn.close()
            except Exception as e:
                logger.debug(f"Pre-open of Photos.sqlite copy failed/warned: {e}")

            cursor.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_COPY_PATH}' AS photos_db;")
            photos_db_attached = True
        except Exception as e:
            logger.warning(f"Could not attach Photos.sqlite for Apple moment lookup: {e}")

        # Fetch the cutoff threshold score (dynamically on each loop iteration, excluding Ignore folder items)
        cutoff_score = 0.0
        if photos_db_attached:
            try:
                cursor.execute("""
                    SELECT v.score_normalized FROM ranked_assets_view v
                    JOIN month_batches mb ON v.month = mb.month
                    LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                    WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '') 
                      AND (a.Z_PK IS NULL OR NOT EXISTS (
                          SELECT 1 FROM photos_db.Z_30ASSETS aa
                          JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                          WHERE aa.Z_3ASSETS = a.Z_PK
                            AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                            AND ga.ZTRASHEDSTATE = 0
                      ))
                    ORDER BY v.score_normalized DESC LIMIT 1
                """)
                row = cursor.fetchone()
                cutoff_score = row[0] if row and row[0] is not None else 0.0
            except Exception as e:
                logger.warning(f"Error querying cutoff score with photos_db: {e}")
        
        if cutoff_score == 0.0:
            try:
                cursor.execute("""
                    SELECT v.score_normalized FROM ranked_assets_view v
                    JOIN month_batches mb ON v.month = mb.month
                    WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '') 
                    ORDER BY v.score_normalized DESC LIMIT 1
                """)
                row = cursor.fetchone()
                cutoff_score = row[0] if row and row[0] is not None else 0.0
            except Exception:
                pass
                
        logger.info(f"Cutoff threshold score: {cutoff_score:.4f}")

        # Record cutoff score in threshold_history if it is a valid positive value
        if cutoff_score > 0.0:
            try:
                cursor.execute("INSERT INTO threshold_history (threshold_score) VALUES (?)", (cutoff_score,))
                conn.commit()
                # Update running historical_min if this is the first recorded threshold or it is smaller
                if historical_min == 0.0 or cutoff_score < historical_min:
                    historical_min = cutoff_score
            except Exception as e:
                logger.warning(f"Could not record threshold in history: {e}")

        # Display threshold status summary
        print("\n==================================================")
        print("📊 Curation Threshold Status")
        print("==================================================")
        print(f" - Current dynamic threshold:  {cutoff_score:.4f}")
        if historical_min > 0.0:
            print(f" - Historical minimum target:  {historical_min:.4f}")
            if cutoff_score > historical_min:
                print(f"👉 Note: Please assign moments to assets in new batches until the threshold reaches {historical_min:.4f} again.")
            else:
                print(f"🎉 Threshold aligned! Current threshold matches or is below historical minimum.")
        else:
            print(" - Historical minimum target:  None (No history recorded yet)")
            print("👉 Note: Once you begin assigning moments, the lowest dynamic threshold reached will be tracked.")
        print("==================================================\n")

        # Determine effective cutoff threshold to use for selecting qualified moments in the table
        effective_threshold = cutoff_score
        if historical_min > 0.0:
            effective_threshold = min(cutoff_score, historical_min) if cutoff_score > 0.0 else historical_min

        # Check for highly ranked assets that do not belong to any Moment in Apple Photos (excluding Ignore items)
        if photos_db_attached:
            cursor.execute("""
                SELECT 
                    v.original_filename, 
                    v.score_normalized, 
                    v.month, 
                    v.date_created_utc,
                    m.ZTITLE,
                    m.ZSUBTITLE
                FROM ranked_assets_view v
                JOIN month_batches mb ON v.month = mb.month
                LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                LEFT JOIN photos_db.ZMOMENT m ON a.ZMOMENT = m.Z_PK
                WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '')
                  AND v.score_normalized > 0.50
                  AND (a.Z_PK IS NULL OR NOT EXISTS (
                      SELECT 1 FROM photos_db.Z_30ASSETS aa
                      JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                      WHERE aa.Z_3ASSETS = a.Z_PK
                        AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                        AND ga.ZTRASHEDSTATE = 0
                  ))
                ORDER BY v.score_normalized DESC
                LIMIT 5
            """)
        else:
            cursor.execute("""
                SELECT v.original_filename, v.score_normalized, v.month, v.date_created_utc, NULL, NULL
                FROM ranked_assets_view v
                JOIN month_batches mb ON v.month = mb.month
                WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '')
                  AND v.score_normalized > 0.50
                ORDER BY v.score_normalized DESC
                LIMIT 5
            """)

        unassigned = cursor.fetchall()

        if unassigned:
            print("\n==================================================")
            print("⚠️  Unassigned High-Rank Assets (Need Moment Naming Decision)")
            print("==================================================")
            print("The following highly-ranked assets are not assigned to any Moment album in Apple Photos:")
            for fname, score, month, date_created, moment_title, moment_subtitle in unassigned:
                captured_str = "—"
                if date_created:
                    try:
                        dt_utc = None
                        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                            try:
                                dt_utc = datetime.strptime(date_created, fmt)
                                break
                            except ValueError:
                                continue
                        if dt_utc:
                            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                            dt_local = dt_utc.astimezone()
                            captured_str = dt_local.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            captured_str = date_created[:19]
                    except Exception:
                        captured_str = date_created[:19]

                # Construct suggested album name if moment information is available
                moment_parts = []
                if moment_title:
                    moment_parts.append(moment_title.replace('\xa0', ' ').strip())
                if moment_subtitle:
                    moment_parts.append(moment_subtitle.replace('\xa0', ' ').strip())

                suggested_info = ""
                if moment_parts:
                    captured_date = captured_str[:10] if captured_str != "—" else (date_created[:10] if date_created else month)
                    suggested_name = f"{captured_date} - {' - '.join(moment_parts)}"
                    suggested_info = f", Suggested Album: {suggested_name}"

                print(f" - {fname:<25} (Score: {score:.4f}, Captured: {captured_str}, Month: {month}{suggested_info})")
            print("👉 Please consider creating a corresponding album under 'Media Organizer on LaCie / Moments' in Apple Photos (creating the album is sufficient, no need to place the files inside).\n")
        # 2. Query assets that have Moments and are in status >= 600
        if photos_db_attached:
            query = """
                SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, v.original_filename,
                       v.aesthetic_score, v.google_favorite, v.apple_favorite, v.apple_photos_monthly_selection,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'to_be_curated') as is_proposed,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'curated') as is_curated,
                       (SELECT album_name FROM moment_exports me WHERE me.asset_id = v.asset_id ORDER BY exported_at_utc DESC LIMIT 1) as exported_album_name,
                       ast.curated_album
                FROM ranked_assets_view v
                JOIN assets ast ON v.asset_id = ast.asset_id
                JOIN month_batches mb ON v.month = mb.month
                LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                WHERE mb.status_code >= '600' AND v.MomentsAlbumName IS NOT NULL AND v.MomentsAlbumName != ''
                  AND LOWER(v.MomentsAlbumName) NOT IN ('skippublishing', 'ignore')
                  AND v.score_normalized > ?
                  AND (a.Z_PK IS NULL OR NOT EXISTS (
                      SELECT 1 FROM photos_db.Z_30ASSETS aa
                      JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                      WHERE aa.Z_3ASSETS = a.Z_PK
                        AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                        AND ga.ZTRASHEDSTATE = 0
                  ))
                ORDER BY v.score_normalized DESC
            """
        else:
            query = """
                SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, v.original_filename,
                       v.aesthetic_score, v.google_favorite, v.apple_favorite, v.apple_photos_monthly_selection,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'to_be_curated') as is_proposed,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'curated') as is_curated,
                       (SELECT album_name FROM moment_exports me WHERE me.asset_id = v.asset_id ORDER BY exported_at_utc DESC LIMIT 1) as exported_album_name,
                       ast.curated_album
                FROM ranked_assets_view v
                JOIN assets ast ON v.asset_id = ast.asset_id
                JOIN month_batches mb ON v.month = mb.month
                WHERE mb.status_code >= '600' AND v.MomentsAlbumName IS NOT NULL AND v.MomentsAlbumName != ''
                  AND LOWER(v.MomentsAlbumName) NOT IN ('skippublishing', 'ignore')
                  AND v.score_normalized > ?
                ORDER BY v.score_normalized DESC
            """
        cursor.execute(query, (effective_threshold,))
        rows = cursor.fetchall()

        # Display qualified assets scoring components breakdown table
        print("\n=========================================================================================================================")
        print("📸 Qualified Assets Scoring Breakdown")
        print("=========================================================================================================================")
        print(f"{'No.':<4} {'Filename':<25} {'Assigned Album':<30} {'Norm Score':<12} {'Aesthetic':<12} {'Google Fav':<12} {'Apple Fav':<12} {'Monthly Sel':<12}")
        print("-" * 125)
        
        # Calculate counts of assets in each assigned album
        album_counts = {}
        processed_rows = []
        for row in rows:
            assigned_album = row[10] if row[10] else (row[11] if row[11] else "—")
            processed_rows.append((row, assigned_album))
            album_counts[assigned_album] = album_counts.get(assigned_album, 0) + 1
            
        # Sort by: 1. not unassigned ('—' at bottom), 2. album size descending, 3. album name ascending, 4. normalized score descending
        processed_rows.sort(
            key=lambda x: (
                x[1] == "—",
                -album_counts[x[1]],
                x[1],
                -(x[0][2] if x[0][2] is not None else 0.0)
            )
        )
        
        for idx, (row, assigned_album) in enumerate(processed_rows, 1):
            filename = row[3] if row[3] else "—"
            
            score_normalized_val = row[2]
            score_normalized_str = f"{score_normalized_val:.4f}" if score_normalized_val is not None else "—"
            
            aesthetic_score_val = row[4]
            aesthetic_score_str = f"{aesthetic_score_val:.4f}" if aesthetic_score_val is not None else "—"
            
            google_fav = "✅ Yes" if row[5] else "❌ No"
            apple_fav = "✅ Yes" if row[6] else "❌ No"
            monthly_sel = "✅ Yes" if row[7] else "❌ No"
            
            print(f"{idx:<4} {filename:<25} {assigned_album:<30} {score_normalized_str:<12} {aesthetic_score_str:<12} {google_fav:<12} {apple_fav:<12} {monthly_sel:<12}")
        print("=========================================================================================================================\n")
        
        # Group by moment name
        moments_data = {}
        for row in rows:
            asset_id, moment_name, score, filename = row[0], row[1], row[2], row[3]
            is_proposed, is_curated = row[8], row[9]
            if moment_name not in moments_data:
                moments_data[moment_name] = {
                    'total_qualified': 0,
                    'proposed_count': 0,
                    'curated_count': 0,
                    'scores': []
                }
            moments_data[moment_name]['total_qualified'] += 1
            if is_proposed:
                moments_data[moment_name]['proposed_count'] += 1
            if is_curated:
                moments_data[moment_name]['curated_count'] += 1
            moments_data[moment_name]['scores'].append(score)

        # 3. Query Apple Photos albums and folders inside Curated and ToBeCurated (to match existence and get counts)
        applescript_code = """
        tell application "Photos"
            set results to {}
            set parentFolderNames to {"Curated", "ToBeCurated"}
            repeat with fName in parentFolderNames
                if exists folder fName of folder "Media Organizer on LaCie" then
                    set subFolder to folder fName of folder "Media Organizer on LaCie"
                    set subAlbums to albums of subFolder
                    repeat with anAlbum in subAlbums
                        set aName to name of anAlbum
                        try
                            set aCount to count of media items of anAlbum
                        on error
                            set aCount to 0
                        end try
                        copy (fName & "|" & aName & "|" & (aCount as string)) to end of results
                    end repeat
                    set subFolders to folders of subFolder
                    repeat with aFolder in subFolders
                        set aName to name of aFolder
                        copy (fName & "|" & aName & "|0") to end of results
                    end repeat
                end if
            end repeat
            
            set oldDelims to AppleScript's text item delimiters
            set AppleScript's text item delimiters to "\\n"
            set resultsString to results as string
            set AppleScript's text item delimiters to oldDelims
            return resultsString
        end tell
        """
        to_be_curated_albums = {}
        curated_albums = {}
        try:
            process = subprocess.Popen(['osascript', '-e', applescript_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if stdout:
                parts = [p.strip() for p in stdout.strip().split('\n')]
                for p in parts:
                    if '|' in p:
                        subparts = p.split('|')
                        if len(subparts) >= 2:
                            folder_name_clean = subparts[0].strip()
                            album_name_clean = subparts[1].strip()
                            item_count = 0
                            if len(subparts) >= 3:
                                try:
                                    item_count = int(subparts[2].strip())
                                except ValueError:
                                    pass
                            
                            if folder_name_clean == 'ToBeCurated':
                                to_be_curated_albums[album_name_clean] = item_count
                            elif folder_name_clean == 'Curated':
                                curated_albums[album_name_clean] = item_count
        except Exception as e:
            logger.warning(f"Could not list Apple Photos albums: {e}")

        # 4. Fetch memory_stage from curated_moments table
        cursor.execute("SELECT moment_name, memory_stage FROM curated_moments")
        stages = dict(cursor.fetchall())

        # 4.5 Fetch publication information
        cursor.execute("SELECT moment_name, MAX(published_at_utc), COUNT(*) FROM publications GROUP BY moment_name")
        pub_info = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        # 5. Format and display status report
        print("\n==================================================")
        print("🌟 Weekly Memory Feature & Publishing (Mode [M])")
        print("==================================================")
        
        ranked_moments = []
        for name, data in moments_data.items():
            avg_score = sum(data['scores']) / len(data['scores']) if data['scores'] else 0.0
            stage = stages.get(name, 'M100')
            
            # Check Apple Photos existence
            to_be_curated_exists = (name in to_be_curated_albums)
            curated_exists = (name in curated_albums)
            
            # Check filesystem curated directory existence
            fs_curated_exists = os.path.exists(os.path.join(CURATED_LACIE_DIR, name))
            
            # Count-weighted rank score to prevent small/single-asset moments from dominating
            rank_score = avg_score * math.log(data['total_qualified'] + 1)
            
            last_pub_date, pub_count = pub_info.get(name, (None, 0))
            last_pub_str = last_pub_date[:10] if last_pub_date else "—"
            
            # Check if featured/published in less than a month (30 days)
            too_recent = False
            if last_pub_date:
                try:
                    pub_dt = datetime.strptime(last_pub_date.split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        pub_dt = datetime.strptime(last_pub_date, "%Y-%m-%d")
                    except ValueError:
                        pub_dt = None
                
                if pub_dt:
                    diff = datetime.now() - pub_dt
                    if diff.days < 30:
                        too_recent = True
            
            if too_recent:
                can_publish_str = "❌ Recent (<30d)"
            else:
                # Can be published if local folder exists, has assets, and we have more curated assets than published ones
                can_publish_str = "✅ Yes" if (fs_curated_exists and data['curated_count'] > 0 and data['curated_count'] > pub_count) else "❌ No"
            
            # Determine asset count to display (use filesystem count if curated folder exists,
            # fallback to database curated count if present, otherwise total qualified proposed assets)
            fs_curated_path = os.path.join(CURATED_LACIE_DIR, name)
            fs_count = 0
            fs_bases = set()
            if os.path.exists(fs_curated_path):
                try:
                    all_files = [f for f in os.listdir(fs_curated_path) 
                                 if os.path.isfile(os.path.join(fs_curated_path, f)) 
                                 and not f.startswith('.')]
                    # Group by base name to treat Live Photos (HEIC + MOV) as a single asset
                    fs_bases = set(os.path.splitext(f)[0].lower() for f in all_files)
                    fs_count = len(fs_bases)
                except Exception:
                    pass

            if fs_count > 0:
                assets_display = str(fs_count)
            elif name in curated_albums:
                # Use count from Apple Photos Curated album if available (before filesystem export)
                assets_display = str(curated_albums[name])
            elif data['curated_count'] > 0:
                assets_display = str(data['curated_count'])
            else:
                assets_display = str(data['total_qualified'])

            # Compare Apple Photos Curated album assets with local filesystem folder contents
            curated_str = "❌ No"
            if curated_exists and fs_curated_exists:
                # Retrieve Apple Photos Curated album asset base names from Photos DB
                photos_bases = set()
                if photos_db_attached:
                    try:
                        cursor.execute("""
                            SELECT DISTINCT aaa.ZORIGINALFILENAME
                            FROM photos_db.ZGENERICALBUM ga
                            JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = ga.Z_PK
                            JOIN photos_db.ZASSET a ON aa.Z_3ASSETS = a.Z_PK
                            JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                            LEFT JOIN photos_db.ZGENERICALBUM p ON ga.ZPARENTFOLDER = p.Z_PK
                            WHERE ga.ZTITLE = ? AND ga.ZTRASHEDSTATE = 0 AND ga.ZKIND <> 1507
                              AND p.ZTITLE = 'Curated'
                        """, (name,))
                        photos_bases = set(os.path.splitext(row[0])[0].lower() for row in cursor.fetchall() if row[0])
                    except Exception as e:
                        logger.warning(f"Error querying Photos curated album assets for {name}: {e}")

                if photos_db_attached and photos_bases:
                    if photos_bases == fs_bases:
                        curated_str = "✅ Yes"
                    else:
                        curated_str = "⚠️  Mismatch"
                else:
                    curated_str = "✅ Yes"
            elif curated_exists and not fs_curated_exists:
                curated_str = "📁 Needs Folder"
            elif not curated_exists and fs_curated_exists:
                curated_str = "📁 Local Only"

            ranked_moments.append({
                'name': name,
                'total_qualified': data['total_qualified'],
                'proposed_count': data['proposed_count'],
                'curated_count': data['curated_count'],
                'avg_score': avg_score,
                'min_score': min(data['scores']) if data['scores'] else 0.0,
                'max_score': max(data['scores']) if data['scores'] else 0.0,
                'rank_score': rank_score,
                'stage': stage,
                'to_be_curated_exists': to_be_curated_exists,
                'curated_exists': curated_exists,
                'fs_curated_exists': fs_curated_exists,
                'last_pub_str': last_pub_str,
                'can_publish_str': can_publish_str,
                'assets_display': assets_display,
                'curated_str': curated_str
            })

        # Detach photos_db now that we are done querying it for moments list
        if photos_db_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db;")
            except Exception as e:
                logger.warning(f"Could not detach Photos.sqlite: {e}")

        # Sort by: 1. Needs update (proposed + curated < total_qualified), 2. Stage (M100 first), 3. Rank score descending
        ranked_moments.sort(key=lambda x: (
            (x['proposed_count'] + x['curated_count']) < x['total_qualified'],
            x['stage'] != 'M500',  # Put published at the bottom
            x['rank_score']
        ), reverse=True)

        print(f"{'No.':<4} {'Moment Name':<30} {'Rank Score':<12} {'Avg Score':<10} {'Min Score':<10} {'Max Score':<10} {'Assets':<8} {'ToBeCurated?':<13} {'Curated?':<15} {'Published?':<11} {'Can Publish?':<13} {'Last Published':<15}")
        print("-" * 160)
        divider_printed = False
        for idx, m in enumerate(ranked_moments, 1):
            is_needs_update = (m['proposed_count'] + m['curated_count']) < m['total_qualified']
            if not is_needs_update and not divider_printed:
                if idx > 1:
                    print("-" * 160)
                    print(f"--- Up-To-Date Moments " + "-" * 137)
                    print("-" * 160)
                divider_printed = True

            to_be_curated_str = "✅ Yes" if m['to_be_curated_exists'] else "❌ No"
            if (m['proposed_count'] + m['curated_count']) < m['total_qualified'] and m['to_be_curated_exists']:
                to_be_curated_str = "🔄 Update needed"
            
            curated_str = m['curated_str']
            published_str = "✅ Yes" if m['stage'] == 'M500' else "❌ No"
            print(f"{idx:<4} {m['name']:<30} {m['rank_score']:<12.4f} {m['avg_score']:<10.4f} {m['min_score']:<10.4f} {m['max_score']:<10.4f} {m['assets_display']:<8} {to_be_curated_str:<13} {curated_str:<15} {published_str:<11} {m['can_publish_str']:<13} {m['last_pub_str']:<15}")

        print("\n--- Actions ---")
        print(" [1] Sync proposed assets to ToBeCurated albums in Apple Photos")
        print(" [2] Export Curated album from Apple Photos to LaCie filesystem")
        print(" [3] Record publication in the database (Mark as Published to Shutterfly/YouTube)")
        print(" [R] Restart the planner")
        print(" [E] Exit")
        
        choice = input("\nSelect action: ").strip().lower()
        if choice == 'r':
            logger.info("Restarting planner...")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            os.execv(sys.executable, [sys.executable] + sys.argv)
        elif choice == '1':
            script_dir = os.path.dirname(os.path.abspath(__file__))
            logger.info("Syncing proposed assets to Apple Photos...")
            try:
                subprocess.run(["python3", os.path.join(script_dir, "create_apple_moments_albums.py")], check=True)
                logger.info("Sync complete.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Sync failed: {e}")
        elif choice == '2':
            moment_name = input("Enter Moment Name to export (or index from list): ").strip()
            if moment_name.isdigit():
                idx = int(moment_name) - 1
                if 0 <= idx < len(ranked_moments):
                    moment_name = ranked_moments[idx]['name']
            
            if not moment_name:
                continue
                
            dest_folder = os.path.join(CURATED_LACIE_DIR, moment_name)
            if not os.path.exists(dest_folder):
                create_confirm = input(f"📁 Folder '{dest_folder}' does not exist. Do you want to create it? [y/N]: ").strip().lower()
                if create_confirm == 'y':
                    os.makedirs(dest_folder, exist_ok=True)
                    logger.info(f"Created folder: {dest_folder}")
                else:
                    logger.warning("Aborted export.")
                    continue
                    
            script_dir = os.path.dirname(os.path.abspath(__file__))
            try:
                subprocess.run(["python3", os.path.join(script_dir, "export_curated_album.py"), moment_name], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Export failed: {e}")
        elif choice == '3':
            moment_name = input("Enter Moment Name to publish (or index from list): ").strip()
            if moment_name.isdigit():
                idx = int(moment_name) - 1
                if 0 <= idx < len(ranked_moments):
                    moment_name = ranked_moments[idx]['name']
            
            if not moment_name:
                continue
                
            cursor.execute("""
                SELECT asset_id FROM moment_exports 
                WHERE album_name = ? AND curation_stage = 'curated'
            """, (moment_name,))
            curated_assets = [row[0] for row in cursor.fetchall()]
            
            if not curated_assets:
                print(f"⚠️ No curated assets found in the DB for '{moment_name}'. Please export the Curated album first.")
                continue
                
            confirm = input(f"Confirm publication of {len(curated_assets)} assets of '{moment_name}' to Shutterfly/YouTube? [y/N]: ").strip().lower()
            if confirm == 'y':
                try:
                    pub_data = [(aid, moment_name, 'Shutterfly/YouTube') for aid in curated_assets]
                    cursor.executemany("""
                        INSERT INTO publications (asset_id, moment_name, platform, published_at_utc)
                        VALUES (?, ?, ?, datetime('now'))
                    """, pub_data)
                    
                    cursor.execute("""
                        INSERT INTO curated_moments (moment_name, memory_stage)
                        VALUES (?, 'M500')
                        ON CONFLICT(moment_name) DO UPDATE SET memory_stage = 'M500'
                    """, (moment_name,))
                    
                    conn.commit()
                    print(f"✅ Recorded publication of {len(curated_assets)} assets for '{moment_name}' in database.")
                except Exception as e:
                    logger.warning(f"Failed to record publication: {e}")
                    conn.rollback()
        elif choice == 'e':
            break

def main(auto_apply):
    # Set up logger with line number in format

    # Check for active planned execution first to prevent overlapping plans
    check_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    check_cursor = check_conn.cursor()
    check_cursor.execute("SELECT planned_month FROM planned_execution WHERE active = 1")
    planned_row = check_cursor.fetchone()
    check_conn.close()

    if planned_row:
        active_month = planned_row[0]
        logger.warning(f"⚠️ An active plan for month {active_month} already exists.")
        logger.info(f"Please run 'python3 scripts/pipeline_executor.py' to execute it, or manually reset the planned_execution table.")
        sys.exit(0)

    # Run bootstrap steps before proceeding
    run_bootstrap_steps(auto_apply, logger)

    # Prompt for session mode: Memory Feature & Publishing or Batch Management
    if not auto_apply:
        print("\n--- 🛠️  Session Mode ---")
        mode = input("Select mode: [B] Batch Management (default) | [M] Memory Feature & Publishing: ").strip().lower()
        if mode == 'm':
            conn = get_connection()
            conn.execute("PRAGMA busy_timeout = 30000")
            cursor = get_cursor()
            run_memory_publishing_flow(cursor, conn)
            close_conn()
            sys.exit(0)

    conn = get_connection()
    conn.execute("PRAGMA busy_timeout = 30000")
    cursor = get_cursor()

    # Check for completed batches that have new assets imported since their last update
    cursor.execute("""
        SELECT mb.month, mb.updated_at_utc, MAX(a.imported_date_utc), COUNT(a.asset_id)
        FROM month_batches mb
        JOIN assets a ON a.month = mb.month
        WHERE mb.status_code >= '600'
        GROUP BY mb.month
        HAVING MAX(a.imported_date_utc) > mb.updated_at_utc
    """)
    outdated_batches = cursor.fetchall()
    if outdated_batches:
        print("\n==================================================")
        print("🔄 Detected New Assets in Completed Batches")
        print("==================================================")
        print("The following processed/finalized batches have new imported photos:")
        for month, finalized_at, newest_import, asset_count in outdated_batches:
            print(f" - {month}: Finalized on {finalized_at}, Newest import: {newest_import}")
        
        for month, finalized_at, newest_import, asset_count in outdated_batches:
            if not auto_apply:
                reset_input = input(f"\nDo you want to reset batch {month} to status '000' (added) to re-process new assets? [y/N]: ").strip().lower()
                if reset_input == 'y':
                    cursor.execute("UPDATE month_batches SET status_code = '000', updated_at_utc = CURRENT_TIMESTAMP WHERE month = ?", (month,))
                    conn.commit()
                    logger.info(f"✅ Reset batch {month} to status '000'.")

    # Shared credentials for all Google API calls in this planner session
    creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)

    # Pre-fetch remote favorites to avoid repeated API calls during analysis
    remote_favs_cache = None
    try:
        logger.info("🌐 Fetching remote favorites from Google Photos API to verify curation status...")
        remote_favs_cache = get_all_favorites(creds)
    except Exception as e:
        logger.warning(f"Could not pre-fetch remote favorites: {e}")

    transitions = get_stage_transitions(cursor)
    batches = get_batch_statuses(cursor)

    display_summary(transitions, batches, cursor, remote_favs_cache)

    # Proactive check for new month readiness
    if batches:
        latest_month_str, latest_status = batches[0]  # Ordered DESC
        if str(latest_status) >= '600':
            now = datetime.now()
            current_month_str = now.strftime('%Y-%m')
            if latest_month_str < current_month_str:
                latest_dt = datetime.strptime(latest_month_str, '%Y-%m')
                next_dt = (latest_dt + timedelta(days=32)).replace(day=1)
                next_month_str = next_dt.strftime('%Y-%m')
                
                # Only suggest if the next month hasn't even started (not in batches)
                if next_month_str not in [b[0] for b in batches]:
                    logger.info(f"✨ Current pipeline progress: {latest_month_str} is complete.")
                    logger.info(f"💡 Suggestion: Ready to start {next_month_str}. Ensure all active sources ({', '.join(ACTIVE_CAMERA_MODELS)}) are imported into Apple Photos.")

    logger.info("=== ✅ Suggested Action ===")

    # Fetch all months in descending order, excluding the current calendar month
    # as it is considered incomplete for processing.
    # TODO - month selection also should be done after the transition type is determined 
    current_month_str = datetime.now().strftime('%Y-%m')
    cursor.execute("SELECT DISTINCT month FROM month_batches WHERE month < ? ORDER BY month DESC", (current_month_str,))
    months_descending = [row[0] for row in cursor.fetchall()]

    # Collect candidates for each transition type, across all months
    manual_candidates = []
    retryable_candidates = []
    pipeline_candidates = []

    for month in months_descending:
        month_status = None
        for m, s in batches:
            if m == month:
                month_status = s
                break
        if month_status is None:
            continue

        # Filter the pre-fetched transitions list for this month's status using string comparison
        transitions_for_month = [
            t for t in transitions 
            if str(t[1]) == str(month_status)
        ]

        # If in an error state (e.g., 400E), find the transition that was attempted (code=400)
        # so we can suggest a retry.
        if not transitions_for_month and str(month_status).endswith('E'):
            failed_code = str(month_status)[:-1]
            # Find the transition where the target code is the one that failed
            retry_candidates = [t for t in transitions if str(t[0]) == failed_code]
            for t in retry_candidates:
                logger.info(f"Found error state '{month_status}' for {month}. Suggesting retry of step {failed_code}.")
                # Treat as a retryable candidate to prioritize resolving the failure
                retryable_candidates.append((month, t))

        logger.debug(f"Inspecting transitions for month {month} with status {month_status}")
        for t in transitions_for_month:
            if t[3] == 'manual':
                logger.debug(f"Found manual transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                manual_candidates.append((month, t))
            elif t[3] == 'retryable':
                logger.debug(f"Found retryable transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                retryable_candidates.append((month, t))
            elif t[3] == 'pipeline':
                logger.debug(f"Found pipeline transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                pipeline_candidates.append((month, t))

    # Precedence: manual > retryable > pipeline. Sort by month descending to prioritize newer batches.
    manual_candidates.sort(key=lambda x: x[0], reverse=True)
    retryable_candidates.sort(key=lambda x: x[0], reverse=True)
    pipeline_candidates.sort(key=lambda x: x[0], reverse=True)

    selected_month = None
    selected_transition = None

    logger.info("🔍 Evaluating manual transition candidates...")
    for month, transition in manual_candidates:
        selected_code, selected_prev, selected_desc, selected_type, short_label = transition
        logger.info(f"  Checking {month} ({selected_desc}, status {selected_prev})...")

        cursor.execute("SELECT MAX(updated_at_utc) FROM assets WHERE uploaded_to_google = 1 AND month = ?", (month,))
        result = cursor.fetchone()
        last_completed_at = result[0] if result else None
        elapsed_days = None
        if last_completed_at:
            try:
                last_dt = datetime.strptime(last_completed_at, "%Y-%m-%d %H:%M:%S")
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                elapsed_days = (now_utc - last_dt).total_seconds() / 86400
            except: pass

        if elapsed_days is not None and elapsed_days < 3:
            logger.info(f"    ⏸️ Too soon: Only {elapsed_days:.1f} days since upload. Need 3 days for Google AI curation.")
            continue

        fav_count, source, _ = check_favorites_count(cursor, month, check_remote=True, all_favs=remote_favs_cache, creds=creds)
        if fav_count == 0:
            if selected_prev == '500':
                logger.info(f"    ⏸️ Manual transition blocked: No favorites in Google Photos.")
                continue
            else:
                logger.warning(f"    ⚠️ No favorites found for {month}. Starring may not be complete.")
        else:
            logger.info(f"    ✨ Detected {fav_count} favorites ({source}).")

        if not auto_apply:
            proceed_input = input(f"\nPlease confirm: has '{short_label}' task been completed for {month}? [y/N]: ")
            if proceed_input.strip().lower() == 'y':
                cursor.execute("UPDATE month_batches SET status_code = ? WHERE month = ?", (selected_code, month))
                conn.commit()
                logger.info(f"✅ Month {month} status updated to {selected_code}.")
                close_conn(); sys.exit(0)
            else:
                logger.info(f"  Skipped manual transition for {month}. Checking next candidate...")
                continue
        else:
            logger.info(f"  Auto-apply enabled; skipping manual task {month} for safety. Checking next candidate...")
            continue

    logger.info("🔍 Evaluating retryable transition candidates...")
    for month, transition in retryable_candidates:
        selected_code, selected_prev, selected_desc, selected_type, short_label = transition

        # Only perform space-based analysis and branching for batches that haven't finished 
        # their primary upload (stage < 400) and are targeting an upload operation.
        is_upload_retry = selected_code in ['399', '400'] and int(selected_prev) < 400
        if is_upload_retry:
            free_space = check_google_quota(creds=creds)
            if free_space is None:
                logger.error("❌ Error: Could not retrieve Google Drive quota."); close_conn(); sys.exit(1)

            import glob
            matched_folders = glob.glob(os.path.join(STAGING_ROOT, f"*{month}*"))
            if matched_folders:
                staging_folder = matched_folders[0]
                staging_size = 0
                for root, dirs, files in os.walk(staging_folder):
                    for f in files:
                        ext = os.path.splitext(f)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            fp = os.path.join(root, f)
                            staging_size += os.path.getsize(fp)
                logger.info(f"Staging folder content for {month}: {human_readable_size(staging_size)} total files.")
            else:
                staging_folder = None; staging_size = 0; logger.warning(f"No staging folder found for {month}")

            cursor.execute("SELECT original_filename FROM assets WHERE month = ? AND uploaded_to_google = 1", (month,))
            uploaded_assets = cursor.fetchall()
            latest_upload_size = 0
            if uploaded_assets and staging_folder:
                for filename_tuple in uploaded_assets:
                    file_path = os.path.join(staging_folder, filename_tuple[0])
                    if os.path.exists(file_path):
                        latest_upload_size += os.path.getsize(file_path)
            logger.info(f"Upload progress: {human_readable_size(latest_upload_size)} of {month} already in Google Photos.")

            remaining_to_upload = max(0, staging_size - latest_upload_size)
            if remaining_to_upload == 0:
                logger.info(f"✅ All assets for {month} appear to be uploaded already.")
                if auto_apply: proceed_transition = True
                else:
                    ans = input(f"All assets uploaded - transition {month} to 400 status? [y/N]: ").strip().lower()
                    proceed_transition = ans == 'y'
                if proceed_transition:
                    cursor.execute("UPDATE month_batches SET status_code = '400' WHERE month = ?", (month,))
                    conn.commit(); logger.info(f"Month {month} updated to 400."); close_conn(); sys.exit(0)
            elif free_space >= remaining_to_upload:
                logger.info(f"🚀 Found {human_readable_size(remaining_to_upload)} left to upload for {month}. "
                            f"Available space: {human_readable_size(free_space)}. Priority given to finishing this batch.")
                selected_month = month
                selected_transition = transition
                break
            else:
                logger.warning(f"⚠️ Insufficient space for {month}. Free: {human_readable_size(free_space)}, Need: {human_readable_size(remaining_to_upload)}.")
                
                # Branch and suggest cleanup for months at stage 600
                cleanup_candidates = [m for m, s in batches if str(s) == '600']
                if cleanup_candidates:
                    logger.info(f"💡 Suggestion: Drive cleanup available for processed months: {', '.join(cleanup_candidates)}")
                    for m_c, t_c in pipeline_candidates:
                        if m_c in cleanup_candidates and str(t_c[1]) == '600':
                            selected_month = m_c
                            selected_transition = t_c
                            logger.info(f"🔄 Branching to cleanup transition (600->650) for {selected_month} to free up space.")
                            break
                    if selected_month:
                        break
                continue

    if not selected_month:
        logger.info("🔍 Evaluating pipeline transition candidates...")
        if not pipeline_candidates:
            logger.info("No pipeline transitions available. Exiting."); close_conn(); sys.exit(0)
        selected_month, selected_transition = pipeline_candidates[0]

    latest_month = selected_month
    selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
    current_status = selected_prev

    if True:
        # Build the full transition path from current status, only including pipeline transitions
        full_transition_list = get_full_transition_path(
            [t for t in transitions if t[3] in ['pipeline', 'retryable']],
            str(current_status)
        )

        # Only perform import continuity and sequencing checks for batches that haven't reached the upload stage (400)
        # This prevents redundant prompts for batches that are already processed or being curated.
        if current_status and str(current_status) < '400':
            # Check if Apple Photos Smart Album exists before proposing migration off of 000 / 100E
            if str(current_status) in ('000', '100E'):
                cursor.execute("SELECT COUNT(*) FROM smart_albums WHERE LOWER(album_name) = ?", (latest_month.lower(),))
                if cursor.fetchone()[0] == 0:
                    logger.error(f"❌ Smart Album '{latest_month}' does not exist in Apple Photos.")
                    logger.info(f"👉 Please create the Smart Album '{latest_month}' inside 'MonthlyExports' in Apple Photos first.")
                    logger.info("Then, re-run the pipeline planner to sync the changes and proceed.")
                    close_conn()
                    sys.exit(0)

            check_active_sources_import_status(cursor, conn, latest_month, auto_apply)

            # --- Check sequencing before recording the plan ---
            if not verify_sequencing_for_planned_month(cursor, conn, latest_month, auto_apply):
                logger.warning(f"Sequencing not confirmed for {latest_month}. Aborting plan recording.")
                close_conn()
                sys.exit(0)

        logger.info(f"Run pipeline for: Month={latest_month}, Transitions={full_transition_list}")

        # --- Check for favorites readiness if transition involves pulling or ranking ---
        is_favorites_pull = any('550' in str(t) or 'Pull Google' in str(t) for t in full_transition_list)
        is_after_pull = any('Rank Assets' in str(t) or 'Ranking' in str(t) for t in full_transition_list)
        
        if is_favorites_pull:
            fav_count, source, fav_names = check_favorites_count(
                cursor, latest_month, check_remote=True, 
                all_favs=remote_favs_cache, creds=creds
            )
            if fav_count == 0:
                logger.warning(f"⚠️ Suggested batch {latest_month} has no favorites in Google Photos yet.")
            else:
                logger.info(f"✨ Batch {latest_month} is ready with {fav_count} favorites in Google Photos (Source: {source}).")
        elif is_after_pull:
            fav_count, source, fav_names = check_favorites_count(
                cursor, latest_month, check_remote=False, 
                all_favs=remote_favs_cache, creds=creds
            )
            if fav_count == 0:
                logger.warning(f"⚠️ Suggested batch {latest_month} has 0 favorites in local DB (Source: {source}). Ranking steps may be skipped.")

        # --- Begin Google quota check for upload transitions ---
        # Determine if any transition in the pipeline represents an upload to Google (e.g., '210->399')
        quota_check_needed = any(
            isinstance(transition, str) and '210->399' in transition
            or (isinstance(transition, (list, tuple)) and len(transition) >= 1 and '210->399' in str(transition))
            for transition in full_transition_list
        )
        # --- End Google quota check (defer actual check to after user confirmation) ---

        if not auto_apply:
            proceed = input("Proceed with this plan? [y/N]: ")
            if proceed.strip().lower() != 'y':
                logger.info("Aborted by user.")
                close_conn()
                sys.exit(0)

        # Now, if quota check is needed, perform the actual quota check before executing
        if quota_check_needed:
            import glob
            matched_folders = glob.glob(os.path.join(STAGING_ROOT, f"*{latest_month}*"))
            if matched_folders:
                staging_folder = matched_folders[0]
                staging_size = 0
                for root, dirs, files in os.walk(staging_folder):
                    for f in files:
                        fp = os.path.join(root, f)
                        staging_size += os.path.getsize(fp)
                logger.info(f"Detected staging folder for month {latest_month}: {staging_folder}, size: {human_readable_size(staging_size)}")
            else:
                staging_folder = None
                staging_size = 0
                logger.warning(f"No staging folder found for month {latest_month}")
            free_space = check_google_quota(creds=creds)
            if free_space is None:
                logger.error("❌ Aborting: Could not retrieve Google Drive quota before upload.")
                close_conn()
                sys.exit(1)
            if free_space < staging_size:
                logger.warning(f"⚠️ Insufficient space: {human_readable_size(free_space)} available vs {human_readable_size(staging_size)} required.")
                
                # Perform estimation of how many assets will fit based on aesthetic score
                cursor.execute("SELECT original_filename, aesthetic_score FROM assets WHERE month = ?", (latest_month,))
                db_scores = {row[0].lower(): (row[1] or -1) for row in cursor.fetchall()}
                
                staging_files = []
                for root, _, fnames in os.walk(staging_folder):
                    for f in fnames:
                        fp = os.path.join(root, f)
                        staging_files.append((f, os.path.getsize(fp), db_scores.get(f.lower(), -1)))
                
                # Sort by score descending (highest ranked first)
                staging_files.sort(key=lambda x: x[2], reverse=True)
                
                can_upload_count = 0
                simulated_sum = 0
                for _, size, _ in staging_files:
                    if simulated_sum + size <= free_space:
                        simulated_sum += size
                        can_upload_count += 1
                    else:
                        break
                
                logger.warning(f"📊 Estimate: Only {can_upload_count} out of {len(staging_files)} assets will fit.")
                
                if not auto_apply:
                    partial_confirm = input(f"Proceed with a partial upload of the highest-ranked assets for {latest_month}? [y/N]: ")
                    if partial_confirm.strip().lower() != 'y':
                        logger.info("Pipeline transition aborted by user.")
                        close_conn()
                        sys.exit(0)
                    logger.info("User confirmed partial upload. Proceeding with plan...")
                else:
                    logger.error("❌ Auto-apply aborted: Insufficient space for full upload. Manual confirmation required for partial sync.")
                    close_conn()
                    sys.exit(1)
            else:
                logger.info(f"Enough Google Drive space available for upload. Free space: {human_readable_size(free_space)}, Staging size: {human_readable_size(staging_size)}")

        logger.info("🚀 Executing planned steps...")
        set_planned_month(cursor, latest_month)
        conn.commit()
        logger.info(f"📌 Month {latest_month} recorded in planned_execution for next pipeline run.")

        if selected_type not in ['manual', 'retryable', 'pipeline']:
            logger.warning(f"Unknown transition type '{selected_type}' for current status {current_status}.")

    # TODO: trigger executor or store plan
    # TODO: Decide whether to implement quota filler strategy (partial month uploads).
    # Current pipeline assumes full-month atomicity (399 -> 400).

    close_conn()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-apply", action="store_true", help="Skip confirmation and apply plan immediately")
    args = parser.parse_args()
    main(auto_apply=args.auto_apply)
