import sys
import os
import subprocess
import re

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
import requests
from datetime import timezone, datetime, timedelta
 

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

    db_mod_time = os.path.getmtime(APPLE_PHOTOS_DB_COPY_PATH)

    if last_sync_time is None:
        return True

    last_sync_timestamp = datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").timestamp()

    return db_mod_time > last_sync_timestamp

# Helper to run bootstrap steps
def run_bootstrap_steps(cursor, auto_apply, logger):
    """
    Run the bootstrap steps: copy_all_media_db.py, storage_status.py, sync_photos_metadata.py.
    """
    steps = [
        ("0.0 Copy all media DB", "copy_all_media_photos_db.py", []),
        ("0.1 Run storage manager", "storage_manager_main.py", ["--migrate"]),
        ("0.2 Sync assets", "sync_photos_raw.py", []),
        ("0.3 Sync metadata", "sync_photos_derived.py", []),
        ("1.0 Generate Batches", "generate_month_batches.py", [])
    ]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for step_name, script_file, step_args in steps:
        script_path = os.path.join(script_dir, script_file)
        logger.info(f"🔧 Running bootstrap step: {step_name} ({script_file})")
        try:
            if script_file in ["sync_photos_raw.py", "sync_photos_derived.py", "sync_photos_metadata.py"]:
                if not should_run_sync_metadata(cursor):
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

def check_active_sources_import_status(cursor, conn, auto_apply):
    """
    Checks if all active camera models have imported assets for the latest complete month.
    Prompts user if any active source is missing.
    """
    if not ACTIVE_CAMERA_MODELS:
        logger.info("No active camera models configured. Skipping active source check.")
        return True

    # Determine the latest complete month (e.g., April if current month is May)
    latest_complete_month = datetime.now().replace(day=1) - timedelta(days=1)
    latest_complete_month_str = latest_complete_month.strftime('%Y-%m')

    logger.info(f"🔍 Checking active source imports for latest complete month: {latest_complete_month_str}")

    # Attach the Apple Photos database
    try:
        cursor.execute(f"ATTACH DATABASE '{APPLE_PHOTOS_DB_COPY_PATH}' AS photos_db;")
        logger.debug("Attached Photos.sqlite database for active source check.")

        # We use conditional aggregation (CASE WHEN) to get the range for the target month 
        # while still being able to group by camera model.
        query = """
            SELECT 
                xa.ZCAMERAMODEL,
                COUNT(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN 1 END) AS assets_in_month,
                MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN aaa.ZORIGINALFILENAME END) AS min_filename,
                MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN aaa.ZORIGINALFILENAME END) AS max_filename,
                MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch') END) AS min_date,
                MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch') END) AS max_date,
                GROUP_CONCAT(DISTINCT CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ? THEN a.ZIMPORTSESSION END) AS involved_import_ids
            FROM photos_db.ZASSET a
            JOIN photos_db.ZEXTENDEDATTRIBUTES xa ON xa.ZASSET = a.Z_PK
            JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
            WHERE a.ZTRASHEDSTATE = 0
              AND xa.ZCAMERAMODEL IN ({})
              AND a.ZDATECREATED >= (strftime('%s', date('now', 'start of month', '-24 month')) - 978307200)
              AND a.ZDATECREATED < (strftime('%s', date('now', 'start of month')) - 978307200)
            GROUP BY xa.ZCAMERAMODEL
        """.format(','.join(['?' for _ in ACTIVE_CAMERA_MODELS]))

        # Passing the month string 6 times for the aggregate filters and GROUP_CONCAT
        cursor.execute(query, [latest_complete_month_str] * 6 + ACTIVE_CAMERA_MODELS)
        results = cursor.fetchall()
        found_models = set()

        for row in results:
            model, count, f_min, f_max, d_min, d_max, involved_import_ids = row
            if count > 0:
                found_models.add(model)

                # Reasonability check: parse numeric part from filenames (ignoring extensions)
                num_min = None
                num_max = None
                if f_min:
                    nums = re.findall(r'(\d+)', os.path.splitext(f_min)[0])
                    if nums: num_min = int(nums[-1])
                if f_max:
                    nums = re.findall(r'(\d+)', os.path.splitext(f_max)[0])
                    if nums: num_max = int(nums[-1])

                gap_info = ""
                if num_min is not None and num_max is not None:
                    # We use abs because string MIN/MAX might flip if sequence is not zero-padded
                    expected_range = abs(num_max - num_min) + 1
                    if expected_range > count:
                        gap_info = f" | ⚠️ Reasonability: {expected_range} expected vs {count} found (gap of {expected_range - count})"

                # Continuity check with previous month's confirmed imports
                continuity_info = ""
                previous_month = (latest_complete_month.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
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

                logger.info(f"📸 Source: {model:20} | Count: {count:4} | Files: {f_min} -> {f_max} | Dates: {d_min} to {d_max}{gap_info}{continuity_info}")

        missing_models = set(ACTIVE_CAMERA_MODELS) - found_models

        if missing_models:
            logger.warning(f"⚠️ Missing imports for active camera models in {latest_complete_month_str}: {', '.join(missing_models)}")
            if not auto_apply:
                proceed_input = input("Some active sources are missing imports for the latest complete month. Do you want to proceed? [y/N]: ")
                if proceed_input.strip().lower() != 'y':
                    logger.info("Operation aborted by user due to missing active source imports.")
                    sys.exit(0)
            else:
                logger.error("❌ Auto-apply aborted: Missing active source imports for the latest complete month. Manual intervention required.")
                sys.exit(1)
        else:
            logger.info(f"✅ All active camera models have imports for {latest_complete_month_str}.")

        # Per-source sequencing confirmation
        if not auto_apply:
            for row in results:
                model, count, f_min, f_max, d_min, d_max, involved_import_ids = row
                if count == 0 or not involved_import_ids:
                    continue
                
                # Extract individual import IDs from the concatenated string
                import_id_list = involved_import_ids.split(',')
                placeholders = ','.join(['?' for _ in import_id_list])
                
                # Check which of these involved imports are still unconfirmed in our local table
                cursor.execute("""
                    SELECT COUNT(*) FROM imports
                    WHERE import_uuid IN ({})
                      AND (sequencing_confirmed = 0 OR sequencing_confirmed IS NULL)
                """.format(placeholders), import_id_list)
                unconfirmed_count = cursor.fetchone()[0]

                if unconfirmed_count > 0:
                    choice = input(f"Verifying that {model} filenames for {latest_complete_month_str} are reasonable. Select I to mark the import as reasonable. [I/n]: ").strip().upper()
                    if choice == 'I':
                        cursor.execute("""
                            UPDATE imports 
                            SET sequencing_confirmed = 1 
                            WHERE import_uuid IN ({})
                        """.format(placeholders), import_id_list)
                        conn.commit()
                        logger.info(f"✅ Marked involved imports for {model} in {latest_complete_month_str} as reasonable.")
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
        SELECT DISTINCT i.import_uuid, i.camera_model, i.min_filename, i.max_filename, i.assets_count
        FROM imports i
        JOIN assets a ON a.import_id = i.import_uuid
        WHERE a.month = ? AND (i.sequencing_confirmed = 0 OR i.sequencing_confirmed IS NULL)
    """, (month,))
    unconfirmed = cursor.fetchall()

    if not unconfirmed:
        return True

    logger.info(f"🧐 Found {len(unconfirmed)} import sessions for {month} requiring sequencing confirmation.")
    for uuid, model, f_min, f_max, count in unconfirmed:
        num_min_matches = re.findall(r'(\d+)', os.path.splitext(f_min)[0]) if f_min else []
        num_min = int(num_min_matches[-1]) if num_min_matches else 0
        num_max_matches = re.findall(r'(\d+)', os.path.splitext(f_max)[0]) if f_max else []
        num_max = int(num_max_matches[-1]) if num_max_matches else 0
        
        expected = abs(num_max - num_min) + 1
        gap = expected - count if expected > count else 0
        gap_str = f" | ⚠️ Gap detected: {gap} items" if gap > 0 else ""
        logger.info(f"   - Session {uuid} ({model}): {f_min} -> {f_max} ({count} files){gap_str}")

    if auto_apply:
        return True

    choice = input("Verifying that the months filenames are reasonable. Select I to mark the import as reasonable. [I/n]: ").strip().upper()
    if choice == 'I':
        placeholders = ','.join(['?' for _ in unconfirmed])
        cursor.execute(f"UPDATE imports SET sequencing_confirmed = 1 WHERE import_uuid IN ({placeholders})", [row[0] for row in unconfirmed])
        conn.commit()
        return True
    return False

def get_stage_transitions(cursor):
    cursor.execute("""
        SELECT code, preceding_code, full_description, transition_type, short_label
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND code NOT LIKE '%E'
    """)
    return cursor.fetchall()

def get_batch_statuses(cursor):
    cursor.execute("""
        SELECT month, status_code
        FROM month_batches
        ORDER BY month DESC
    """)
    return cursor.fetchall()

def get_latest_import_and_month(cursor, transition_type="pipeline"):
    """
    Fetch the latest import and complete month for a given transition type.
    Default is 'pipeline'.
    """
    cursor.execute(f"""
        SELECT (
            SELECT i.import_uuid
            FROM assets a
            JOIN imports i ON a.import_id = i.import_uuid
            WHERE a.month = mb2.month
            ORDER BY i.import_uuid DESC
            LIMIT 1
        ) AS latest_import,
        mb2.month
        FROM month_batches mb2
        WHERE mb2.month < strftime('%Y-%m', 'now')
          AND mb2.status_code IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM batch_status bs
            WHERE bs.preceding_code = mb2.status_code
              AND bs.transition_type = ?
              AND bs.code NOT LIKE '%E'
          )
        ORDER BY mb2.month DESC
        LIMIT 1;
     """, (transition_type,))
    return cursor.fetchone()


def display_summary(transitions, batches):
    print("\n=== 📊 Stage Transitions ===")
    for code, prev, desc, ttype, label in transitions:
        print(f"{prev} ➜ {code}: {desc} (Type: {ttype})")

    print("\n=== 📦 Batch Statuses ===")
    for month, status in batches:
        print(f"Month: {month}, Status: {status}")

    # TODO - Cannot determine below without knowing the transition type
    # print("\n=== 🗓️ Latest Info ===")
    # print(f"Latest Import Month: {latest_import}")
    # print(f"Latest Complete Month: {latest_month}")

def main(auto_apply):
    # Set up logger with line number in format

    conn = get_connection()
    cursor = get_cursor()

    # Run bootstrap steps before proceeding
    run_bootstrap_steps(cursor, auto_apply, logger)

    # Check active sources import status
    check_active_sources_import_status(cursor, conn, auto_apply)

    # Shared credentials for all Google API calls in this planner session
    creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)

    transitions = get_stage_transitions(cursor)
    batches = get_batch_statuses(cursor)
    # TODO - need to fix this - those latest values make sense only when transition type is known
    # TODO - unless we filtered right here by pipeline type
    # latest_import, latest_month = get_latest_import_and_month(cursor)

    display_summary(transitions, batches)

    logger.info("=== ✅ Suggested Action ===")

    # Fetch all months in descending order
    # TODO - month selection also should be done after the transition type is determined 
    cursor.execute("SELECT DISTINCT month FROM month_batches ORDER BY month DESC")
    months_descending = [row[0] for row in cursor.fetchall()]

    # Collect candidates for each transition type, across all months
    manual_candidates = []
    retryable_candidates = []
    pipeline_candidates = []
    remote_favs_cache = None

    for month in months_descending:
        month_status = None
        for m, s in batches:
            if m == month:
                month_status = s
                break
        if month_status is None:
            continue

        # --- NEW: Manual Upload Detection / Fast-Forward Logic ---
        # If the month is in an early stage but we suspect it might have been manually uploaded,
        # check for remote favorites.
        if month_status < '550':
            # Only fetch remote favorites once per planner run
            if remote_favs_cache is None:
                try:
                    logger.info("🌐 Fetching remote favorites from Google Photos API to detect potential manual uploads...")
                    remote_favs_cache = get_all_favorites(creds)
                except Exception as e:
                    logger.warning(f"Failed to pre-fetch remote favorites: {e}")
                    remote_favs_cache = []

            fav_count, source, fav_names = check_favorites_count(cursor, month, check_remote=True, all_favs=remote_favs_cache, creds=creds, verbose=False)
            if source == 'remote' and fav_count > 0:
                logger.info(f"✨ [Manual Upload Detected] Month {month} has {fav_count} favorites on Google Photos: {fav_names}")
                logger.info(f"Current local status is '{month_status}' (Export/Upload stage).")
                
                if not auto_apply:
                    prompt = f"Would you like to skip Export/Upload and fast-forward {month} to 'Pull Favorites' stage? [y/N]: "
                    jump_input = input(prompt)
                    if jump_input.strip().lower() == 'y':
                        # Identify the code that enables Pull Favorites (550)
                        cursor.execute("""
                            SELECT preceding_code FROM batch_status 
                            WHERE code = '550' AND transition_type = 'pipeline'
                        """)
                        row = cursor.fetchone()
                        if row:
                            new_status = row[0]
                            cursor.execute("UPDATE month_batches SET status_code = ? WHERE month = ?", (new_status, month))
                            commit()
                            logger.info(f"✅ Month {month} status fast-forwarded to {new_status}. Re-run planner to see the new execution plan.")
                            close_conn()
                            sys.exit(0)
                        else:
                            logger.warning("Could not find pipeline transition for stage 550 to perform fast-forward.")

        cursor.execute("""
            SELECT code, preceding_code, full_description, transition_type, short_label
            FROM batch_status
            WHERE preceding_code = ?
        """, (month_status,))
        transitions_for_month = cursor.fetchall()

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

    def pick_latest(candidates):
        # Ensure months are compared as YYYY-MM strings correctly
        return max(candidates, key=lambda x: x[0])

    selected_month = None
    selected_transition = None
    selected_type = None
    current_status = None
    latest_month = None

    # Precedence: manual > retryable > pipeline
    if manual_candidates:
        selected_month, selected_transition = pick_latest(manual_candidates)
    elif retryable_candidates:
        selected_month, selected_transition = pick_latest(retryable_candidates)
    elif pipeline_candidates:
        selected_month, selected_transition = pick_latest(pipeline_candidates)
    else:
        logger.info("No eligible month found with available transitions. Exiting.")
        close_conn()
        sys.exit(0)

    if selected_month and selected_transition:
        selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
        current_status = selected_prev
        latest_month = selected_month
    else:
        logger.info("No eligible month found with available transitions. Exiting.")
        close_conn()
        sys.exit(0)

    # Handle manual transition logic
    if selected_type == 'manual':
        logger.info("🔍 Checking for manual transition candidates...")
        logger.info(f"Month {latest_month} is waiting for manual transition ({selected_desc}, status {current_status}).")
        # --- Check how long ago the upload for this month was done ---
        # Use the latest updated_at_utc from assets where uploaded_to_google = 1 for this month
        cursor.execute("""
            SELECT MAX(a.updated_at_utc)
            FROM assets a
            WHERE a.uploaded_to_google = 1
              AND a.month = ?
        """, (latest_month,))
        result = cursor.fetchone()
        last_completed_at = result[0] if result else None
        elapsed_days = None
        if last_completed_at:
            try:
                # Assume updated_at_utc is in format 'YYYY-MM-DD HH:MM:SS'
                last_completed_dt = datetime.strptime(last_completed_at, "%Y-%m-%d %H:%M:%S")
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                elapsed = now_utc - last_completed_dt
                elapsed_days = elapsed.total_seconds() / (60 * 60 * 24)
            except Exception as e:
                logger.warning(f"Could not parse updated_at_utc ({last_completed_at}): {e}")
        if elapsed_days is not None and elapsed_days < 3:
            logger.info(f"Too soon for manual transition: Only {elapsed_days:.2f} days since upload for month {latest_month}. Minimum is 3 days. Skipping manual transition prompt.")
            # Skip suggesting manual transition, fall back to retryable or pipeline
            selected_type = None
        else:
            # Check for favorites before prompting for manual completion
            fav_count, source, fav_names = check_favorites_count(cursor, latest_month, check_remote=True, creds=creds)
            if fav_count == 0:
                logger.warning(f"⚠️ No favorites found in Google Photos for {latest_month}. Starring may not be complete.")
            else:
                logger.info(f"✨ Detected {fav_count} favorites for month {latest_month} in Google Photos (Source: {source}).")

            if not auto_apply:
                proceed_input = input(f"Please confirm that the '{short_label}' task has been completed so I can move month {latest_month} to the next phase? [y/N]: ")
                if proceed_input.strip().lower() == 'y':
                    # Find next status code for this manual transition
                    next_status = None
                    for code, prev, desc, ttype, label in transitions:
                        if prev == current_status and ttype == 'manual':
                            next_status = code
                            break
                    if next_status is not None:
                        cursor.execute("UPDATE month_batches SET status_code = ? WHERE month = ?", (next_status, latest_month))
                        conn.commit()
                        logger.info(f"Month {latest_month} status forcibly updated to {next_status}.")
                    else:
                        logger.warning("No manual transition found from current status.")
                    close_conn()
                    sys.exit(0)
                else:
                    logger.info("Manual transition aborted by user. Falling back to retryable or pipeline transitions.")
                    selected_type = None
            else:
                logger.info("Auto-apply enabled, skipping manual transition prompt. Falling back to retryable or pipeline transitions.")
                selected_type = None

    # If manual transition was not performed, or if we are now at retryable
    if selected_type is None:
        # Try retryable candidates
        if retryable_candidates:
            selected_month, selected_transition = pick_latest(retryable_candidates)
            selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
            current_status = selected_prev
            latest_month = selected_month
        elif pipeline_candidates:
            selected_month, selected_transition = pick_latest(pipeline_candidates)
            selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
            current_status = selected_prev
            latest_month = selected_month
        else:
            logger.info("No eligible month found with available transitions after manual transition fallback. Exiting.")
            close_conn()
            sys.exit(0)

    if selected_type == 'retryable':
        logger.info("🔍 Checking for retryable transition candidates...")
        logger.info(f"Handling retryable transition for month with current status {current_status}.")
        # Existing quota logic for 399->400 transition
        cursor.execute("SELECT month FROM month_batches WHERE status_code = 399 ORDER BY month DESC")
        months_399 = cursor.fetchall()
        if months_399:
            latest_399_month = months_399[0][0]
            free_space = check_google_quota(creds=creds)
            if free_space is None:
                logger.error("❌ Aborting: Could not retrieve Google Drive quota. Please check your connection and credentials.")
                close_conn()
                sys.exit(1)
            import glob

            matched_folders = glob.glob(os.path.join(STAGING_ROOT, f"*{latest_399_month}*"))
            if matched_folders:
                staging_folder = matched_folders[0]
                staging_size = 0
                for root, dirs, files in os.walk(staging_folder):
                    for f in files:
                        fp = os.path.join(root, f)
                        staging_size += os.path.getsize(fp)
                logger.info(f"Detected staging folder for month {latest_399_month}: {staging_folder}, size: {human_readable_size(staging_size)}")
            else:
                staging_folder = None
                staging_size = 0
                logger.warning(f"No staging folder found for month {latest_399_month}")

            # Fetch list of uploaded assets for the month from assets table
            cursor.execute("""
                SELECT original_filename
                FROM assets
                WHERE month = ?
                  AND uploaded_to_google = 1
                ORDER BY updated_at_utc
            """, (latest_399_month,))
            uploaded_assets = cursor.fetchall()

            latest_upload_size = 0
            if uploaded_assets and staging_folder:
                for filename_tuple in uploaded_assets:
                    filename = filename_tuple[0]
                    file_path = os.path.join(staging_folder, filename)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        latest_upload_size += file_size
                        logger.debug(f"Found uploaded asset: {file_path}, size: {human_readable_size(file_size)}")
            logger.info(f"Total latest upload size for month {latest_399_month}: {human_readable_size(latest_upload_size)}")

            if free_space >= staging_size:
                logger.info(f"Enough Google Drive space available to transition month {latest_399_month} from 399 to 400. Free space: {human_readable_size(free_space)}, Staging size: {human_readable_size(staging_size)}, Latest upload size: {human_readable_size(latest_upload_size)}")
                if auto_apply:
                    proceed_transition = True
                else:
                    proceed_input = input(f"Transition month {latest_399_month} from status 399 to 400? [y/N]: ")
                    proceed_transition = proceed_input.strip().lower() == 'y'
                if proceed_transition:
                    cursor.execute("UPDATE month_batches SET status_code = 400 WHERE month = ?", (latest_399_month,))
                    conn.commit()
                    logger.info(f"Month {latest_399_month} status updated to 400.")
            else:
                logger.warning(f"Not enough Google Drive space to transition month {latest_399_month} from 399 to 400. Free space: {human_readable_size(free_space)}, Staging size: {human_readable_size(staging_size)}, Latest upload size: {human_readable_size(latest_upload_size)}")
                logger.info("Retryable transition not possible. Falling back to pipeline transitions.")
                selected_type = None
        else:
            logger.info("No months with status 399 found to retry transition.")

    # After retryable block, if selected_type is None and there are pipeline candidates, pick latest pipeline candidate
    if selected_type is None and pipeline_candidates:
        selected_month, selected_transition = pick_latest(pipeline_candidates)
        selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
        current_status = selected_prev
        latest_month = selected_month

    if selected_type == 'pipeline' or selected_type is None:
        logger.info("🔍 Checking for pipeline transition candidates across all months...")
        latest_pipeline_candidate = None
        latest_pipeline_transitions = []

        for month in months_descending:
            status = next((s for m, s in batches if m == month), None)
            if status is None:
                continue
            pt = [t for t in transitions if t[3] == 'pipeline' and t[1] == status]
            if pt:
                latest_pipeline_candidate = (month, status)
                latest_pipeline_transitions = pt
                break  # first in descending order is newest

        if not latest_pipeline_candidate:
            logger.info("No pipeline transitions available for any month. Exiting.")
            close_conn()
            sys.exit(0)

        latest_month, current_status = latest_pipeline_candidate
        # Build the full transition path from current status, only including pipeline transitions
        full_transition_list = get_full_transition_path(
            [t for t in transitions if t[3] == 'pipeline'],
            current_status
        )
        logger.info(f"Run pipeline for: Month={latest_month}, Transitions={full_transition_list}")

        # --- Check for favorites readiness if transition involves pulling or ranking ---
        is_favorites_pull = any('550' in str(t) or 'Pull Google' in str(t) for t in full_transition_list)
        is_after_pull = any('Rank Assets' in str(t) or 'Ranking' in str(t) for t in full_transition_list)
        
        if is_favorites_pull:
            fav_count, source, fav_names = check_favorites_count(cursor, latest_month, check_remote=True, creds=creds)
            if fav_count == 0:
                logger.warning(f"⚠️ Suggested batch {latest_month} has no favorites in Google Photos yet.")
            else:
                logger.info(f"✨ Batch {latest_month} is ready with {fav_count} favorites in Google Photos (Source: {source}).")
        elif is_after_pull:
            fav_count, source, fav_names = check_favorites_count(cursor, latest_month, check_remote=False, creds=creds)
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

        # --- Check sequencing before recording the plan ---
        if not verify_sequencing_for_planned_month(cursor, conn, latest_month, auto_apply):
            logger.warning(f"Sequencing not confirmed for {latest_month}. Aborting plan recording.")
            close_conn()
            sys.exit(0)

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
