import sys
import os
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from utils.logger import setup_logger
from constants import LOG_PATH, STAGING_ROOT
from utils.utils import get_full_transition_path
from google_photos import check_google_quota
import argparse
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_COPY_PATH, LOG_PATH
from db.connections import get_connection, get_cursor, commit, close as close_conn
import logging
import requests


SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary",
    'https://www.googleapis.com/auth/drive.readonly'
]

#     "https://www.googleapis.com/auth/photoslibrary.readonly"
#    'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',



logger = setup_logger(LOG_PATH, "pipeline_planner")
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))



def human_readable_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    p = 1024
    while size_bytes >= p and i < len(size_name)-1:
        size_bytes /= p
        i += 1
    return f"{size_bytes:.2f}{size_name[i]}"

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
        WHERE label = '0.3 Sync Metadata' AND status = 'success'
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

    import datetime
    last_sync_timestamp = datetime.datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").timestamp()

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
        ("0.3 Sync metadata", "sync_photos_derived.py", []),
        ("1.0 Generate Batches", "generate_month_batches.py", [])
    ]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for step_name, script_file, step_args in steps:
        script_path = os.path.join(script_dir, script_file)
        logger.info(f"🔧 Running bootstrap step: {step_name} ({script_file})")
        try:
            if script_file == "sync_photos_metadata.py":
                conn = None
                try:
                    import sqlite3
                    from constants import MEDIA_ORGANIZER_DB_PATH
                    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
                    cursor = conn.cursor()
                    if not should_run_sync_metadata(cursor):
                        logger.info(f"Skipping {script_file} as sync is up to date.")
                        continue
                finally:
                    if conn:
                        close_conn()
            subprocess.run(["python3", script_path] + step_args, check=True)
            logger.info(f"✅ Completed: {step_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error in bootstrap step {step_name}: {e}")
            if script_file == "storage_status.py":
                logger.error("Storage status check failed. Exiting planner due to migration failure.")
                sys.exit(1)
            else:
                sys.exit(1)

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
    run_bootstrap_steps(auto_apply, logger)

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

    for month in months_descending:
        month_status = None
        for m, s in batches:
            if m == month:
                month_status = s
                break
        if month_status is None:
            continue

        cursor.execute("""
            SELECT code, preceding_code, full_description, transition_type, short_label
            FROM batch_status
            WHERE preceding_code = ?
        """, (month_status,))
        transitions_for_month = cursor.fetchall()

        logger.debug(f"Inspecting transitions for month {month} with status {month_status}")
        for t in transitions_for_month:
            if t[3] == 'manual':
                logger.info(f"Found manual transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                manual_candidates.append((month, t))
            elif t[3] == 'retryable':
                logger.info(f"Found retryable transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
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
        import datetime
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
                last_completed_dt = datetime.datetime.strptime(last_completed_at, "%Y-%m-%d %H:%M:%S")
                now_utc = datetime.datetime.utcnow()
                elapsed = now_utc - last_completed_dt
                elapsed_days = elapsed.total_seconds() / (60 * 60 * 24)
            except Exception as e:
                logger.warning(f"Could not parse updated_at_utc ({last_completed_at}): {e}")
        if elapsed_days is not None and elapsed_days < 3:
            logger.info(f"Too soon for manual transition: Only {elapsed_days:.2f} days since upload for month {latest_month}. Minimum is 3 days. Skipping manual transition prompt.")
            # Skip suggesting manual transition, fall back to retryable or pipeline
            selected_type = None
        else:
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
            free_space = check_google_quota()
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
            free_space = check_google_quota()
            if free_space < staging_size:
                logger.warning(f"Not enough Google Drive space to upload month {latest_month}. Free space: {human_readable_size(free_space)}, Staging size: {human_readable_size(staging_size)}")
                logger.info("Pipeline transition aborted due to insufficient Google quota.")
                close_conn()
                sys.exit(0)
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
