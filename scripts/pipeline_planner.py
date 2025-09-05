import sys
import os
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from utils.logger import setup_logger
from constants import LOG_PATH, STAGING_ROOT
from utils.utils import get_full_transition_path
from upload_to_google_photos import check_google_quota
import argparse
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH


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
    apple_photos_db_path = os.path.expanduser("~/Pictures/Photos Library.photoslibrary/database/Photos.sqlite")
    if not os.path.exists(apple_photos_db_path):
        return True  # If DB does not exist, better to run sync

    db_mod_time = os.path.getmtime(apple_photos_db_path)

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
        ("0.0.0 Copy all media DB", "copy_all_media_db.py", []),
        ("0.0.1 Check storage status", "storage_status.py", ["--migrate"]),
        ("0.0.3 Sync metadata", "sync_photos_metadata.py", []),
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
                        conn.close()
            subprocess.run(["python3", script_path] + step_args, check=True)
            logger.info(f"✅ Completed: {step_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error in bootstrap step {step_name}: {e}")
            sys.exit(1)

def get_stage_transitions(cursor):
    cursor.execute("""
        SELECT code, preceding_code, full_description
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND code NOT LIKE '%E'
    """)
    return cursor.fetchall()

def get_batch_statuses(cursor):
    cursor.execute("""
        SELECT month, status_code
        FROM month_batches
    """)
    return cursor.fetchall()

def get_latest_import_and_month(cursor):
    # Placeholder: replace with actual logic to fetch latest import and complete month
    cursor.execute("""
        SELECT DISTINCT i.import_uuid, a.month
        FROM imports i
        LEFT JOIN assets a ON a.import_id = i.import_uuid
        LEFT JOIN month_batches m ON m.month = a.month
        WHERE (latest_import_id < i.import_uuid OR latest_import_id IS NULL)
        AND (m.status_code < (
                SELECT code
                FROM batch_status
                WHERE preceding_code IS NOT NULL
                AND LENGTH(code) = 3
                ORDER BY code DESC
                LIMIT 1
            ) OR m.status_code IS NULL)
        -- exclude current month to avoid incomplete batch
        AND a.month < strftime('%Y-%m', 'now')
        ORDER BY i.import_uuid DESC, a.month DESC
        LIMIT 1;
    """)
    return cursor.fetchone()


def display_summary(transitions, batches, latest_import, latest_month):
    print("\n=== 📊 Stage Transitions ===")
    for code, prev, desc in transitions:
        print(f"{prev} ➜ {code}: {desc}")

    print("\n=== 📦 Batch Statuses ===")
    for month, status in batches:
        print(f"Month: {month}, Status: {status}")

    print("\n=== 🗓️ Latest Info ===")
    print(f"Latest Import Month: {latest_import}")
    print(f"Latest Complete Month: {latest_month}")

def main(auto_apply):
    logger = setup_logger(LOG_PATH, "pipeline_planner")

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Run bootstrap steps before proceeding
    run_bootstrap_steps(auto_apply, logger)

    # Check for months with status_code 399
    cursor.execute("SELECT month FROM month_batches WHERE status_code = 399 ORDER BY month DESC")
    months_399 = cursor.fetchall()
    if months_399:
        latest_399_month = months_399[0][0]
        free_space = check_google_quota()
        import glob

        # base_staging_dir = os.path.expanduser("~/staging")÷
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
                    logger.info(f"Found uploaded asset: {file_path}, size: {human_readable_size(file_size)}")
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
    # End of added logic

    transitions = get_stage_transitions(cursor)
    batches = get_batch_statuses(cursor)
    latest_import, latest_month = get_latest_import_and_month(cursor)

    display_summary(transitions, batches, latest_import, latest_month)

    logger.info("=== ✅ Suggested Action ===")
    current_status = None
    for month, status in batches:
        if month == latest_month:
            current_status = status
            break

    transitions_str = get_full_transition_path(transitions, current_status)
    logger.info(f"Run pipeline for: Month={latest_month}, Transitions={transitions_str}")

    if not auto_apply:
        proceed = input("Proceed with this plan? [y/N]: ")
        if proceed.strip().lower() != 'y':
            logger.info("Aborted by user.")
            conn.close()
            sys.exit(0)

    logger.info("🚀 Executing planned steps...")
    set_planned_month(cursor, latest_month)
    conn.commit()
    logger.info(f"📌 Month {latest_month} recorded in planned_execution for next pipeline run.")
    # TODO: trigger executor or store plan

    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-apply", action="store_true", help="Skip confirmation and apply plan immediately")
    args = parser.parse_args()
    main(auto_apply=args.auto_apply)
