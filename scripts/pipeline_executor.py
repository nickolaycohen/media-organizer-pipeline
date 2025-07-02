import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import subprocess
import platform
import shutil
import logging
from db.queries import get_month_batch
from utils.utils import set_batch_status
from utils.logger import setup_logger
from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH
import sqlite3
from uuid import uuid4
import time
from datetime import datetime, timedelta
import constants
import tzlocal
from datetime import timezone


MODULE_TAG = "run_pipeline"

# Generate a unique session ID
session_id = str(uuid4())
logger = setup_logger(LOG_PATH, MODULE_TAG, extra_fields={"session_id": session_id})
logger.info(f"🆔 Session ID: {session_id}")

# DB_PATH = os.path.join(os.path.dirname(__file__), "..", "media_organizer.db")

def log_execution(label, status):
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_executions (session_id, label, status) VALUES (?, ?, ?)
    """, (session_id, label, status))
    conn.commit()
    conn.close()

def run_step(label, batch_status_code, command, dry_run=False, month=None):
    logger.info(f"▶️ Starting: {label}")
    if dry_run:
        logger.info(f"[Dry Run] Would run: {' '.join(command)}")
        log_execution(label, "dry-run")
        return True
    try:
        subprocess.run(command, check=True)
        logger.info(f"✅ Completed: {label}")
        log_execution(label, "success")
        if month is not None:
            conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            cursor = conn.cursor()
            set_batch_status(cursor, month, batch_status_code)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {label} with error: {e}")
        log_execution(label, "failed")
        return False

def is_applescript_available():
    return platform.system() == "Darwin" and shutil.which("osascript") is not None

def get_batch_status_metadata(cursor, code):
    cursor.execute("""
        SELECT short_label, full_description, script_name
        FROM batch_status WHERE code = ?
    """, (code,))
    return cursor.fetchone()

def main():
    dry_run = "--dry-run" in sys.argv
    from_index = 0
    to_index = None
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Lookup label for sync_photos_metadata.py step dynamically
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT code, short_label FROM batch_status WHERE script_name LIKE '%sync_photos_metadata.py%'
    """)
    row = cursor.fetchone()
    sync_metadata_label = f"{row[0]} {row[1]}" if row else "0.3 Sync Metadata from Photos DB"
    logger.info("*** sync_metadata_label:", sync_metadata_label)
    conn.close()

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        ("0.1 Storage Status", "", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        (sync_metadata_label, "" ,["python3", os.path.join(SCRIPT_DIR, "sync_photos_metadata.py")]),
        ("1.1 Detect Gaps", "000", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    step_codes = ['100', '200']
    steps = []

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    for code in step_codes:
        meta = get_batch_status_metadata(cursor, code)
        if meta:
            short_label, _, script_name = meta
            label = f"{code} {short_label}"
            cmd = ["python3", os.path.join(SCRIPT_DIR, script_name)]
            steps.append((label, code, cmd))
    conn.close()

    remaining_steps = [
        ("2.3 Verify Staging", ["python3", "scripts/verify_staging.py"]),
        ("2.3.5 Sync Photo Metadata", ["python3", "scripts/sync_photos_assets.py"]),
        ("2.4 Upload to Google Photos", ["python3", "scripts/upload_to_google_photos.py"]),
        ("3.2.5 Pull Google Favorites", ["python3", "scripts/pull_google_favorites.py"]),
        ("3.3 Rank Assets by Score", ["python3", "scripts/rank_assets_by_score.py"]),
    ]

    all_steps = bootstrap_steps.copy()
    all_steps.extend(steps)
    all_steps.extend(remaining_steps)

    if "--from" in sys.argv:
        from_index = int(sys.argv[sys.argv.index("--from") + 1])
    else:
        print("\n📋 Pipeline Step Selection (interactive mode)")
        print("============================================")
        for idx, step in enumerate(all_steps):
            label = step[0]
            print(f"  {idx:>2}: {label}")
        print("============================================")

        default_from = 0
        from_input = input(f"\n🔢 Enter START step index [default: {default_from}]: ").strip()
        from_index = int(from_input) if from_input else default_from

    if "--to" in sys.argv:
        to_index = int(sys.argv[sys.argv.index("--to") + 1])
    else:
        default_to = len(all_steps) - 1
        to_input = input(f"🔢 Enter END step index (inclusive) [default: {default_to}]: ").strip()
        to_index = int(to_input) + 1 if to_input else default_to + 1

    selected_steps = all_steps[from_index:to_index]

    # Check for batches in error state
    conn_err = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cur_err = conn_err.cursor()
    cur_err.execute("SELECT month, status_code FROM month_batches WHERE status_code LIKE '%E'")
    error_batches = cur_err.fetchall()
    conn_err.close()

    month = None
    conn = None
    # Determine if any selected step requires a batch status code (length 3)
    requires_batch = any(len(step) == 3 and step[1] for step in selected_steps)

    if requires_batch:
        if error_batches:
            print("\n⚠️  Error State Detected")
            for m, status in error_batches:
                print(f"  - Batch {m} is in error state ({status})")

            choice = input("\n❓ Retry failed batch? [y/N]: ").strip().lower()
            if choice == "y":
                month = error_batches[0][0]
                logger.info(f"🔁 Retrying failed batch: {month}")
            else:
                logger.info("➡️ Proceeding with next eligible batch.")
        if month is None:
            conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            cursor = conn.cursor()
            # Extract first step that requires a batch code
            first_code_step = next((step for step in selected_steps if len(step) == 3 and step[1]), None)
            month = get_month_batch(cursor, first_code_step[1]) if first_code_step else None
            if not month:
                logger.error(f"🚫 No eligible batch found to process for step {first_code_step[0]} (code {first_code_step[1]}).")
                cursor.execute('SELECT month, status_code FROM month_batches ORDER BY month')
                all_batches = cursor.fetchall()
                if all_batches:
                    logger.info("📋 Current month_batches:")
                    for m, s in all_batches:
                        logger.info(f" - Month: {m}, Status: {s}")
                else:
                    logger.info("ℹ️ No entries in month_batches table.")
                if conn:
                    conn.close()
                return
            logger.info(f"📦 Batch selected: {month}")


    def get_current_quarter_start(dt):
        minute = (dt.minute // 15) * 15
        return dt.replace(minute=minute, second=0, microsecond=0)

    def should_run_sync_metadata(label):
        try:

            db_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            db_cursor = db_conn.cursor()
            db_cursor.execute("""
                SELECT MAX(executed_at_utc) FROM pipeline_executions
                WHERE label = ? AND status = 'success'
            """, (label,))
            result = db_cursor.fetchone()
            last_successful_run = result[0] if result and result[0] else None
            logger.info(f"*** last_successful_run utc: {last_successful_run}")
            if last_successful_run:
                try:
                    utc_dt = datetime.strptime(last_successful_run, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    local_tz = tzlocal.get_localzone()
                    local_dt = utc_dt.astimezone(local_tz)
                    logger.info(f"*** last_successful_run (converted to Local): {local_dt}")
                except Exception as conv_err:
                    logger.warning(f"⚠️ Failed to convert last_successful_run to local time: {conv_err}")
            db_conn.close()

            photos_db_mtime = os.path.getmtime(constants.APPLE_PHOTOS_DB_PATH)
            photos_db_mtime_dt = datetime.fromtimestamp(photos_db_mtime, tz=local_tz)
            logger.info(f"Latest Apple Photos DB mtime: {photos_db_mtime_dt}")

            now_dt = datetime.now(local_tz)
            quarter_start = get_current_quarter_start(now_dt.replace(tzinfo=local_tz))

            logger.debug(f"📁 Evaluating whether to run: {label}")
            logger.debug(f"🕒 Now: {now_dt}")
            logger.debug(f"🕒 Current 15-min interval start: {quarter_start}")
            if local_dt and local_dt > photos_db_mtime_dt:
                minutes_since_last_sync = int((local_dt - photos_db_mtime_dt).total_seconds() // 60)
                logger.info(f"Decision: Skipping 0.3 Sync Metadata - last sync was {minutes_since_last_sync} minutes ago.")
                return False
            else:
                logger.info("Decision: Running 0.3 Sync Metadata.")
                return True
        except Exception as e:
            logger.warning(f"⚠️ Could not evaluate last sync time: {e}")
            return True

    for i, (label, batch_status_code, command) in enumerate(bootstrap_steps):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        logger.info(f"*** label: {label}")
        if label == sync_metadata_label:
            if not should_run_sync_metadata(label):
                continue
        if not run_step(label, batch_status_code, command, dry_run):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            if conn:
                conn.close()
            return

    for i, (label, batch_status_code, command) in enumerate(steps, start=len(bootstrap_steps)):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(label, batch_status_code, command, dry_run, month):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            if conn:
                conn.close()
            return

    start_index = len(bootstrap_steps) + len(steps)
    for j, (label, command) in enumerate(remaining_steps):
        i = start_index + j
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(label, command, dry_run, month):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            if conn:
                conn.close()
            return

    if conn:
        conn.close()

if __name__ == "__main__":
    main()
