import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import subprocess
import platform
import shutil
import argparse
import logging
from db.queries import get_next_code
from utils.utils import set_batch_status
from utils.logger import setup_logger
from constants import LOG_PATH, APPLE_PHOTOS_DB_COPY_PATH, APPLE_PHOTOS_DB_LOCK_PATH
import json
import socket
import errno
import sqlite3
from uuid import uuid4
import time
from datetime import datetime, timedelta
import tzlocal
from datetime import timezone
from dataclasses import dataclass
from typing import List
import atexit
from db.connections import get_connection, get_cursor, commit, close as close_conn

LOCK_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../db/executor.lock"))

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                pid = int(f.read().strip())
            # Check if process is still running
            os.kill(pid, 0)
            logger.info(f"⏭️ Executor is already running (PID: {pid}). Exiting.")
            sys.exit(0)
        except (ValueError, OSError):
            # PID is not running, or lock file is invalid/stale
            pass

    try:
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
        atexit.register(release_lock)
        logger.info(f"🔒 Acquired execution lock file (PID: {os.getpid()}).")
    except Exception as e:
        logger.error(f"Failed to create lock file: {e}")
        sys.exit(1)

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("🔓 Released execution lock file.")
    except Exception as e:
        logger.warning(f"Failed to release lock file: {e}")

def read_lock_file():
    if not os.path.exists(APPLE_PHOTOS_DB_LOCK_PATH):
        return None
    try:
        with open(APPLE_PHOTOS_DB_LOCK_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return None

def write_lock_file(status, pid, started_at=None, latest_successful_refresh_utc="—"):
    try:
        lock_data = {
            "status": status,
            "pid": pid,
            "started_at": started_at,
            "host": socket.gethostname(),
            "latest_successful_refresh_utc": latest_successful_refresh_utc
        }
        with open(APPLE_PHOTOS_DB_LOCK_PATH, "w") as f:
            json.dump(lock_data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to write lock file: {e}")

def is_pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        return True

def acquire_db_lock():
    while True:
        lock = read_lock_file()
        last_refresh = "—"
        
        if lock:
            last_refresh = lock.get("latest_successful_refresh_utc", "—")
            status = lock.get("status")
            lock_pid = lock.get("pid")
            
            if status in ["refreshing", "planner_active"]:
                if is_pid_alive(lock_pid):
                    logger.info(f"ℹ️ Database copy is currently locked by {status} (PID: {lock_pid}). Waiting for lock release...")
                    time.sleep(10)
                    continue
                else:
                    logger.warning(f"⚠️ Found stale {status} lock file from dead PID {lock_pid}. Overriding lock.")
            elif status == "executor_active" and lock_pid == os.getpid():
                # Already own the lock
                return
        
        # Lock is available, acquire it
        logger.info(f"🔐 Acquiring executor database lock (PID: {os.getpid()}).")
        write_lock_file(
            status="executor_active",
            pid=os.getpid(),
            started_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            latest_successful_refresh_utc=last_refresh
        )
        break

def release_db_lock():
    lock = read_lock_file()
    if lock and lock.get("pid") == os.getpid() and lock.get("status") == "executor_active":
        logger.info("🔓 Releasing executor database lock.")
        write_lock_file(
            status="available",
            pid=None,
            latest_successful_refresh_utc=lock.get("latest_successful_refresh_utc", "—")
        )



@dataclass
class PipelineStep:
    # description: str
    label: str
    code: str
    command: List[str]

def interactive_mode(all_steps, bootstrap_count):
    print("\n📋 Pipeline Step Selection (interactive mode)")
    print("============================================")
    print(" Bootstrap Steps")
    for idx, step in enumerate(all_steps[:bootstrap_count]):
        print(f"  {idx:>2}: {step.label}")
    print("\n Regular Steps")
    for idx, step in enumerate(all_steps[bootstrap_count:], start=bootstrap_count):
        print(f"  {idx:>2}: {step.label}")
    print("============================================")

    default_from = 0
    from_input = input(f"\n🔢 Enter START step index [default: {default_from}]: ").strip()
    from_index = int(from_input) if from_input else default_from

    default_to = len(all_steps) - 1
    to_input = input(f"🔢 Enter END step index (inclusive) [default: {default_to}]: ").strip()
    to_index = int(to_input) + 1 if to_input else default_to + 1

    return from_index, to_index


MODULE_TAG = "run_pipeline"

# Generate a unique session ID
session_id = str(uuid4())
logger = setup_logger(LOG_PATH, MODULE_TAG, extra_fields={"session_id": session_id})
logger.info(f"🆔 Session ID: {session_id}")
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))


def run_bootstrap_steps(bootstrap_steps, from_index, to_index, dry_run, sync_metadata_label, month):
    for i, step in enumerate(bootstrap_steps):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(step, dry_run, month):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            sys.exit(1)

def run_regular_steps(bootstrap_steps, steps, from_index, to_index, dry_run, month):
    for i, step in enumerate(steps, start=len(bootstrap_steps)):
        if i < from_index or (to_index is not None and i >= to_index):
            continue

        if step.code and not month: # A step with a code requires a month batch
            logger.warning(f"⚠️ No batch found in status {step.code} — skipping step {step.label}")
            continue

        # --- Begin status check logic ---
        # For a planned month, we check its current status against what the step expects.
        if month and step.code:
            acquire_db_lock()
            conn = get_connection()
            cur_status = conn.cursor()
            # Find the status of the month we are supposed to process
            cur_status.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
            row = cur_status.fetchone()
            batch_status_code = row[0] if row else None
            
            expected_prev_code = None
            if batch_status_code:
                cur_status.execute("SELECT preceding_code FROM batch_status WHERE code = ? AND transition_type IN ('pipeline', 'retryable')", (step.code,))
                expected_prev = cur_status.fetchone()
                expected_prev_code = expected_prev[0] if expected_prev else None
                
            close_conn()
            release_db_lock()
            
            if batch_status_code and expected_prev_code and batch_status_code != expected_prev_code:
                # Allow retry if the batch is in the error state of the CURRENT step
                if batch_status_code == str(step.code) + 'E':
                    logger.info(f"🔄 Retrying failed step '{step.label}' for month {month} (Current status: {batch_status_code}).")
                else:
                    logger.info(f"⏭️ Skipping step '{step.label}' for month {month}. Its status '{batch_status_code}' doesn't match the expected preceding status '{expected_prev_code}'.")
                    continue
        # --- End status check logic ---

        # Prompt for confirmation if about to pull favorites (Step 550)
        if step.code == '550' and not dry_run:
            print(f"\n⚠️  Manual verification required for {month} (Step: {step.label})")
            confirm = input(f"Have all old assets for {month} NOT uploaded by this app been removed from Google Account? (Required to accurately pull Favorites) [y/N]: ").strip().lower()
            if confirm != 'y':
                logger.error(f"❌ Execution halted: Old assets for {month} must be removed from Google Account before pulling favorites to ensure matching accuracy.")
                sys.exit(1)

        # Prompt for confirmation for Cleanup (Step 650)
        if step.code == '650' and not dry_run:
            print(f"\n⚠️  Action Required: Google Drive Storage Cleanup for {month}")
            print("The pipeline is ready to transition this batch to 'Cleaned'.")
            print("You must manually delete these photos from your Google Photos Library and Trash to free up space.")
            confirm = input("Are you ready to proceed with the interactive cleanup script? [y/N]: ").strip().lower()
            if confirm != 'y':
                logger.warning(f"⏭️ Skipping cleanup for {month} at user request.")
                continue

        # Prepare command with current_month replaced if available
        command = [arg.replace("{month}", month) if month else arg for arg in step.command]

        if not run_step(step, dry_run, month, command):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            sys.exit(1)

def log_execution(conn, label, status, batch_month_id=None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_executions (session_id, label, status, batch_month_id) VALUES (?, ?, ?, ?)
    """, (session_id, label, status, batch_month_id))
    conn.commit()

def run_step(step: PipelineStep, dry_run=False, month=None, command=None):
    logger.info(f"▶️ Starting: {step.label}")
    batch_month_id = None
    if month is not None:
        acquire_db_lock()
        conn = get_connection()
        cur_lookup = conn.cursor()
        cur_lookup.execute("SELECT id FROM month_batches WHERE month = ?", (month,))
        row = cur_lookup.fetchone()
        if row:
            batch_month_id = row[0]
        close_conn()
        release_db_lock()
        
    if dry_run:
        cmd_str = ' '.join(command if command else step.command)
        logger.info(f"[Dry Run] Would run: {cmd_str}")
        acquire_db_lock()
        conn = get_connection()
        log_execution(conn, step.label, "dry-run", batch_month_id)
        close_conn()
        release_db_lock()
        return True
        
    try:
        cmd_to_run = command if command else step.command
        subprocess.run(cmd_to_run, check=True)
        logger.info(f"✅ Completed: {step.label}")
        
        acquire_db_lock()
        conn = get_connection()
        cursor = conn.cursor()
        log_execution(conn, step.label, "success", batch_month_id)
        
        # Always update batch status centrally after a successful step with a valid code
        if step.code and month is not None:
            # For upload steps, the script itself determines the final status (399 or 400).
            # We read the actual status from the database instead of blindly setting it to step.code.
            if step.code in ['399', '400']:
                cursor.execute("SELECT status_code FROM month_batches WHERE month = ?", (month,))
                actual_status_row = cursor.fetchone()
                resolved_code = actual_status_row[0] if actual_status_row else step.code
            else:
                resolved_code = step.code

            next_code = get_next_code(cursor, resolved_code)

            if not next_code:
                # Look for any pipeline step that has current step as preceding_code (even if added later)
                cursor.execute("""
                    SELECT code FROM batch_status
                    WHERE preceding_code = ? AND transition_type = 'pipeline'
                    ORDER BY code ASC
                    LIMIT 1
                """, (resolved_code,))
                row = cursor.fetchone()
                if row:
                    next_code = row[0]

            if next_code:
                cursor.execute("SELECT transition_type FROM batch_status WHERE code = ?", (next_code,))
                row = cursor.fetchone()
                transition_type = row[0] if row else None

                # Update the batch to the resolved step's code regardless of the next transition type
                set_batch_status(cursor, month, resolved_code, session_id=session_id)
                logger.info(f"✅ Batch {month} status updated to {resolved_code}")

                # Log and update associated import records
                cursor.execute("""
                    SELECT DISTINCT a.import_id
                    FROM assets a
                    JOIN month_batches mb ON a.month = mb.month
                    WHERE mb.month = ?
                """, (month,))
                import_uuids = [row[0] for row in cursor.fetchall()]
                logger.info(f"🔎 Imports to update for month {month}: {import_uuids}")
                cursor.execute("""
                    UPDATE imports
                    SET execution_id = ?, status_code = ?
                    WHERE import_uuid IN (
                        SELECT DISTINCT a.import_id
                        FROM assets a
                        JOIN month_batches mb ON a.month = mb.month
                        WHERE mb.month = ?
                    )
                """, (session_id, resolved_code, month))
                logger.info(f"📌 Updated imports for month {month} with execution_id={session_id}, status_code={resolved_code}")
                
                if transition_type == 'manual':
                    logger.info(f"⏸️ Next transition from {resolved_code} is manual. Halting automated execution.")

                conn.commit()
            else:
                # Final step reached for this month, set batch status and update imports
                set_batch_status(cursor, month, resolved_code, session_id=session_id)
                # Log which import_uuids will be updated
                cursor.execute("""
                    SELECT DISTINCT a.import_id
                    FROM assets a
                    JOIN month_batches mb ON a.month = mb.month
                    WHERE mb.month = ?
                """, (month,))
                import_uuids = [row[0] for row in cursor.fetchall()]
                logger.info(f"🔎 Imports to update for month {month}: {import_uuids}")
                # Update imports table with execution_id and status_code
                cursor.execute("""
                    UPDATE imports
                    SET execution_id = ?, status_code = ?
                    WHERE import_uuid IN (
                        SELECT DISTINCT a.import_id
                        FROM assets a
                        JOIN month_batches mb ON a.month = mb.month
                        WHERE mb.month = ?
                    )
                """, (session_id, resolved_code, month))
                conn.commit()
                logger.info(f"🏁 Final step reached for month {month}; batch status set and imports updated with execution_id={session_id}, status_code={resolved_code}")
                
        close_conn()
        release_db_lock()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {step.label} with error: {e}")
        
        acquire_db_lock()
        conn = get_connection()
        cursor = conn.cursor()
        log_execution(conn, step.label, "failed", batch_month_id)
        if step.code is not None and month is not None:
            error_code = None
            cursor.execute("SELECT code FROM batch_status WHERE code = ?", (step.code + 'E',))
            row = cursor.fetchone()
            if row:
                error_code = row[0]
            if error_code:
                set_batch_status(cursor, month, error_code, session_id=session_id)
                conn.commit()
                logger.info(f"⚠️ Batch {month} moved to error state {error_code} due to failure in step {step.label}.")
        close_conn()
        release_db_lock()
        return False

def is_applescript_available():
    return platform.system() == "Darwin" and shutil.which("osascript") is not None

def get_batch_status_metadata(cursor, code):
    cursor.execute("""
        SELECT short_label, full_description, script_name
        FROM batch_status WHERE code = ?
    """, (code,))
    return cursor.fetchone()

def get_current_quarter_start(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)

def get_pipeline_steps(script_dir, use_mock_data=False):
    """
    Fetches and constructs the pipeline steps from the batch_status table.
    If use_mock_data is True, returns a hardcoded list of steps for testing.
    """
    if use_mock_data:
        logger.info("Using mocked pipeline steps data.")
        
        return [
            PipelineStep("2.1 Verify Smart Album", "100", [sys.executable, os.path.join(script_dir, "verify_smart_album.py"), "{month}"]),
            PipelineStep("2.2 Export Assets", "200", [sys.executable, os.path.join(script_dir, "export_photos_applescript.py"), "{month}"]),
            PipelineStep("2.2.5 Remove duplicate assets based on extension and size", "210", [sys.executable, os.path.join(script_dir, "deduplicate_assets.py"), "{month}"]),
            PipelineStep("2.4 Partial upload to Google Photos due to insufficient space", "399", [sys.executable, os.path.join(script_dir, "upload_to_google_photos.py"), "{month}"]),
            PipelineStep("3.2 Pull Google Photos Favorites and update asset flags", "550", [sys.executable, os.path.join(script_dir, "pull_google_favorites.py"), "{month}"]),
            PipelineStep("3.4 Rank Assets by Score", "600", [sys.executable, os.path.join(script_dir, "rank_assets_by_score.py"), "{month}"]),
        ]

    steps = []
    logger.info("Fetching pipeline steps from the database.")
    acquire_db_lock()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipeline_stage, full_description, code, script_name, transition_type
        FROM batch_status
        WHERE code GLOB '[0-9][0-9][0-9]'
          AND script_name NOT LIKE '%generate_month_batches.py%'
        ORDER BY code
    """)
    rows = cursor.fetchall()
    for pipeline_stage, full_description, code, script_name, transition_type in rows:
        label = f"{pipeline_stage} {full_description}"
        cmd = []
        if script_name:
            script_path = os.path.join(script_dir, script_name.split()[0])
            cmd = [sys.executable, script_path]
            if "{month}" in script_name:
                cmd.append("{month}")
        steps.append(PipelineStep(label, code, cmd))
    close_conn()
    release_db_lock()
    return [step for step, (_, _, _, _, ttype) in zip(steps, rows) if ttype in ['pipeline', 'retryable']]


def main(args):
    acquire_lock()
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        #PipelineStep("0.0.5 Pipeline Status Overview", "", ["python3", os.path.join(SCRIPT_DIR, "pipeline_planner.py")]),
        #PipelineStep("0.1 Storage Status", "", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        # PipelineStep("0.4 Sync Assets from Photos DB", "", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_assets.py")]),
        # PipelineStep("1.1 Detect Gaps", "000", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    steps = get_pipeline_steps(SCRIPT_DIR, use_mock_data=args.mock_steps)

    all_steps = bootstrap_steps.copy()
    all_steps.extend(steps)

    # Check for active planned executions in queue
    acquire_db_lock()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, planned_month FROM planned_execution WHERE active = 1 ORDER BY id ASC")
    planned_rows = cursor.fetchall()
    close_conn()
    release_db_lock()

    if not planned_rows:
        if args.cron:
            logger.info("No active planned execution found in queue. Exiting silently.")
            sys.exit(0)
        else:
            logger.error("🚫 No active planned execution found in queue. Please run pipeline_planner first.")
            sys.exit(1)

    total_plans = len(planned_rows)
    logger.info(f"📋 Found {total_plans} active planned execution(s) in queue: {[r[1] for r in planned_rows]}")

    from_index, to_index = 0, len(all_steps)

    for idx, (plan_id, month) in enumerate(planned_rows, 1):
        logger.info(f"\n{'='*60}\n🚀 [{idx}/{total_plans}] Starting execution for planned batch: {month} (Queue ID: {plan_id})\n{'='*60}")
        
        run_bootstrap_steps(bootstrap_steps, from_index, to_index, args.dry_run, None, month)
        run_regular_steps(bootstrap_steps, steps, from_index, to_index, args.dry_run, month)

        # Mark this specific planned execution as inactive upon successful completion
        if not args.dry_run:
            acquire_db_lock()
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE planned_execution SET active = 0 WHERE id = ?", (plan_id,))
            conn.commit()
            close_conn()
            release_db_lock()
            logger.info(f"✅ Planned execution for {month} (Queue ID: {plan_id}) completed and marked as inactive.")

    logger.info(f"🎉 Completed all {total_plans} queued planned execution(s).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute the media organizer pipeline.")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing them.")
    parser.add_argument("--mock-steps", action="store_true", help="Use mocked pipeline steps instead of querying the database.")
    parser.add_argument("--cron", action="store_true", help="Run in cron mode (silent exit with 0 if no active plan).")
    args = parser.parse_args()

    main(args)
