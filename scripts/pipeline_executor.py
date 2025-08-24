import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import subprocess
import platform
import shutil
import logging
from db.queries import get_next_code
from utils.utils import set_batch_status
from utils.logger import setup_logger
from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_PATH
import sqlite3
from uuid import uuid4
import time
from datetime import datetime, timedelta
import tzlocal
from datetime import timezone
from dataclasses import dataclass
from typing import List

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

def run_bootstrap_steps(bootstrap_steps, from_index, to_index, dry_run, sync_metadata_label, conn, month):
    for i, step in enumerate(bootstrap_steps):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if step.label == sync_metadata_label:
            if not should_run_sync_metadata(step.label):
                continue
        if not run_step(conn, step, dry_run):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            conn.close()
            sys.exit(1)

def run_regular_steps(bootstrap_steps, steps, from_index, to_index, dry_run, month, conn):
    for i, step in enumerate(steps, start=len(bootstrap_steps)):
        if i < from_index or (to_index is not None and i >= to_index):
            continue

        current_month = month
        if step.code and not current_month:
            logger.warning(f"⚠️ No batch found in status {step.code} — skipping step {step.label}")
            continue

        # Prepare command with current_month replaced if available
        command = [arg.replace("{month}", current_month) if current_month else arg for arg in step.command]

        if not run_step(conn, step, dry_run, current_month, command):
            logger.error(f"❌ Pipeline execution halted. Session ID: {session_id}")
            conn.close()
            sys.exit(1)

def log_execution(conn, label, status, batch_month_id=None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_executions (session_id, label, status, batch_month_id) VALUES (?, ?, ?, ?)
    """, (session_id, label, status, batch_month_id))
    conn.commit()

def run_step(conn, step: PipelineStep, dry_run=False, month=None, command=None):
    logger.info(f"▶️ Starting: {step.label}")
    batch_month_id = None
    if month is not None:
        cur_lookup = conn.cursor()
        cur_lookup.execute("SELECT id FROM month_batches WHERE month = ?", (month,))
        row = cur_lookup.fetchone()
        if row:
            batch_month_id = row[0]
    if dry_run:
        cmd_str = ' '.join(command if command else step.command)
        logger.info(f"[Dry Run] Would run: {cmd_str}")
        log_execution(conn, step.label, "dry-run", batch_month_id)
        return True
    try:
        cmd_to_run = command if command else step.command
        subprocess.run(cmd_to_run, check=True)
        logger.info(f"✅ Completed: {step.label}")
        log_execution(conn, step.label, "success", batch_month_id)
        # Always update batch status centrally after a successful step with a valid code
        if step.code:
            if month is not None:
                cursor = conn.cursor()
                next_code = get_next_code(cursor, step.code)
                if next_code is not None:
                    set_batch_status(cursor, month, next_code, session_id=session_id)
                    conn.commit()
                else:
                    logger.info(f"🎯 Final step reached for month {month}, no status transition needed.")
                    conn.commit()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {step.label} with error: {e}")
        log_execution(conn, step.label, "failed", batch_month_id)
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
        if last_successful_run:
            try:
                utc_dt = datetime.strptime(last_successful_run, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                local_tz = tzlocal.get_localzone()
                local_dt = utc_dt.astimezone(local_tz)
                logger.info(f"*** last_successful_run (converted to Local): {local_dt}")
            except Exception as conv_err:
                logger.warning(f"⚠️ Failed to convert last_successful_run to local time: {conv_err}")
        db_conn.close()

        photos_db_mtime = os.path.getmtime(APPLE_PHOTOS_DB_PATH)
        photos_db_mtime_dt = datetime.fromtimestamp(photos_db_mtime, tz=local_tz)
        logger.info(f"Latest Apple Photos DB mtime: {photos_db_mtime_dt}")

        now_dt = datetime.now(local_tz)
        quarter_start = get_current_quarter_start(now_dt.replace(tzinfo=local_tz))

        logger.debug(f"📁 Evaluating whether to run: {label}")
        logger.debug(f"🕒 Now: {now_dt}")
        logger.debug(f"🕒 Current 15-min interval start: {quarter_start}")
        # if local_dt and local_dt >= photos_db_mtime_dt:
        minutes_since_last_sync = (now_dt - local_dt).total_seconds() / 60
        logger.info(f"⏱️ Minutes since last sync: {minutes_since_last_sync:.1f}")
        if minutes_since_last_sync < 15:
            logger.info(f"Decision: Skipping 0.3 Sync Metadata - last sync was {minutes_since_last_sync:.1f} minutes ago.")
            return False
        else:
            logger.info("Decision: Running 0.3 Sync Metadata.")
            return True
    except Exception as e:
        logger.warning(f"⚠️ Could not evaluate last sync time: {e}")
        return True

def main():
    dry_run = "--dry-run" in sys.argv
    from_index = 0
    to_index = None
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Open a single SQLite connection to be used throughout
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Lookup label for sync_photos_metadata.py step dynamically
    cursor.execute("""
        SELECT code, short_label FROM batch_status WHERE script_name LIKE '%sync_photos_metadata.py%'
    """)
    row = cursor.fetchone()
    sync_metadata_label = f"*** sync_metadata_label: {row[0]} {row[1]}" if row else "0.3 Sync Metadata from Photos DB"
    logger.info(sync_metadata_label)

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        #PipelineStep("0.0.5 Pipeline Status Overview", "", ["python3", os.path.join(SCRIPT_DIR, "pipeline_planner.py")]),
        #PipelineStep("0.1 Storage Status", "", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        #PipelineStep(sync_metadata_label, "", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_metadata.py")]),
        # PipelineStep("0.4 Sync Assets from Photos DB", "", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_assets.py")]),
        # PipelineStep("1.1 Detect Gaps", "000", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    steps = []
    cursor.execute("""
        SELECT pipeline_stage, full_description, code, script_name
        FROM batch_status
        WHERE code GLOB '[0-9][0-9][0-9]'
        ORDER BY code
    """)
    for pipeline_stage, full_description, code, script_name in cursor.fetchall():
        label = f"{pipeline_stage} {full_description}"
        script_path = os.path.join(SCRIPT_DIR, script_name.split()[0])  # Strip {month} if present
        cmd = ["python3", script_path]

        # Append any extra arguments (like {month}) dynamically during execution
        if "{month}" in script_name:
            cmd.append("{month}")  # Placeholder to be replaced at execution time

        steps.append(PipelineStep(label, code, cmd))

    all_steps = bootstrap_steps.copy()
    all_steps.extend(steps)

    # Check for planned execution before interactive mode
    cursor.execute("SELECT planned_month FROM planned_execution WHERE active = 1 LIMIT 1")
    planned_row = cursor.fetchone()
    if planned_row:
        month = planned_row[0]
        logger.info(f"📋 Planned execution found. Using batch: {month}")
        from_index, to_index = 0, len(all_steps)
    else:
        logger.error("🚫 No active planned execution found. Please run pipeline_planner first.")
        conn.close()
        sys.exit(1)

    selected_steps = all_steps[from_index:to_index]

    # Check for batches in error state
    cur_err = conn.cursor()
    cur_err.execute("SELECT month, status_code FROM month_batches WHERE status_code LIKE '%E'")
    error_batches = cur_err.fetchall()

    # month = None
    # Determine if any selected step requires a batch status code (non-empty code)
    requires_batch = any(step.code for step in selected_steps)

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
            # Extract first step that requires a batch code
            first_code_step = next((step for step in selected_steps if step.code), None)
            # month = None
            if first_code_step:
                step_code = first_code_step.code
                if step_code == '000':
                    logger.info(f"ℹ️ Step {first_code_step.label} (code {step_code}) has no prerequisites and will run unconditionally.")
            if not month:
                if first_code_step and first_code_step.code == '000':
                    logger.info("ℹ️ Proceeding without batch for step 000.")
                else:
                    logger.error(f"🚫 No eligible batch found to process for step {first_code_step.label} (code {first_code_step.code}).")
                    # Show batch_status short_label for visibility
                    cursor.execute('''
                        SELECT mb.month, mb.status_code, bs.short_label
                        FROM month_batches mb
                        LEFT JOIN batch_status bs ON mb.status_code = bs.code
                        ORDER BY mb.month
                    ''')
                    all_batches = cursor.fetchall()
                    if all_batches:
                        logger.info("📋 Current month_batches (with status labels):")
                        for m, s, label in all_batches:
                            label_display = f" ({label})" if label else ""
                            logger.info(f" - Month: {m}, Status: {s}{label_display}")
                    else:
                        logger.info("ℹ️ No entries in month_batches table.")
                    conn.close()
                    return 
            logger.info(f"📦 Batch selected: {month}")

    run_bootstrap_steps(bootstrap_steps, from_index, to_index, dry_run, sync_metadata_label, conn, month)
    run_regular_steps(bootstrap_steps, steps, from_index, to_index, dry_run, month, conn)

    # If a planned execution was used, mark it as inactive
    if planned_row:
        cursor.execute("UPDATE planned_execution SET active = 0 WHERE planned_month = ?", (month,))
        conn.commit()

    start_index = len(bootstrap_steps) + len(steps)
    conn.close()

if __name__ == "__main__":
    main()
