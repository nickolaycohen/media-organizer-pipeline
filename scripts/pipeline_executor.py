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
from constants import LOG_PATH, APPLE_PHOTOS_DB_COPY_PATH
import sqlite3
from uuid import uuid4
import time
from datetime import datetime, timedelta
import tzlocal
from datetime import timezone
from dataclasses import dataclass
from typing import List
from db.connections import get_connection, get_cursor, commit, close as close_conn


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


def run_bootstrap_steps(bootstrap_steps, from_index, to_index, dry_run, sync_metadata_label, conn, month):
    for i, step in enumerate(bootstrap_steps):
        if i < from_index or (to_index is not None and i >= to_index):
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

        # --- Begin status check logic ---
        if current_month and step.code:
            cur_status = conn.cursor()
            cur_status.execute("SELECT status_code FROM month_batches WHERE month = ?", (current_month,))
            row = cur_status.fetchone()
            if row:
                batch_status_code = row[0]
                cursor = conn.cursor()
                cursor.execute("SELECT preceding_code FROM batch_status WHERE code = ?", (step.code,))
                expected_prev = cursor.fetchone()
                expected_prev_code = expected_prev[0] if expected_prev else None
                if expected_prev_code and batch_status_code != expected_prev_code:
                    logger.info(f"⏭️ Skipping step {step.label} for month {current_month} as current status {batch_status_code} does not match expected preceding code {expected_prev_code}")
                    continue
        # --- End status check logic ---

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
        if step.code and month is not None:
            cursor = conn.cursor()
            next_code = get_next_code(cursor, step.code)

            if not next_code:
                # Look for any pipeline step that has current step as preceding_code (even if added later)
                cursor.execute("""
                    SELECT code FROM batch_status
                    WHERE preceding_code = ? AND transition_type = 'pipeline'
                    ORDER BY code ASC
                    LIMIT 1
                """, (step.code,))
                row = cursor.fetchone()
                if row:
                    next_code = row[0]

            if next_code:
                cursor.execute("SELECT transition_type FROM batch_status WHERE code = ?", (next_code,))
                row = cursor.fetchone()
                transition_type = row[0] if row else None
                if transition_type != 'manual':
                    set_batch_status(cursor, month, step.code, session_id=session_id)
                    logger.info(f"✅ Batch {month} status updated to {next_code}")
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
                    """, (session_id, step.code, month))
                    logger.info(f"📌 Updated imports for month {month} with execution_id={session_id}, status_code={step.code}")
                    conn.commit()
                else:
                    logger.info(f"⏸️ Skipping manual transition {step.code}")
                conn.commit()
            else:
                # Final step reached for this month, set batch status and update imports
                set_batch_status(cursor, month, step.code, session_id=session_id)
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
                """, (session_id, step.code, month))
                conn.commit()
                logger.info(f"🏁 Final step reached for month {month}; batch status set and imports updated with execution_id={session_id}, status_code={step.code}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {step.label} with error: {e}")
        log_execution(conn, step.label, "failed", batch_month_id)
        if step.code is not None and month is not None:
            cursor = conn.cursor()
            error_code = None
            cursor.execute("SELECT code FROM batch_status WHERE code = ?", (step.code + 'E',))
            row = cursor.fetchone()
            if row:
                error_code = row[0]
            if error_code:
                set_batch_status(cursor, month, error_code, session_id=session_id)
                conn.commit()
                logger.info(f"⚠️ Batch {month} moved to error state {error_code} due to failure in step {step.label}.")
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



def main():
    dry_run = "--dry-run" in sys.argv
    from_index = 0
    to_index = None
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Open a single SQLite connection to be used throughout
    conn = get_connection()
    cursor = get_cursor()

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        #PipelineStep("0.0.5 Pipeline Status Overview", "", ["python3", os.path.join(SCRIPT_DIR, "pipeline_planner.py")]),
        #PipelineStep("0.1 Storage Status", "", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        # PipelineStep("0.4 Sync Assets from Photos DB", "", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_assets.py")]),
        # PipelineStep("1.1 Detect Gaps", "000", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    steps = []
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
        if script_name:
            script_path = os.path.join(SCRIPT_DIR, script_name.split()[0])
            cmd = ["python3", script_path]
            if "{month}" in script_name:
                cmd.append("{month}")
        else:
            cmd = []
        steps.append(PipelineStep(label, code, cmd))
    steps = [step for step, (_, _, _, _, ttype) in zip(steps, rows) if ttype == 'pipeline']

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
                # Reset the batch status to the previous valid code (strip 'E')
                prev_code = error_batches[0][1]
                if prev_code and prev_code.endswith('E'):
                    clean_code = prev_code[:-1]
                    cursor.execute(
                        "UPDATE month_batches SET status_code = ? WHERE month = ?",
                        (clean_code, month)
                    )
                    conn.commit()
                    logger.info(f"♻️ Reset batch {month} status from {prev_code} to {clean_code}")
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

    run_bootstrap_steps(bootstrap_steps, from_index, to_index, dry_run, None, conn, month)
    run_regular_steps(bootstrap_steps, steps, from_index, to_index, dry_run, month, conn)

    # If a planned execution was used, mark it as inactive
    if planned_row:
        cursor.execute("UPDATE planned_execution SET active = 0 WHERE planned_month = ?", (month,))
        conn.commit()

    start_index = len(bootstrap_steps) + len(steps)
    conn.close()

if __name__ == "__main__":
    main()
