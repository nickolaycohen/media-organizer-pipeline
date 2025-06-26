import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import subprocess
import platform
import shutil
import logging
from db.queries import get_month_batch_added
from utils.utils import set_batch_status
from utils.logger import setup_logger
from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH
import sqlite3
from uuid import uuid4

MODULE_TAG = "run_pipeline"

# Generate a unique session ID
session_id = str(uuid4())
logger = setup_logger(LOG_PATH, MODULE_TAG, extra_fields={"session_id": session_id})

# DB_PATH = os.path.join(os.path.dirname(__file__), "..", "media_organizer.db")

def log_execution(label, status):
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_executions (session_id, label, status) VALUES (?, ?, ?)
    """, (session_id, label, status))
    conn.commit()
    conn.close()

def run_step(label, command, dry_run=False, month=None):
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
            set_batch_status(month, label.split()[0])
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {label} with error: {e}")
        log_execution(label, "failed")
        return False

def is_applescript_available():
    return platform.system() == "Darwin" and shutil.which("osascript") is not None

def main():
    dry_run = "--dry-run" in sys.argv
    from_index = 0
    to_index = None
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        ("0.1 Storage Status", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        ("0.3 Sync Metadata from Photos DB", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_metadata.py")]),
        ("1.1 Detect Gaps", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    steps = [
        ("2.1 Verify Smart Album", ["python3", os.path.join(SCRIPT_DIR, "verify_export_album.py")]),
    ]

    remaining_steps = [
        ("2.3 Verify Staging", ["python3", "scripts/verify_staging.py"]),
        ("2.3.5 Sync Photo Metadata", ["python3", "scripts/sync_photos_assets.py"]),
        ("2.4 Upload to Google Photos", ["python3", "scripts/upload_to_google_photos.py"]),
        ("3.2.5 Pull Google Favorites", ["python3", "scripts/pull_google_favorites.py"]),
        ("3.3 Rank Assets by Score", ["python3", "scripts/rank_assets_by_score.py"]),
    ]

    all_steps = bootstrap_steps + steps + [("2.2 Export Photos", None)] + remaining_steps
    if "--from" in sys.argv:
        from_index = int(sys.argv[sys.argv.index("--from") + 1])
    else:
        print("\nAvailable steps:")
        for idx, (label, _) in enumerate(all_steps):
            print(f"{idx}: {label}")
        from_index = int(input("\nEnter start step index: "))

    if "--to" in sys.argv:
        to_index = int(sys.argv[sys.argv.index("--to") + 1])
    else:
        to_index = int(input("Enter end step index (inclusive): ")) + 1

    for i, (label, command) in enumerate(bootstrap_steps):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(label, command, dry_run):
            return

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    month = get_month_batch_added(cursor)
    if not month:
        logger.error("No pending batches found.")
        conn.close()
        return
    logger.info(f"📦 Batch selected: {month}")

    for i, (label, command) in enumerate(steps, start=len(bootstrap_steps)):
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(label, command, dry_run, month):
            conn.close()
            return

    # Step 2.2: Export Photos via AppleScript
    label = "2.2 Export Photos"
    i = len(bootstrap_steps) + len(steps)
    if i < from_index or (to_index is not None and i >= to_index):
        pass
    else:
        if is_applescript_available():
            command = ["osascript", "scripts/export_photos_applescript.scpt", month]
            if run_step(label, command, dry_run, month):
                set_batch_status(month, label.split()[0])
            else:
                conn.close()
                return
        else:
            logger.warning(f"🚫 Skipped {label} — AppleScript not available on this system.")

    start_index = len(bootstrap_steps) + len(steps) + 1
    for j, (label, command) in enumerate(remaining_steps):
        i = start_index + j
        if i < from_index or (to_index is not None and i >= to_index):
            continue
        if not run_step(label, command, dry_run, month):
            conn.close()
            return

    conn.close()

if __name__ == "__main__":
    main()
