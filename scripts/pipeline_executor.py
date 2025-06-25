import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import subprocess
import platform
import shutil
import logging
from db.queries import get_next_batch
from utils.utils import set_batch_status
from utils.logger import setup_logger
from constants import LOG_PATH

MODULE_TAG = "run_pipeline"
logger = setup_logger(LOG_PATH, MODULE_TAG)


def run_step(label, command, dry_run=False, month=None):
    logger.info(f"▶️ Starting: {label}")
    if dry_run:
        logger.info(f"[Dry Run] Would run: {' '.join(command)}")
        return True
    try:
        subprocess.run(command, check=True)
        logger.info(f"✅ Completed: {label}")
        if month is not None:
            set_batch_status(month, label.split()[0])
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed: {label} with error: {e}")
        return False

def is_applescript_available():
    return platform.system() == "Darwin" and shutil.which("osascript") is not None

def main():
    dry_run = "--dry-run" in sys.argv
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Bootstrap: run initial steps before determining which batch to process
    bootstrap_steps = [
        ("0.1 Storage Status", ["python3", os.path.join(SCRIPT_DIR, "storage_status.py"), "--migrate"]),
        ("0.3 Sync Metadata from Photos DB", ["python3", os.path.join(SCRIPT_DIR, "sync_photos_metadata.py")]),
        ("1.1 Detect Gaps", ["python3", os.path.join(SCRIPT_DIR, "generate_month_batches.py")]),
    ]

    for label, command in bootstrap_steps:
        if not run_step(label, command, dry_run):
            return

    month = get_next_batch()
    if not month:
        logger.error("No pending batches found.")
        return
    logger.info(f"📦 Batch selected: {month}")

    steps = [
        ("2.1 Verify Smart Album", ["python3", os.path.join(SCRIPT_DIR, "verify_export_album.py")]),
    ]

    for label, command in steps:
        if not run_step(label, command, dry_run, month):
            return

    # Step 2.2: Export Photos via AppleScript
    label = "2.2 Export Photos"
    if is_applescript_available():
        command = ["osascript", "scripts/export_photos_applescript.scpt", month]
        if run_step(label, command, dry_run, month):
            set_batch_status(month, label.split()[0])
        else:
            return
    else:
        logger.warning(f"🚫 Skipped {label} — AppleScript not available on this system.")

    remaining_steps = [
        ("2.3 Verify Staging", ["python3", "scripts/verify_staging.py"]),
        ("2.3.5 Sync Photo Metadata", ["python3", "scripts/sync_photos_assets.py"]),
        ("2.4 Upload to Google Photos", ["python3", "scripts/upload_to_google_photos.py"]),
        ("3.2.5 Pull Google Favorites", ["python3", "scripts/pull_google_favorites.py"]),
        ("3.3 Rank Assets by Score", ["python3", "scripts/rank_assets_by_score.py"]),
    ]

    for label, command in remaining_steps:
        if not run_step(label, command, dry_run, month):
            return

if __name__ == "__main__":
    main()
