import os
import sys
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime
from collections import defaultdict
from utils import setup_logger
from constants import STAGING_ROOT, LOG_PATH, MEDIA_ORGANIZER_DB_PATH
import sqlite3
from db.queries import get_next_batch  # Assumes such utility exists

MODULE_TAG = 'verify_staging'


# Setup logger
logger = setup_logger(LOG_PATH, MODULE_TAG)

def verify_batch_folder(batch_path):
    """Verify presence and types of files in the given album folder."""
    if not os.path.exists(batch_path):
        os.makedirs(batch_path)
        logger.info(f"✅ Created missing export folder: {batch_path}")

    files = [f for f in os.listdir(batch_path) if os.path.isfile(os.path.join(batch_path, f))]
    if not files:
        logger.warning(f"No files found in {batch_path}")
        return

    # Group and count files by extension
    ext_count = defaultdict(int)
    for f in files:
        _, ext = os.path.splitext(f)
        ext_count[ext.lower()] += 1

    logger.info(f"File type breakdown for {os.path.basename(batch_path)}:")
    for ext, count in sorted(ext_count.items()):
        logger.info(f"  - {count} file(s) with extension '{ext}'")

    # Optional: Warn on unknown file types
    known_exts = {'.heic', '.jpeg', '.jpg', '.png', '.mov', '.mp4'}
    unknown_exts = set(ext_count.keys()) - known_exts
    if unknown_exts:
        logger.warning(f"⚠️ Unrecognized file types found: {', '.join(unknown_exts)}")

def main():
    logger.info("🔍 Starting staging verification")

    if not os.path.exists(STAGING_ROOT):
        logger.error(f"Staging root not found: {STAGING_ROOT}")
        sys.exit(1)

    # Fetch latest batch from DB with status = 'exported' or equivalent
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()
    batch_month = get_next_batch(cursor)
    conn.close()

    if not batch_month:
        logger.warning("No eligible batch found in database to verify.")
        return

    full_path = os.path.join(STAGING_ROOT, batch_month)
    verify_batch_folder(full_path)

    logger.info("✅ Staging folder verification completed.")

if __name__ == "__main__":
    main()
