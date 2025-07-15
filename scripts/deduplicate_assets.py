import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import sqlite3
import logging
from collections import defaultdict
from pathlib import Path
from constants import STAGING_ROOT as STAGING_ROOT_STR, MEDIA_ORGANIZER_DB_PATH, LOG_PATH
from pathlib import Path
STAGING_ROOT = Path(STAGING_ROOT_STR)
#
# from utils.utils import set_batch_status
from uuid import uuid4
session_id = str(uuid4())
from utils.logger import setup_logger


MODULE_TAG = "deduplicate_assets"

# Constants
EXTENSION_PRIORITY = ["heic", "jpg", "jpeg", "png", "mp4", "mov"]

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger("deduplicate_assets")
logger = setup_logger(LOG_PATH, MODULE_TAG, extra_fields={"session_id": session_id})

def get_priority(ext):
    try:
        return EXTENSION_PRIORITY.index(ext.lower())
    except ValueError:
        return len(EXTENSION_PRIORITY)

def collect_files(base_dir):
    file_groups = defaultdict(list)
    for root, _, files in os.walk(base_dir):
        for fname in files:
            path = Path(root) / fname
            stem = path.stem
            key = (path.parent, stem)
            file_groups[key].append(path)
    return file_groups

def deduplicate_files(file_groups):
    kept_files = set()
    removed_files = []

    for (folder, base), paths in file_groups.items():
        if len(paths) == 1:
            kept_files.add(paths[0])
            continue

        sorted_files = sorted(paths, key=lambda p: (get_priority(p.suffix[1:]), -p.stat().st_size))
        kept = sorted_files[0]
        kept_files.add(kept)

        for p in sorted_files[1:]:
            try:
                p.unlink()
                removed_files.append(p)
                logger.info(f"🗑️ Removed {p} (preferring {kept.name})")
            except Exception as e:
                logger.warning(f"⚠️ Could not remove {p}: {e}")

    return kept_files, removed_files

def update_batch_asset_count(staging_path, retained_count):
    month = Path(staging_path).name
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    conn.execute("PRAGMA busy_timeout = 5000")
    cursor = conn.cursor()
    cursor.execute("UPDATE month_batches SET assets_count = ? WHERE month = ?", (retained_count, month))
    conn.commit()
    conn.close()
    logger.info(f"✅ Updated asset count for {month} to {retained_count}")


def main():
    batch_month = sys.argv[1] if len(sys.argv) > 1 else None
    if not batch_month:
        logger.error("❌ Month parameter is required.")
        sys.exit(1)

    staging_folder = STAGING_ROOT / batch_month
    if not staging_folder.exists():
        logger.warning(f"⚠️ Staging folder does not exist: {staging_folder}")
        return

    logger.info(f"🔍 Deduplicating assets for batch: {batch_month} at {staging_folder}")
    file_groups = collect_files(staging_folder)
    kept, removed = deduplicate_files(file_groups)
    update_batch_asset_count(staging_folder, len(kept))
    logger.info(f"✅ Deduplication complete for {batch_month}.")

if __name__ == "__main__":
    main()