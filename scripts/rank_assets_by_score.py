

import os
import sqlite3
from utils.logger import setup_logger
from constants import DB_PATH, CURATED_EXPORT_DIR
from db.queries import get_next_batch

from shutil import copy2

MODULE_TAG = "rank_assets"
logger = setup_logger(MODULE_TAG)

def export_ranked_assets(threshold_score=0.6):
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get the current batch
    next_batch = get_next_batch(cursor)
    if not next_batch:
        logger.warning("No pending batch found.")
        return

    month = next_batch["month"]
    logger.info(f"📊 Ranking assets for batch: {month}")

    # Query from view
    query = """
        SELECT original_filename, score_normalized
        FROM ranked_assets_view
        WHERE month = ? AND score_normalized >= ?
        ORDER BY score_normalized DESC;
    """

    cursor.execute(query, (month, threshold_score))
    ranked_assets = cursor.fetchall()
    conn.close()

    if not ranked_assets:
        logger.warning("No assets met the threshold score.")
        return

    # Prepare source and destination folders
    export_path = os.path.join(CURATED_EXPORT_DIR, month)
    os.makedirs(export_path, exist_ok=True)

    source_root = os.path.join(CURATED_EXPORT_DIR, month, "all_exports")
    try:
        os.makedirs(source_root, exist_ok=True)
    except Exception as e:
        logger.error(f"Unable to create or access source export folder: {source_root}. Error: {e}")
        return

    logger.info(f"Exporting {len(ranked_assets)} assets to {export_path}")

    exported_count = 0
    for filename, score in ranked_assets:
        source_file = os.path.join(source_root, filename)
        dest_file = os.path.join(export_path, filename)
        if os.path.exists(source_file):
            copy2(source_file, dest_file)
            exported_count += 1
        else:
            logger.warning(f"File missing: {source_file}")

    logger.info(f"✅ Exported {exported_count} curated assets.")

if __name__ == "__main__":
    export_ranked_assets()