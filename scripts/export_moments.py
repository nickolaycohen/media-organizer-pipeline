import os
import sys
import sqlite3
import shutil
from pathlib import Path

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH, MOMENTS_EXPORT_DIR, STAGING_ROOT, LOG_PATH

MODULE_TAG = "export_moments"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def main():
    logger.info("📂 Starting Moments Export process...")
    
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    # Query from view, sorted by total score descending
    # MomentName is mapped to MomentsAlbumName in the view
    query = """
        SELECT v.original_filename, v.month, v.MomentsAlbumName, v.score_normalized
        FROM ranked_assets_view v
        JOIN month_batches mb ON v.month = mb.month
        WHERE mb.status_code >= '600'
        ORDER BY v.score_normalized DESC;
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    # Find the cutoff threshold: the score of the first asset with a NULL Moment
    threshold_score = None
    for filename, month, moment_name, score in rows:
        if not moment_name:
            threshold_score = score
            logger.info(f"🛑 Found first asset without a Moment: {filename} (Score: {score:.4f}). Cutoff threshold established.")
            break

    if threshold_score is None:
        logger.warning("No assets with NULL Moments found to establish a cutoff threshold.")
        return

    # Export assets that have a score strictly higher than the threshold asset and have a valid Moment name
    export_list = [r for r in rows if r[3] > threshold_score and r[2]]

    if not export_list:
        logger.warning(f"No assets with Moments found with a score higher than {threshold_score:.4f}.")
        return

    logger.info(f"✨ Found {len(export_list)} assets categorized into Moments to export.")
    
    # Format: <MomentName/file> under the MomentsExport directory
    # Moved one level up from CURATED_EXPORT_DIR (02-AICurrateList)
    export_root = MOMENTS_EXPORT_DIR
    os.makedirs(export_root, exist_ok=True)

    success_count = 0
    for filename, month, moment_name, score in export_list:
        # Sanitize moment name for directory naming (remove illegal characters)
        safe_moment_name = "".join([c for c in moment_name if c.isalnum() or c in (' ', '-', '_')]).strip()
        
        source_file = os.path.join(STAGING_ROOT, month, filename)
        dest_dir = os.path.join(export_root, safe_moment_name)
        dest_file = os.path.join(dest_dir, filename)

        if not os.path.exists(source_file):
            logger.warning(f"  Missing source: {source_file} (Month: {month}, Score: {score:.4f})")
            continue

        os.makedirs(dest_dir, exist_ok=True)
        try:
            shutil.copy2(source_file, dest_file)
            success_count += 1
            logger.info(f"  [Copy] {safe_moment_name}/{filename} (Score: {score:.4f})")
        except Exception as e:
            logger.error(f"  ❌ Error copying {filename}: {e}")

    logger.info(f"✅ Moments Export finished. Successfully exported {success_count} files to {export_root}")

if __name__ == "__main__":
    main()