import os
import sys
import sqlite3
from shutil import copy2

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH, CURATED_EXPORT_DIR, STAGING_ROOT, LOG_PATH

MODULE_TAG = "rank_assets"
logger = setup_logger(LOG_PATH, MODULE_TAG)

def export_ranked_assets(month, threshold_score=0.6):
    if not os.path.exists(MEDIA_ORGANIZER_DB_PATH):
        logger.error(f"Database not found at {MEDIA_ORGANIZER_DB_PATH}")
        return

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    logger.info(f"📊 Ranking assets for batch: {month}")

    # Query from view
    query = """
        SELECT original_filename, score_normalized, google_favorite, aesthetic_score, apple_photos_monthly_selection, mobile_apple_photos_featured_photos
        FROM ranked_assets_view
        WHERE month = ? AND (score_normalized >= ? OR google_favorite = 1 OR apple_photos_monthly_selection = 1 OR mobile_apple_photos_featured_photos = 1)
        ORDER BY score_normalized DESC;
    """

    cursor.execute(query, (month, threshold_score))
    ranked_assets = cursor.fetchall()
    conn.close()

    if not ranked_assets:
        logger.warning("No assets met the threshold score.")
        return

    # Prepare source and destination folders
    fav_count = sum(1 for row in ranked_assets if row[2] or row[4] or row[5])
    high_score_count = len(ranked_assets) - fav_count
    
    export_path = os.path.join(CURATED_EXPORT_DIR, month)
    os.makedirs(export_path, exist_ok=True)

    # Files were exported to STAGING_ROOT/{month} in step 200
    source_root = os.path.join(STAGING_ROOT, month)
    if not os.path.exists(source_root):
        logger.error(f"Source staging folder missing: {source_root}")
        return

    logger.info(f"Found {len(ranked_assets)} total assets ({fav_count} favorites, {high_score_count} high-score).")
    logger.info(f"🚀 Exporting curated assets to {export_path}...")

    exported_count = 0
    for filename, score, is_fav, apple_score, is_sel, is_feat in ranked_assets:
        fav_status = "Yes" if is_fav else "No"
        sel_status = "Yes" if is_sel else "No"
        feat_status = "Yes" if is_feat else "No"
        logger.info(f"  - {filename:40} | Total Score: {score:.4f} (Apple Score: {apple_score or 0.0:.4f}, Google Fav: {fav_status}, Monthly Sel: {sel_status}, Featured Feat: {feat_status})")

        source_file = os.path.join(source_root, filename)
        dest_file = os.path.join(export_path, filename)
        if os.path.exists(source_file):
            copy2(source_file, dest_file)
            exported_count += 1
        else:
            logger.warning(f"File missing: {source_file}")

    logger.info(f"✅ Exported {exported_count} curated assets.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python3 rank_assets_by_score.py <YYYY-MM>")
        sys.exit(1)
    
    target_month = sys.argv[1]
    export_ranked_assets(target_month)