import sqlite3
import os
from utils.logger import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH, LOG_PATH

MODULE_TAG = "create_ranked_view"
logger = setup_logger(LOG_PATH,MODULE_TAG)

def create_view():
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at: {DB_PATH}")
        return

    view_sql = """
    DROP VIEW IF EXISTS ranked_assets_view;
    CREATE VIEW ranked_assets_view AS
    SELECT
        a.asset_id,
        a.original_filename,
        a.month,
        a.aesthetic_score,
        a.google_favorite,
        a.apple_favorite,
        a.apple_photos_monthly_selection,
        (
            (COALESCE(a.aesthetic_score, 0) * 0.875) + 
            (a.google_favorite * 0.125) + 
            (a.apple_photos_monthly_selection * 0.15)
            + (a.apple_favorite * 0.125)
        ) AS score_normalized,
        a.date_created_utc,
        a.MomentsAlbumName
    FROM
        assets a;
    """

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executescript(view_sql)
        conn.commit()
        conn.close()
        logger.info("✅ ranked_assets_view created successfully.")
    except Exception as e:
        logger.error(f"Failed to create ranked_assets_view: {e}")

if __name__ == "__main__":
    create_view()
