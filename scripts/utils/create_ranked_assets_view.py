import sqlite3
import os
from utils import setup_logger
from constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH, LOG_PATH

MODULE_TAG = "create_ranked_view"
logger = setup_logger(LOG_PATH,MODULE_TAG)

def create_view():
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at: {DB_PATH}")
        return

    view_sql = """
    CREATE VIEW IF NOT EXISTS ranked_assets_view AS
    SELECT
        a.id AS asset_id,
        a.original_filename,
        a.month,
        a.aesthetic_score,
        a.google_favorite,
        a.aesthetic_score + (a.google_favorite * 0.125) AS score_normalized
    FROM
        assets a
    WHERE
        a.aesthetic_score IS NOT NULL
    ORDER BY
        score_normalized DESC;
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
