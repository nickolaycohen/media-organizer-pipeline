import sqlite3
from scripts.constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH

def set_batch_status(month, status_code):
    """Update the batch status in the database for the given month."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE month_batches SET status = ?, updated_at_utc = datetime('now') WHERE month = ?",
            (status_code, month),
        )
        conn.commit()
        conn.close()
        print(f"[DB] ✅ Updated batch {month} to status {status_code}")
    except Exception as e:
        print(f"[DB] ❌ Failed to update status for {month}: {e}")