import sqlite3
from scripts.constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH

def set_batch_status(cursor, month, current_code, success=True):
    """Update the batch status for the given month based on the outcome of a step."""
    try:
        lookup_field = "code" if success else "error_code"
        cursor.execute(f"""
            SELECT {lookup_field}
            FROM batch_status
            WHERE code = ?
        """, (current_code,))
        result = cursor.fetchone()
        if not result or not result[0]:
            raise ValueError(f"No {'success' if success else 'error'} transition found for code: {current_code}")
        next_code = result[0]

        cursor.execute(
            "UPDATE month_batches SET status_code = ?, updated_at_utc = datetime('now') WHERE month = ?",
            (next_code, month),
        )
        print(f"[DB] ✅ Updated batch {month} to status {next_code}")
    except Exception as e:
        print(f"[DB] ❌ Failed to update status for {month}: {e}")