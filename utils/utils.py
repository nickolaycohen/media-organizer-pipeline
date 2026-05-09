import sqlite3

def human_readable_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    p = 1024
    while size_bytes >= p and i < len(size_name)-1:
        size_bytes /= p
        i += 1
    return f"{size_bytes:.2f}{size_name[i]}"

def set_batch_status(cursor, month, current_code, success=True, session_id=None):
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

        # Also log this update in the pipeline_executions table
        cursor.execute(
            "SELECT id FROM month_batches WHERE month = ?",
            (month,)
        )
        batch_row = cursor.fetchone()
        if batch_row:
            batch_month_id = batch_row[0]
            cursor.execute(
                "INSERT INTO pipeline_executions (label, status, batch_month_id, session_id) VALUES (?, 'success', ?, ?)",
                (next_code, batch_month_id, session_id)
            )
    except Exception as e:
        print(f"[DB] ❌ Failed to update status for {month}: {e}")

def get_full_transition_path(transitions, current_status):
    path = []
    status = current_status
    while True:
        next_steps = [code for code, prev, *_ in transitions if prev == status]
        if not next_steps:
            break
        next_code = next_steps[0]
        path.append(f"{status}->{next_code}")
        status = next_code
    return path
