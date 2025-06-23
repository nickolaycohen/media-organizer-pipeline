def run(conn):
    cursor = conn.cursor()
    # cursor.execute("ALTER TABLE month_batches ADD COLUMN status_code TEXT;")
    # cursor.execute("ALTER TABLE month_batches ADD COLUMN status_label TEXT;")
    print("✅ Migration 001 applied")

def rollback(conn):
    # SQLite doesn't support DROP COLUMN directly.
    # This is a no-op or requires table recreation.
    print("⚠️ Manual rollback required for SQLite column removal")