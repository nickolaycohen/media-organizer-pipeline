def run(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS device_owners (
                camera_model TEXT PRIMARY KEY,
                owner_name TEXT NOT NULL,
                updated_at_utc TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        print("✅ Migration 045: 'device_owners' table created successfully.")
    except Exception as e:
        print(f"⚠️ Migration 045 failed: {e}")
        raise
