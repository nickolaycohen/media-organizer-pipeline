def run(conn):
    cursor = conn.cursor()

    # Create DB Updates table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS db_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        update_type TEXT NOT NULL, -- e.g. "copy_all_media_db"
        updated_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
        notes TEXT
    );
    """)

    conn.commit()
    print("✅ Added db updates table ... ")