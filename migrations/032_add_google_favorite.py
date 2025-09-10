def run(conn):
    cursor = conn.cursor()

    try:
        # Add google_favorite column if it does not exist
        cursor.execute("""
            ALTER TABLE assets
            ADD COLUMN google_favorite INTEGER DEFAULT 0
        """)
        conn.commit()
        print("✅ Added column 'google_favorite' to assets table with default 0")
    except Exception as e:
        print(f"⚠️ Failed to add column 'google_favorite': {e}")
        raise