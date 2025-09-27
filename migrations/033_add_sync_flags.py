def run(conn):
    cursor = conn.cursor()

    try:
        # Ensure db_updates table exists (id is autoincrement, raw_synced and derived_synced are booleans default 0)
        cursor.execute("""
            ALTER TABLE db_updates
            ADD COLUMN 
                raw_synced BOOLEAN NOT NULL DEFAULT 0
        """)
        conn.commit()
        cursor.execute("""
            ALTER TABLE db_updates
            ADD COLUMN 
                derived_synced BOOLEAN NOT NULL DEFAULT 0
        """)
        conn.commit()
        print("✅ Inserted row into db_updates with raw_synced=0, derived_synced=0")
    except Exception as e:
        print(f"⚠️ Failed to create db_updates table: {e}")
        raise

    