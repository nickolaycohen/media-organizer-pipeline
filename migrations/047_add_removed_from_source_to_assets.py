def run(conn):
    cursor = conn.cursor()

    try:
        cursor.execute("PRAGMA table_info(assets)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "removed_from_source" not in columns:
            cursor.execute("ALTER TABLE assets ADD COLUMN removed_from_source INTEGER DEFAULT 0")
            print("✅ Added 'removed_from_source' column to assets table")
        else:
            print("ℹ️ 'removed_from_source' column already exists in assets table")

        if "removed_from_source_at_utc" not in columns:
            cursor.execute("ALTER TABLE assets ADD COLUMN removed_from_source_at_utc TEXT")
            print("✅ Added 'removed_from_source_at_utc' column to assets table")
        else:
            print("ℹ️ 'removed_from_source_at_utc' column already exists in assets table")
            
        conn.commit()
        print("✅ Migration 047: 'removed_from_source' tracking columns added successfully.")
    except Exception as e:
        print(f"⚠️ Migration 047 failed: {e}")
        conn.rollback()
        raise
