def run(conn):
    cursor = conn.cursor()

    try:
        # Add apple_photos_monthly_selection column to the assets table
        cursor.execute("PRAGMA table_info(assets)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "apple_photos_monthly_selection" not in columns:
            cursor.execute("ALTER TABLE assets ADD COLUMN apple_photos_monthly_selection INTEGER DEFAULT 0")
            print("✅ Added 'apple_photos_monthly_selection' column to assets table")
        else:
            print("ℹ️ 'apple_photos_monthly_selection' column already exists in assets table")
            
        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 040 failed: {e}")
        raise