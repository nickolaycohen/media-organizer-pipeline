def run(conn):
    cursor = conn.cursor()

    try:
        # Add mobile_apple_photos_featured_photos column to the assets table
        cursor.execute("PRAGMA table_info(assets)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "mobile_apple_photos_featured_photos" not in columns:
            cursor.execute("ALTER TABLE assets ADD COLUMN mobile_apple_photos_featured_photos INTEGER DEFAULT 0")
            print("✅ Added 'mobile_apple_photos_featured_photos' column to assets table")
        else:
            print("ℹ️ 'mobile_apple_photos_featured_photos' column already exists in assets table")
            
        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 043 failed: {e}")
        raise
