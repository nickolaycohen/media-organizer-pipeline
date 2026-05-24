def run(conn):
    cursor = conn.cursor()

    try:
        # Add MomentsAlbumName column to the assets table
        cursor.execute("PRAGMA table_info(assets)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "MomentsAlbumName" not in columns:
            cursor.execute("ALTER TABLE assets ADD COLUMN MomentsAlbumName TEXT")
            print("✅ Added 'MomentsAlbumName' column to assets table")
        else:
            print("ℹ️ 'MomentsAlbumName' column already exists in assets table")
            
        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 038 failed: {e}")
        raise