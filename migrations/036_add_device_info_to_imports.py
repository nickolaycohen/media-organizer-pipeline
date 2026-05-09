def run(conn):
    cursor = conn.cursor()

    try:
        # Add columns to the imports table
        cursor.execute("PRAGMA table_info(imports)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "camera_make" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN camera_make TEXT")
        if "camera_model" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN camera_model TEXT")
            
        conn.commit()
        print("✅ Added 'camera_make' and 'camera_model' columns to imports table")
    except Exception as e:
        print(f"⚠️ Migration 036 failed: {e}")
        raise