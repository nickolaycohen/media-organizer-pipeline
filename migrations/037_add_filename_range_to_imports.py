def run(conn):
    cursor = conn.cursor()

    try:
        # Add min_filename and max_filename columns to the imports table
        cursor.execute("PRAGMA table_info(imports)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "min_filename" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN min_filename TEXT")
            print("✅ Added 'min_filename' column to imports table")
        else:
            print("ℹ️ 'min_filename' column already exists in imports table")
            
        if "max_filename" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN max_filename TEXT")
            print("✅ Added 'max_filename' column to imports table")
        else:
            print("ℹ️ 'max_filename' column already exists in imports table")
            
        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 037 failed: {e}")
        raise