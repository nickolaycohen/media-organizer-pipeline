def run(conn):
    cursor = conn.cursor()

    try:
        # Check existing columns in month_batches table
        cursor.execute("PRAGMA table_info(month_batches)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "is_bypassed" not in columns:
            cursor.execute("ALTER TABLE month_batches ADD COLUMN is_bypassed INTEGER DEFAULT 0")
            print("✅ Added 'is_bypassed' column to month_batches table")
        else:
            print("ℹ️ 'is_bypassed' column already exists in month_batches table")

        if "bypass_timestamp" not in columns:
            cursor.execute("ALTER TABLE month_batches ADD COLUMN bypass_timestamp TEXT")
            print("✅ Added 'bypass_timestamp' column to month_batches table")
        else:
            print("ℹ️ 'bypass_timestamp' column already exists in month_batches table")
            
        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 044 failed: {e}")
        raise
