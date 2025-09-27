def run(conn):
    cursor = conn.cursor()

    try:
        # Add execution_id column if it does not exist
        cursor.execute("PRAGMA table_info(imports)")
        columns = [row[1] for row in cursor.fetchall()]
        if "execution_id" not in columns:
            cursor.execute("""
                ALTER TABLE imports
                ADD COLUMN execution_id TEXT NULL
            """)
            conn.commit()
            print("✅ Added execution_id column to imports table")
        else:
            print("ℹ️ execution_id column already exists in imports table")

    except Exception as e:
        print(f"⚠️ Migration 035 failed: {e}")
        raise