def run(conn):
    cursor = conn.cursor()

    try:
        # Add sequencing_confirmed column if missing
        cursor.execute("PRAGMA table_info(imports)")
        columns = [row[1] for row in cursor.fetchall()]
        if "sequencing_confirmed" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN sequencing_confirmed INTEGER DEFAULT 0")

        conn.commit()
        print("✅ Migration 038 applied: sequencing_confirmed added to imports.")

    except Exception as e:
            print(f"⚠️ Migration 038 failed: {e}")
            raise