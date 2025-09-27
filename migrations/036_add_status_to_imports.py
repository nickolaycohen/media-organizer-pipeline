
def run(conn):
    cursor = conn.cursor()

    try:
        # Add status_code column if missing
        cursor.execute("PRAGMA table_info(imports)")
        columns = [row[1] for row in cursor.fetchall()]
        if "status_code" not in columns:
            cursor.execute("ALTER TABLE imports ADD COLUMN status_code TEXT DEFAULT NULL")

        conn.commit()
        print("✅ Migration 036 applied: status_code added to imports.")

    except Exception as e:
            print(f"⚠️ Migration 036 failed: {e}")
            raise
if __name__ == "__main__":
    migrate()