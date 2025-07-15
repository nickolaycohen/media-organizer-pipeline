def run(conn):
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(planned_execution);")
    columns = [row[1] for row in cursor.fetchall()]
    if 'active' not in columns:
        cursor.execute("""
            -- Migration script to add `active` column to `planned_execution` table
            ALTER TABLE planned_execution ADD COLUMN active INTEGER NOT NULL DEFAULT 0;
        """)

    conn.commit()
