
def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS planned_execution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planned_month TEXT,
            set_at_utc TEXT DEFAULT (datetime('now'))
        );
    """)

    conn.commit()
