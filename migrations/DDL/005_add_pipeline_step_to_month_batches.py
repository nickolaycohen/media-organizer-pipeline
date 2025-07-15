

def run(conn):
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(month_batches);")
    columns = {row[1] for row in cursor.fetchall()}
    if "pipeline_step" not in columns:
        cursor.execute("ALTER TABLE month_batches ADD COLUMN pipeline_step TEXT;")
        print("✅ Added 'pipeline_step' column to month_batches.")
    else:
        print("ℹ️ 'pipeline_step' column already exists in month_batches.")

    conn.commit()