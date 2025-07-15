def run(conn):
    cursor = conn.cursor()

    # Check if the column already exists
    cursor.execute("PRAGMA table_info(month_batches);")
    columns = [row[1] for row in cursor.fetchall()]
    if 'latest_import_id' not in columns:
        cursor.execute("""
            ALTER TABLE month_batches ADD COLUMN latest_import_id INTEGER;
        """)

    cursor.execute("""
        UPDATE month_batches
        SET latest_import_id = (
            SELECT MAX(import_id)
            FROM assets
            WHERE assets.month = month_batches.month
            AND assets.uploaded_to_google = 1
        );
    """)
    conn.commit()