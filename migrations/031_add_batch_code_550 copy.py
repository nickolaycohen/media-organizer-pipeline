def run(conn):
    cursor = conn.cursor()

    # Check if code 550 already exists
    cursor.execute("SELECT 1 FROM batch_status WHERE code='550'")
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO batch_status (
                code, short_label, full_description, pipeline_stage, script_name, preceding_code, transition_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            '550',
            'pull_google_favorites',
            'Pull Google Photos Favorites and update asset flags',
            '3.2',
            'pull_google_favorites.py',
            '500',
            'pipeline'
        ))
        conn.commit()
        print("✅ Inserted '550' batch status (pull_google_favorites.py).")
    else:
        print("ℹ️ batch_status code 550 already exists, skipping insertion.")