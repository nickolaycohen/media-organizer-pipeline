def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name, preceding_code
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        '210',
        'deduplicate_assets',
        'Remove duplicate assets based on extension and size',
        '2.2.5',
        'deduplicate_assets.py',
        '200'
    ))

    conn.commit()
    print("✅ Inserted '210' batch status (deduplicate_assets) into batch_status table.")