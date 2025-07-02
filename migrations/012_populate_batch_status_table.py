def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        '200',
        'exported',
        'Photos exported to staging',
        '2.2',
        'export_photos_wrapper.py'
    ))

    conn.commit()
    print("✅ Inserted '200' batch status (exported) into batch_status table.")