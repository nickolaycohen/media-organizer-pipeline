

def run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name, preceding_code
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        '200E',
        'export_failed',
        'Photo export to staging failed',
        '2.2',
        'export_photos_wrapper.py',
        '100'
    ))

    conn.commit()
    print("✅ Inserted '200E' error state for export failure into batch_status table.")