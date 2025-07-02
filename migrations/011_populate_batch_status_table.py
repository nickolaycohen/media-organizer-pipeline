def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        '100',
        'album_verified',
        'Smart Album verified for current month',
        '2.1',
        'verify_export_album.py'
    ))

    conn.commit()
    print("✅ Inserted '100' batch status (album_verified) into batch_status table.")