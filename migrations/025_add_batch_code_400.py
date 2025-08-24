def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name, preceding_code
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        '400',
        'uploaded_to_google',
        'Upload to Google Account from Staging Folder',
        '2.4',
        'upload_to_google_photos.py',
        '210'
    ))

    conn.commit()
    print("✅ Inserted '400' batch status (uploaded_to_google) into batch_status table.")