def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO batch_status (code, pipeline_stage, short_label, full_description, script_name)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "400E",          # error code
        "4",             # stage 4 (Upload)
        "Upload Error",  # short label
        "Upload to Google Photos failed",  # full description
        "upload_to_google_photos.py"       # script reference
    ))

    conn.commit()
    print("✅ Inserted '400E' batch status (uploaded_to_google) into batch_status table.")