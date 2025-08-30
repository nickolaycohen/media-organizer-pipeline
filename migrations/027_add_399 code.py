def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO batch_status (code, pipeline_stage, short_label, full_description, script_name)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "399",                     # partial upload / quota-constrained
        "4",                       # stage 4 (Upload)
        "Partial Upload",          # short label
        "Partial upload to Google Photos due to insufficient space",  # full description
        "upload_to_google_photos.py {month}"  # script reference
    ))

    conn.commit()
    print("✅ Inserted '399' batch status (partial upload) into batch_status table.")