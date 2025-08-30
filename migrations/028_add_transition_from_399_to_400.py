def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO batch_status (code, preceding_code, full_description, script_name )
        VALUES (?, ?, ?, ?)
    """, (
        "400",
        "399",
        "Quota available: move from partial upload to full upload",
        "upload_to_google_photos.py {month}"  # script reference

    ))

    conn.commit()
    print("✅ Added transition from '399' to '400' when quota becomes available.")


    