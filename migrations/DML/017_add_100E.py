

def run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name, preceding_code
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        '100E',
        'verify_failed',
        'Smart Album verification failed',
        '2.1',
        'verify_export_album.py',
        '000'
    ))

    conn.commit()
    print("✅ Inserted '100E' error state for Smart Album verification failure into batch_status table.")