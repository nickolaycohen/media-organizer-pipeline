

def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_status (
            code, short_label, full_description, pipeline_stage, script_name
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        '000',
        'added',
        'Batch initialized and added to DB',
        '1.2',
        'generate_month_batches.py'
    ))

    conn.commit()
    print("✅ Inserted initial '000' batch status into batch_status table.")