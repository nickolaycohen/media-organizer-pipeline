def run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        ALTER TABLE batch_status
        ADD COLUMN error_code TEXT
    """)
    conn.commit()
    print("✅ Added 'error_code' column to batch_status table.")
