

def run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        ALTER TABLE batch_status
        ADD COLUMN preceding_code TEXT
    """)
    conn.commit()
    print("✅ Added 'preceding_code' column to batch_status table.")