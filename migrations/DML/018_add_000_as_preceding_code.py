def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE batch_status
        SET preceding_code = '000'
        WHERE code = '100'
    """)

    conn.commit()
    print("✅ Set preceding_code='000' for code='100' in batch_status table.")
