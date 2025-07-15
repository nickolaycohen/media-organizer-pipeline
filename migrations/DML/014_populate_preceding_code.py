


def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE batch_status
        SET preceding_code = '100'
        WHERE code = '200'
    """)

    conn.commit()
    print("✅ Set preceding_code='100' for code='200' in batch_status.")