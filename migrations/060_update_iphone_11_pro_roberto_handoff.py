def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 060: Updating iPhone 11 Pro handoff from Niki to Roberto (2021-05-08 -> 2021-05-09)...")
    cursor.execute("""
        UPDATE device_owners
        SET start_date = '2019-11-19',
            end_date = '2021-05-08',
            updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 11 Pro' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners
        SET start_date = '2021-05-09',
            end_date = '2024-06-21',
            notes = 'Handed down from Niki on 2021-05-09',
            updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 11 Pro' AND owner_name = 'Roberto'
    """)
    conn.commit()
    print("✅ iPhone 11 Pro ownership periods updated for Niki and Roberto.")

