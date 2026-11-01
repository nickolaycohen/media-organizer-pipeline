def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 053: Adding new sources from Apple Photos (HDR-AS30V, iPhone 4, iPod touch, iPad 5th gen, Samsung L730)...")

    # Ensure Cohen Bulgaria exists in owners
    cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES ('Cohen Bulgaria')")

    new_devices = [
        ('HDR-AS30V', 'Niki', None, None, 'Sony Action Cam HDR-AS30V'),
        ('Sony Action Cam HDR-AS30V', 'Niki', None, None, 'Alias for HDR-AS30V'),
        ('iPhone 4', 'Niki', None, None, None),
        ('iPod touch', 'Benny', None, None, None),
        ('iPad (5th generation)', 'Benny', None, None, None),
        ('<VLUU L730  / Samsung L730>', 'Cohens Bulgaria', None, None, 'Samsung L730'),
        ('Samsung L730', 'Cohens Bulgaria', None, None, 'Alias for <VLUU L730  / Samsung L730>'),
    ]

    for model, owner, s_date, e_date, notes in new_devices:
        cursor.execute("SELECT COUNT(*) FROM device_owners WHERE camera_model = ? AND owner_name = ?", (model, owner))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (model, owner, s_date, e_date, notes))

    conn.commit()
    print("✅ Migration 053 complete: New sources added to device_owners successfully.")

