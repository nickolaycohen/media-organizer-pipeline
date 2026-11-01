def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 054: Adding Lee and new sources from Apple Photos (SGH-A817, M753, W580i, SCH-I800, iPad mini, M341)...")

    # 1. Ensure Lee exists in owners
    cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES ('Lee')")

    # 2. Devices to add to device_owners
    new_devices = [
        # Lee
        ('KODAK EASYSHARE M341 Digital Camera', 'Lee', None, None, 'Lee Kodak EasyShare M341 (July 3, 2010)'),
        ('KODAK EASYSHARE M341', 'Lee', None, None, 'Alias for KODAK EASYSHARE M341 Digital Camera'),

        # Benny
        ('SGH-A817', 'Benny', None, None, 'Samsung SGH-A817'),
        ('Samsung SGH-A817', 'Benny', None, None, 'Alias for SGH-A817'),
        ('SCH-I800', 'Benny', None, None, 'Samsung Galaxy Tab SCH-I800'),
        ('Samsung SCH-I800', 'Benny', None, None, 'Alias for SCH-I800'),
        ('iPad mini', 'Benny', None, None, 'iPad mini (1st generation)'),

        # Cohens
        ('KODAK EASYSHARE M753 ZOOM DIGITAL CAMERA', 'Cohens', None, None, 'Kodak EasyShare M753'),
        ('KODAK EASYSHARE M753', 'Cohens', None, None, 'Alias for KODAK EASYSHARE M753 ZOOM DIGITAL CAMERA'),

        # Niki
        ('W580i', 'Niki', None, None, 'Sony Ericsson W580i'),
        ('Sony Ericsson W580i', 'Niki', None, None, 'Alias for W580i'),
        ('HDR-AS30V', 'Niki', None, None, 'Sony Action Cam HDR-AS30V'),
        ('Sony Action Cam HDR-AS30V', 'Niki', None, None, 'Alias for HDR-AS30V'),
        ('iPhone 4', 'Niki', None, None, None),
    ]

    added_count = 0
    for model, owner, s_date, e_date, notes in new_devices:
        cursor.execute("SELECT COUNT(*) FROM device_owners WHERE camera_model = ? AND owner_name = ?", (model, owner))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (model, owner, s_date, e_date, notes))
            added_count += 1

    conn.commit()
    print(f"✅ Migration 054 complete: {added_count} device mappings added to device_owners.")

