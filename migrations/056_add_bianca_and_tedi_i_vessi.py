def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 056: Adding Bianca (iPhone 5s) and Tedi i Vessi (Olympus FE-5020)...")

    # 1. Ensure owners exist
    cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES ('Bianca')")
    cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES ('Tedi i Vessi')")

    # 2. Devices to add to device_owners
    new_devices = [
        # Bianca
        ('iPhone 5s', 'Bianca', None, None, 'iPhone 5s - Bianca'),

        # Tedi i Vessi
        ('FE5020,X935            ', 'Tedi i Vessi', None, None, 'Olympus FE-5020 / X-935 (exact EXIF model with padding)'),
        ('FE5020,X935', 'Tedi i Vessi', None, None, 'Olympus FE-5020 / X-935 (trimmed)'),
        ('Olympus FE-5020', 'Tedi i Vessi', None, None, 'Alias for FE5020,X935'),
        ('FE-5020', 'Tedi i Vessi', None, None, 'Alias for FE5020,X935'),
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
    print(f"✅ Migration 056 complete: {added_count} device mappings added to device_owners.")

