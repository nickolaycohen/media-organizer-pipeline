def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 055: Adding Nickolay Vassilev and DMC-FZ40...")

    # 1. Ensure Nickolay Vassilev exists in owners
    cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES ('Nickolay Vassilev')")

    # 2. Add DMC-FZ40 to device_owners
    new_devices = [
        ('DMC-FZ40', 'Nickolay Vassilev', '2011-05-01', '2012-01-31', 'Panasonic Lumix DMC-FZ40 - Nickolay Vassilev (ex-husband of Baba Eli)'),
        ('Panasonic DMC-FZ40', 'Nickolay Vassilev', '2011-05-01', '2012-01-31', 'Alias for DMC-FZ40'),
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
    print(f"✅ Migration 055 complete: {added_count} device mappings added to device_owners.")

