def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 052: Synchronizing device owners and date ranges with Apple Photos Sources albums...")

    # 1. Register new owners
    new_owners = ['Sid Father', 'Michelle', 'Ilian Stojanov', 'Goro', 'Doris']
    for owner in new_owners:
        cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES (?)", (owner,))

    # 2. Update date intervals for existing Benny devices
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2013-01-01', end_date = '2015-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'SCH-I535' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2015-01-01', end_date = '2016-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'SCH-I435' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2013-01-01', end_date = '2017-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPad' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2015-01-01', end_date = '2020-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPad mini 2' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2016-01-01', end_date = '2017-07-03', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 6s' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2017-07-04', end_date = '2021-05-08', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 7' AND owner_name = 'Benny'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2020-01-01', end_date = '2021-05-08', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone SE (2nd generation)' AND owner_name = 'Benny'
    """)

    # 3. Update date intervals for existing Cohens devices
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2004-01-01', end_date = '2008-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'DSC-T1' AND owner_name = 'Cohens'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2004-01-01', end_date = '2005-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'KODAK DX6490 ZOOM DIGITAL CAMERA' AND owner_name = 'Cohens'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2008-01-01', end_date = '2012-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'DSC-T300' AND owner_name = 'Cohens'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2012-01-01', end_date = '2018-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'NIKON 1 J1' AND owner_name = 'Cohens'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2024-01-01', end_date = '2025-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'Canon EOS Rebel T7' AND owner_name = 'Cohens'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2007-01-01', end_date = '2015-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'DSC-H2' AND owner_name = 'Cohens Bulgaria'
    """)

    # 4. Update date intervals for existing Niki devices
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2010-01-01', end_date = '2011-06-30', updated_at_utc = datetime('now')
        WHERE camera_model = 'DROIDX 225a0001ffd800000a3a7de10201a02c' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2011-04-30', end_date = '2013-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'DROIDX' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2013-01-01', end_date = '2014-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'SCH-I545' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2015-01-01', end_date = '2016-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'SM-G920V' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2014-01-01', end_date = '2017-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPad Air' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2019-01-01', end_date = '2021-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 11 Pro' AND owner_name = 'Niki'
    """)
    cursor.execute("""
        UPDATE device_owners 
        SET start_date = '2021-05-08', end_date = '2024-12-31', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 12 Pro Max' AND owner_name = 'Niki'
    """)

    # 5. Add multi-period hand-down: iPhone 11 Pro to Roberto in 2024
    cursor.execute("""
        SELECT COUNT(*) FROM device_owners 
        WHERE camera_model = 'iPhone 11 Pro' AND owner_name = 'Roberto'
    """)
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, notes)
            VALUES ('iPhone 11 Pro', 'Roberto', '2024-01-01', '2024-12-31', 'Handed down from Niki')
        """)

    # 6. Reassign iPhone 8 Plus to Goro
    cursor.execute("""
        UPDATE device_owners 
        SET owner_name = 'Goro', updated_at_utc = datetime('now')
        WHERE camera_model = 'iPhone 8 Plus'
    """)

    # 7. Add new camera models from the album structure
    new_devices = [
        ('Canon EOS 5D Mark IV', 'Sid Father', '2019-01-01', '2019-12-31', None),
        ('Canon EOS DIGITAL REBEL', 'Michelle', None, None, None),
        ('EX-S600', 'Ilian Stojanov', None, None, None),
        ('iPhone 8', 'Doris', None, None, None),
    ]
    for model, owner, s_date, e_date, notes in new_devices:
        cursor.execute("SELECT COUNT(*) FROM device_owners WHERE camera_model = ? AND owner_name = ?", (model, owner))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (model, owner, s_date, e_date, notes))

    conn.commit()
    print("✅ Migration 052 complete: Device owners and date intervals successfully synchronized.")

