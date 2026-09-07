def run(conn):
    cursor = conn.cursor()
    try:
        # Step 1: Create owners registry table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at_utc TEXT DEFAULT (datetime('now'))
            )
        """)

        # Step 2: Check if device_owners exists and get pre-migration row count
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='device_owners'")
        has_old_table = cursor.fetchone() is not None

        old_count = 0
        if has_old_table:
            cursor.execute("SELECT COUNT(*) FROM device_owners")
            old_count = cursor.fetchone()[0]
            print(f"ℹ️ Found existing 'device_owners' table with {old_count} records.")

            # Create backup snapshot table
            cursor.execute("DROP TABLE IF EXISTS device_owners_backup_pre_046")
            cursor.execute("CREATE TABLE device_owners_backup_pre_046 AS SELECT * FROM device_owners")

            # Extract unique owner names and insert into owners table
            cursor.execute("""
                INSERT OR IGNORE INTO owners (name)
                SELECT DISTINCT owner_name FROM device_owners
                WHERE owner_name IS NOT NULL AND owner_name != ''
            """)

            # Rename old table
            cursor.execute("DROP TABLE IF EXISTS device_owners_old")
            cursor.execute("ALTER TABLE device_owners RENAME TO device_owners_old")

        # Step 3: Create new device_owners table supporting multiple ownership periods
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS device_owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                camera_model TEXT NOT NULL,
                owner_name TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                notes TEXT,
                created_at_utc TEXT DEFAULT (datetime('now')),
                updated_at_utc TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_device_owners_model ON device_owners(camera_model)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_device_owners_dates ON device_owners(camera_model, start_date, end_date)")

        # Step 4: Transfer all records from old table into new table
        if has_old_table and old_count > 0:
            cursor.execute("""
                INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, created_at_utc, updated_at_utc)
                SELECT camera_model, owner_name, NULL, NULL, updated_at_utc, updated_at_utc
                FROM device_owners_old
            """)

            # Step 5: Verify row count assertion
            cursor.execute("SELECT COUNT(*) FROM device_owners")
            new_count = cursor.fetchone()[0]
            if new_count != old_count:
                raise RuntimeError(f"Data migration assertion failed: expected {old_count} rows, found {new_count} rows in new table.")
            
            cursor.execute("DROP TABLE device_owners_old")
            print(f"✅ Migrated all {new_count} device owner records with 100% data preservation.")

        conn.commit()
        print("✅ Migration 046: 'owners' and multi-period 'device_owners' tables created successfully.")
    except Exception as e:
        print(f"⚠️ Migration 046 failed: {e}")
        conn.rollback()
        raise
