def run(conn):
    cursor = conn.cursor()

    # Check if the old column 'filename' exists
    cursor.execute("PRAGMA table_info(schema_migrations);")
    columns = {row[1] for row in cursor.fetchall()}
    if "filename" not in columns:
        print("✅ 'filename' column already removed.")
        return

    print("🔁 Migrating from 'filename' to 'migration'...")

    # Step 1: Add 'migration' column if missing
    if "migration" not in columns:
        cursor.execute("ALTER TABLE schema_migrations ADD COLUMN migration TEXT;")

    # Step 2: Copy values
    cursor.execute("UPDATE schema_migrations SET migration = filename WHERE migration IS NULL;")

    # Step 3: Rebuild the table without 'filename'
    cursor.execute("ALTER TABLE schema_migrations RENAME TO schema_migrations_old;")
    cursor.execute("""
        CREATE TABLE schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration TEXT NOT NULL,
            applied_at_utc TEXT,
            status TEXT DEFAULT 'applied',
            description TEXT DEFAULT NULL
        );
    """)
    cursor.execute("""
        INSERT INTO schema_migrations (id, migration, applied_at_utc, status, description)
        SELECT id, migration, applied_at_utc, status, description
        FROM schema_migrations_old;
    """)
    cursor.execute("DROP TABLE schema_migrations_old;")

    conn.commit()
    print("✅ Migrated 'filename' to 'migration' and dropped old column.")