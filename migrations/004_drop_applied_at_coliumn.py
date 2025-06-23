def run(conn):
    cursor = conn.cursor()

    # Check if the old column exists
    cursor.execute("PRAGMA table_info(schema_migrations);")
    columns = [row[1] for row in cursor.fetchall()]
    if "applied_at" not in columns:
        print("✅ Column 'applied_at' already removed.")
        return

    # Rename old table
    cursor.execute("ALTER TABLE schema_migrations RENAME TO schema_migrations_old;")

    # Recreate the table without 'applied_at', preserving all other columns
    cursor.execute("""
        CREATE TABLE schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration TEXT NOT NULL UNIQUE,
            applied_at_utc TEXT,
            status TEXT DEFAULT 'applied',
            description TEXT DEFAULT NULL
        );
    """)

    # Copy all columns except 'applied_at'
    cursor.execute("""
        INSERT INTO schema_migrations (id, migration, applied_at_utc, status, description)
        SELECT id, migration, applied_at_utc, status, description
        FROM schema_migrations_old;
    """)

    # Drop old table
    cursor.execute("DROP TABLE schema_migrations_old;")

    conn.commit()
    print("✅ Column 'applied_at' successfully removed from schema_migrations.")