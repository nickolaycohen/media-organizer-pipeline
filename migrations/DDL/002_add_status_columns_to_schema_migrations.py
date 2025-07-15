def run(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(schema_migrations);")
    columns = {row[1] for row in cursor.fetchall()}

    if 'status' not in columns:
        # cursor.execute("ALTER TABLE schema_migrations ADD COLUMN status TEXT DEFAULT 'applied';")

    if 'description' not in columns:
        cursor.execute("ALTER TABLE schema_migrations ADD COLUMN description TEXT DEFAULT NULL;")

    conn.commit()