def run(conn):
    cursor = conn.cursor()

    # Check and convert only records missing applied_at_utc
    try:
        # cursor.execute("""
        #     SELECT id, applied_at
        #     FROM schema_migrations
        #     WHERE applied_at_utc IS NULL AND applied_at IS NOT NULL
        # """)
        # rows = cursor.fetchall()

        # for row_id, applied_at in rows:
        #     cursor.execute("""
        #         UPDATE schema_migrations
        #         SET applied_at_utc = datetime(applied_at, 'localtime')
        #         WHERE id = ?
        #     """, (row_id,))

        conn.commit()
    except Exception as e:
        print(f"Error during UTC backfill of applied_at_utc: {e}")
        raise