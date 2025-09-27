def run(conn):
    cursor = conn.cursor()

    try:
        # set legacy sync flags to 1
        cursor.execute("""
            UPDATE db_updates
            SET raw_synced = 1,
                derived_synced = 1
        """)
        conn.commit()
        print("✅ Bulk inserted sync flags")
    except Exception as e:
        print(f"⚠️ Failed to update db_updates table: {e}")
        raise

    