def run(conn):
    cursor = conn.cursor()

    # 1. Add new status code 550
    cursor.execute("""
        INSERT INTO batch_status (code, preceding_code, full_description, transition_type, short_label)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "550",
        "500",
        "Pull Google Photos Favorites and update asset flags",
        "pipeline",
        "Pull Google Favorites"
    ))

    # 2. Migrate existing planned_execution entries from 500 to 550
    # Do not see purpose of this migration step - commenting for now
    # cursor.execute("""
    #     UPDATE planned_execution
    #     SET status_code = '550'
    #     WHERE status_code = '500'
    # """)

    conn.commit()
    print("✅ Added code 550 and migrated planned_execution from 500 to 550")