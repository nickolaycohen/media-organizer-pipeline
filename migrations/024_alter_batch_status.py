def run(conn):
    cursor = conn.cursor()

    updates = [
        ("100", "verify_export_album.py {month}"),
        ("200", "export_photos_wrapper.py {month}"),
        ("200E", "export_photos_wrapper.py {month}"),
        ("100E", "verify_export_album.py {month}"),
        ("210", "deduplicate_assets.py {month}"),
    ]

    for code, new_script_name in updates:
        cursor.execute("""
            UPDATE batch_status
            SET script_name = ?
            WHERE code = ?;
        """, (new_script_name, code))

    conn.commit()