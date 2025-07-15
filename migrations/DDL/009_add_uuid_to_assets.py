def run(conn):
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE assets ADD COLUMN apple_photos_uuid TEXT;")
