def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 058: Adding performance indexes for proximity matching...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_date_created ON assets(date_created_utc);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_import_id ON assets(import_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_moments_album ON assets(MomentsAlbumName);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zasset_zuuid ON ZASSET(ZUUID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zasset_lat_lon ON ZASSET(ZLATITUDE, ZLONGITUDE);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zextended_pk ON ZEXTENDEDATTRIBUTES(Z_PK);")
    conn.commit()
    print("✅ Performance indexes created successfully.")

