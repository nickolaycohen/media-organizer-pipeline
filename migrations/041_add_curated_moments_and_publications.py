def run(conn):
    cursor = conn.cursor()
    try:
        # Create curated_moments table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS curated_moments (
                moment_name          TEXT PRIMARY KEY,
                to_be_curated_count  INTEGER DEFAULT 0,
                curated_count        INTEGER DEFAULT 0,
                memory_stage         TEXT DEFAULT 'M100',
                last_pipeline_sync   TEXT,
                last_curated_sync    TEXT,
                photos_to_be_curated_exists INTEGER DEFAULT 0,
                photos_curated_exists       INTEGER DEFAULT 0
            )
        """)
        print("✅ Created 'curated_moments' table")

        # Create publications table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS publications (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id            TEXT NOT NULL,
                moment_name         TEXT NOT NULL,
                platform            TEXT NOT NULL,
                published_at_utc    TEXT NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
            )
        """)
        print("✅ Created 'publications' table")

        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 041 failed: {e}")
        raise
