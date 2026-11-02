def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 060: Adding published_url to publications and shutterfly_url/youtube_url to curated_moments...")

    # 1. Add published_url column to publications table
    cursor.execute("PRAGMA table_info(publications)")
    pub_cols = {row[1] for row in cursor.fetchall()}
    if "published_url" not in pub_cols:
        cursor.execute("ALTER TABLE publications ADD COLUMN published_url TEXT")
        print("  ✅ Added 'published_url' column to 'publications' table.")

    # 2. Add shutterfly_url and youtube_url columns to curated_moments table
    cursor.execute("PRAGMA table_info(curated_moments)")
    cm_cols = {row[1] for row in cursor.fetchall()}
    if "shutterfly_url" not in cm_cols:
        cursor.execute("ALTER TABLE curated_moments ADD COLUMN shutterfly_url TEXT")
        print("  ✅ Added 'shutterfly_url' column to 'curated_moments' table.")
    if "youtube_url" not in cm_cols:
        cursor.execute("ALTER TABLE curated_moments ADD COLUMN youtube_url TEXT")
        print("  ✅ Added 'youtube_url' column to 'curated_moments' table.")

    conn.commit()
    print("✅ Migration 060 complete: Published URL tracking columns added.")

