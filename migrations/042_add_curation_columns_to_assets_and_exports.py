def run(conn):
    cursor = conn.cursor()
    try:
        # Check assets columns
        cursor.execute("PRAGMA table_info(assets)")
        assets_cols = [row[1] for row in cursor.fetchall()]
        
        if "to_be_curated_album" not in assets_cols:
            cursor.execute("ALTER TABLE assets ADD COLUMN to_be_curated_album TEXT")
            print("✅ Added 'to_be_curated_album' column to assets table")
        if "curated_album" not in assets_cols:
            cursor.execute("ALTER TABLE assets ADD COLUMN curated_album TEXT")
            print("✅ Added 'curated_album' column to assets table")

        # Check moment_exports columns
        cursor.execute("PRAGMA table_info(moment_exports)")
        exports_cols = [row[1] for row in cursor.fetchall()]

        if "curation_stage" not in exports_cols:
            cursor.execute("ALTER TABLE moment_exports ADD COLUMN curation_stage TEXT DEFAULT 'to_be_curated'")
            print("✅ Added 'curation_stage' column to moment_exports table")

        conn.commit()
    except Exception as e:
        print(f"⚠️ Migration 042 failed: {e}")
        raise
