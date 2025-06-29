

def run(conn):
    cursor = conn.cursor()

    # Step 1: Rename the current table
    cursor.execute("ALTER TABLE assets RENAME TO assets_old;")

    # Step 2: Recreate the table without the 'apple_photos_uuid' column
    cursor.execute("""
        CREATE TABLE assets (
            file_hash TEXT,
            month TEXT,
            import_id TEXT,
            aesthetic_score REAL,
            original_filename TEXT,
            date_created_utc TEXT,
            imported_date_utc TEXT,
            score_imported_at_utc TEXT,
            uploaded_to_google INTEGER,
            created_at_utc TEXT,
            updated_at_utc TEXT,
            asset_id TEXT,
            PRIMARY KEY (original_filename, month)
        );
    """)

    # Step 3: Copy the data back from the old table
    cursor.execute("""
        INSERT INTO assets (
            file_hash, month, import_id, aesthetic_score, original_filename,
            date_created_utc, imported_date_utc, score_imported_at_utc,
            uploaded_to_google, created_at_utc, updated_at_utc, asset_id
        )
        SELECT
            file_hash, month, import_id, aesthetic_score, original_filename,
            date_created_utc, imported_date_utc, score_imported_at_utc,
            uploaded_to_google, created_at_utc, updated_at_utc, asset_id
        FROM assets_old;
    """)

    # Step 4: Drop the old table
    cursor.execute("DROP TABLE assets_old;")