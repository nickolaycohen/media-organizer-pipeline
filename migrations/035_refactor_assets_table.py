import sqlite3

def run(conn):
    cursor = conn.cursor()
    
    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='assets'")
    if not cursor.fetchone():
        return # Table doesn't exist yet, init_schema will handle it

    # Check current schema
    cursor.execute("PRAGMA table_info(assets)")
    columns = {row[1]: row for row in cursor.fetchall()}
    
    # We want to ensure asset_id is the Primary Key and apple_favorite exists
    is_pk = columns.get('asset_id') and columns['asset_id'][5] == 1
    has_apple_fav = 'apple_favorite' in columns

    if not is_pk or not has_apple_fav:
        print("Refactoring 'assets' table to update Primary Key and add missing columns...")
        
        # 0. Drop dependent views first to prevent schema corruption during rename
        cursor.execute("DROP VIEW IF EXISTS ranked_assets_view")

        # 1. Rename old table
        cursor.execute("ALTER TABLE assets RENAME TO assets_old")
        
        # 2. Create new table with correct schema
        cursor.execute("""
            CREATE TABLE assets (
                asset_id TEXT PRIMARY KEY,
                original_filename TEXT,
                month TEXT,
                MomentsAlbumName TEXT,
                date_created_utc TEXT,
                imported_date_utc TEXT,
                import_id TEXT,
                aesthetic_score REAL,
                google_favorite INTEGER DEFAULT 0,
                apple_favorite INTEGER DEFAULT 0,
                apple_photos_monthly_selection INTEGER DEFAULT 0,
                ignore_continuity_check INTEGER DEFAULT 0,
                file_hash TEXT,
                uploaded_to_google INTEGER DEFAULT 0,
                score_imported_at_utc TEXT,
                created_at_utc TEXT,
                updated_at_utc TEXT
            )
        """)
        
        # 3. Copy data (only columns that exist in assets_old)
        cursor.execute("PRAGMA table_info(assets_old)")
        old_cols = [row[1] for row in cursor.fetchall()]
        target_cols = ["asset_id", "original_filename", "month", "MomentsAlbumName", "date_created_utc", 
                       "imported_date_utc", "import_id", "aesthetic_score", "google_favorite", 
                       "apple_photos_monthly_selection", "ignore_continuity_check", "file_hash", 
                       "uploaded_to_google", "score_imported_at_utc", "created_at_utc", "updated_at_utc"]
        
        valid_cols = [c for c in target_cols if c in old_cols]
        col_str = ", ".join(valid_cols)
        
        cursor.execute(f"INSERT INTO assets ({col_str}) SELECT {col_str} FROM assets_old")
        
        # 4. Cleanup
        cursor.execute("DROP TABLE assets_old")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_asset_id ON assets(asset_id)")
        conn.commit()