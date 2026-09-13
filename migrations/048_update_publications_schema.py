import os
import re

def run(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(publications)")
        cols = {row[1]: row for row in cursor.fetchall()}
        
        if "asset_uuid" in cols and "filename" in cols and "account" in cols:
            print("ℹ️ 'publications' table already updated to new schema.")
            return

        print("🔄 Migrating 'publications' table to new schema...")
        
        # Read existing records from publications
        cursor.execute("""
            SELECT p.id, p.asset_id, p.moment_name, p.platform, p.published_at_utc, a.original_filename
            FROM publications p
            LEFT JOIN assets a ON a.asset_id = p.asset_id
            ORDER BY p.id ASC
        """)
        old_rows = cursor.fetchall()
        print(f"📦 Found {len(old_rows)} existing publication records to migrate.")

        migrated_data = []
        for row in old_rows:
            p_id, asset_id, moment_name, platform, pub_date, orig_fname = row
            fname = orig_fname or ""
            
            # Determine media type from filename extension
            ext = os.path.splitext(fname)[1].lower() if fname else ""
            if ext in ('.mov', '.mp4', '.m4v', '.avi', '.mpg'):
                row_platform = 'YouTube'
                row_account = 'nickolay.cohen@gmail.com'
            else:
                row_platform = 'Shutterfly'
                row_account = 'nickolay.cohen@gmail.com'
                
            migrated_data.append((
                p_id,
                asset_id,
                fname,
                moment_name,
                row_platform,
                row_account,
                pub_date
            ))

        # Recreate publications table with new schema
        cursor.execute("DROP TABLE IF EXISTS publications")
        cursor.execute("""
            CREATE TABLE publications (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_uuid          TEXT NOT NULL,
                filename            TEXT NOT NULL,
                moment_name         TEXT NOT NULL,
                platform            TEXT NOT NULL,
                account             TEXT NOT NULL,
                published_at_utc    TEXT NOT NULL,
                FOREIGN KEY (asset_uuid) REFERENCES assets(asset_id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_publications_asset_uuid ON publications(asset_uuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_publications_moment_name ON publications(moment_name)")

        if migrated_data:
            cursor.executemany("""
                INSERT INTO publications (id, asset_uuid, filename, moment_name, platform, account, published_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, migrated_data)

        conn.commit()
        print(f"✅ Successfully migrated {len(migrated_data)} publication records with new schema.")
    except Exception as e:
        print(f"⚠️ Migration 048 failed: {e}")
        conn.rollback()
        raise
