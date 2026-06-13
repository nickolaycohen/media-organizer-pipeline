import sqlite3

def run(conn):
    cursor = conn.cursor()
    
    print("Creating 'moment_exports' table to track assets pushed to Apple Photos...")
    
    # This table tracks which assets were exported to which albums and when.
    # Using a composite primary key to allow the same asset to be exported to different albums
    # if the pipeline logic changes, while preventing duplicate log entries for the same target.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS moment_exports (
            asset_id TEXT,
            album_name TEXT,
            exported_at_utc TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (asset_id, album_name)
        )
    """)
    conn.commit()