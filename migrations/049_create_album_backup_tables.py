import os
import sys

def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 049: Creating Media Organizer albums backup tables...")

    # 1. media_organizer_albums: captures full hierarchy of folders and albums under 'Media Organizer on LaCie'
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS media_organizer_albums (
            album_pk            INTEGER PRIMARY KEY,
            album_name          TEXT NOT NULL,
            parent_folder_pk    INTEGER,
            parent_folder_name  TEXT,
            full_path           TEXT NOT NULL,
            is_folder           INTEGER NOT NULL,
            depth               INTEGER NOT NULL,
            zkind               INTEGER NOT NULL,
            asset_count         INTEGER DEFAULT 0,
            synced_at_utc       TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_albums_parent ON media_organizer_albums(parent_folder_pk)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_albums_path ON media_organizer_albums(full_path)")

    # 2. media_organizer_album_assets: captures membership of assets in every album under 'Media Organizer on LaCie'
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS media_organizer_album_assets (
            album_pk            INTEGER NOT NULL,
            asset_id            TEXT NOT NULL,
            original_filename   TEXT,
            date_created_utc    TEXT,
            synced_at_utc       TEXT NOT NULL,
            PRIMARY KEY (album_pk, asset_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_album_assets_asset_id ON media_organizer_album_assets(asset_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mo_album_assets_album_pk ON media_organizer_album_assets(album_pk)")

    conn.commit()
    print("✅ Migration 049 complete: 'media_organizer_albums' and 'media_organizer_album_assets' tables created.")
