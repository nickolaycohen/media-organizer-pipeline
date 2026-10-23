import os
import sys

def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 051: Adding visual similarity and Moment tracking columns...")

    # 1. Add visual similarity and Moment tracking columns to triage_session_assets
    cursor.execute("PRAGMA table_info(triage_session_assets)")
    existing_asset_cols = {row[1] for row in cursor.fetchall()}

    new_asset_cols = [
        ("matched_asset_uuid", "TEXT"),
        ("matched_moment_name", "TEXT"),
        ("visual_similarity_score", "REAL"),
        ("hamming_distance", "INTEGER"),
        ("perceptual_hash", "TEXT"),
    ]

    for col_name, col_type in new_asset_cols:
        if col_name not in existing_asset_cols:
            cursor.execute(f"ALTER TABLE triage_session_assets ADD COLUMN {col_name} {col_type}")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_assets_moment ON triage_session_assets(matched_moment_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_assets_uuid ON triage_session_assets(matched_asset_uuid)")

    # 2. Add visual similarity summary counters to triage_sessions
    cursor.execute("PRAGMA table_info(triage_sessions)")
    existing_session_cols = {row[1] for row in cursor.fetchall()}

    new_session_cols = [
        ("visual_duplicate_count", "INTEGER DEFAULT 0"),
        ("moment_matched_count", "INTEGER DEFAULT 0"),
    ]

    for col_name, col_type in new_session_cols:
        if col_name not in existing_session_cols:
            cursor.execute(f"ALTER TABLE triage_sessions ADD COLUMN {col_name} {col_type}")

    conn.commit()
    print("✅ Migration 051 complete: Visual similarity and Moment tracking columns added.")
