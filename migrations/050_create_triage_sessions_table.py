import os
import sys

def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 050: Creating triage_sessions and triage_session_assets tables...")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS triage_sessions (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            triage_id                   TEXT UNIQUE NOT NULL,
            source_directory            TEXT NOT NULL,
            destination_directory       TEXT NOT NULL,
            status                      TEXT NOT NULL DEFAULT 'running',
            total_files_scanned         INTEGER DEFAULT 0,
            duplicates_count            INTEGER DEFAULT 0,
            duplicates_bytes            INTEGER DEFAULT 0,
            compressed_duplicates_count INTEGER DEFAULT 0,
            compressed_duplicates_bytes INTEGER DEFAULT 0,
            review_similar_count        INTEGER DEFAULT 0,
            review_similar_bytes        INTEGER DEFAULT 0,
            upgrade_candidates_count    INTEGER DEFAULT 0,
            upgrade_candidates_bytes    INTEGER DEFAULT 0,
            discard_sim_count           INTEGER DEFAULT 0,
            discard_sim_bytes           INTEGER DEFAULT 0,
            web_artifact_count          INTEGER DEFAULT 0,
            web_artifact_bytes          INTEGER DEFAULT 0,
            new_count                   INTEGER DEFAULT 0,
            new_bytes                   INTEGER DEFAULT 0,
            dry_run                     INTEGER DEFAULT 0,
            started_at_utc              TEXT DEFAULT (datetime('now')),
            completed_at_utc            TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_sessions_triage_id ON triage_sessions(triage_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_sessions_status ON triage_sessions(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_sessions_started_at ON triage_sessions(started_at_utc)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS triage_session_assets (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            triage_session_id           INTEGER NOT NULL REFERENCES triage_sessions(id),
            triage_id                   TEXT NOT NULL,
            original_rel_path           TEXT NOT NULL,
            filename                    TEXT NOT NULL,
            file_size_bytes             INTEGER NOT NULL,
            category                    TEXT NOT NULL,
            sub_type                    TEXT,
            decision_reason             TEXT,
            destination_path            TEXT,
            created_at_utc              TEXT DEFAULT (datetime('now'))
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_assets_session_id ON triage_session_assets(triage_session_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_assets_category ON triage_session_assets(category)")

    conn.commit()
    print("✅ Migration 050 complete: 'triage_sessions' and 'triage_session_assets' tables created.")
