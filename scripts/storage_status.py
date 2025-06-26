import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH
from constants import LOG_PATH
from utils.logger import setup_logger

MODULE_TAG = "storage_status"
logger = setup_logger(LOG_PATH, MODULE_TAG)


def get_migration_status(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'")
    name = cursor.fetchone()
    if name:
        logger.info("✅ schema_migrations table exists.")
        cursor.execute("SELECT migration, applied_at_utc FROM schema_migrations ORDER BY applied_at_utc DESC LIMIT 1")
        latest = cursor.fetchone()
        if latest:
            from datetime import datetime, timezone
            utc_time = latest[1]
            dt_utc = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            local_time = dt_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
            logger.info(f"📦 Latest applied migration: {latest[0]} at {utc_time} UTC / {local_time} local")
            # NOTE: This check only works if migration records are pre-inserted with status = 'pending'
            # before being executed by the migration engine.
            # Check for pending migrations
            cursor.execute("SELECT migration, description FROM schema_migrations WHERE status = 'pending'")
            pending = cursor.fetchall()
            if pending:
                logger.info("🕒 Pending migrations:")
                for filename, desc in pending:
                    logger.info(f" - {filename}: {desc}")
            else:
                logger.info("✅ No pending migrations.")
        else:
            logger.info("📦 No migrations recorded yet.")
    else:
        logger.warning("⚠️ No migration table found. Initializing DB...")
        init_schema(cursor)

def init_schema(cursor):
    # Initial DB structure: create essential tables and migration record
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS month_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            batch_number INTEGER,
            assets_count INTEGER,
            status_code TEXT,
            status_label TEXT,
            created_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            label TEXT NOT NULL,
            status TEXT NOT NULL,
            executed_at_utc TEXT DEFAULT (datetime('now'))
        );
    """)
    cursor.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        migration TEXT NOT NULL UNIQUE,
        applied_at_utc TEXT
    );""")

    # Drop and rebuild schema_migrations if 'filename' column exists
    cursor.execute("PRAGMA table_info(schema_migrations);")
    columns = {row[1] for row in cursor.fetchall()}
    if "filename" in columns:
        logger.warning("📛 Detected legacy 'filename' column — rebuilding schema_migrations table...")
        cursor.execute("ALTER TABLE schema_migrations RENAME TO schema_migrations_old;")
        cursor.execute("""
            CREATE TABLE schema_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration TEXT NOT NULL,
                applied_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'applied',
                description TEXT DEFAULT NULL
            )
        """)
        cursor.execute("""
            INSERT INTO schema_migrations (id, migration, applied_at_utc, status, description)
            SELECT id, filename, applied_at_utc, status, description
            FROM schema_migrations_old;
        """)
        cursor.execute("DROP TABLE schema_migrations_old;")
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration TEXT NOT NULL,
                applied_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'applied',
                description TEXT DEFAULT NULL
            )
        """)
    cursor.execute("""
        INSERT INTO schema_migrations (migration, status, description)
        VALUES (?, 'applied', ?)
    """, ("000_initial_schema", "Initial schema with month_batches and schema_migrations"))
    logger.info("✅ Initialized database schema.")

def main():
    logger.info(f"🗂  Checking Storage Status at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    get_migration_status(cursor)
    conn.commit()
    conn.close()
    if "--migrate" in sys.argv:
        import subprocess
        migration_script = os.path.join(os.path.dirname(__file__), "migrate.py")
        logger.info("🔍 Checking for unapplied migrations...")
        result = subprocess.run([sys.executable, migration_script, "--dry-run"], capture_output=True, text=True)
        if "No pending migrations" in result.stdout:
            logger.info("✅ No unapplied migrations to run.")
        else:
            logger.info("🔧 Running pending migrations...")
            subprocess.run([sys.executable, migration_script])

if __name__ == "__main__":
    main()