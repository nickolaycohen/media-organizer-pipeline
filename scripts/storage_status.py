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
        # List migration files in migrations/ folder
        migrations_folder = os.path.join(project_root, "migrations")
        migration_files = []
        if os.path.isdir(migrations_folder):
            migration_files = sorted(f for f in os.listdir(migrations_folder) if f.endswith(".py"))
        else:
            logger.warning(f"⚠️ Migrations folder not found at {migrations_folder}")

        # Get existing migrations from DB
        cursor.execute("SELECT migration FROM schema_migrations")
        existing_migrations = {row[0] for row in cursor.fetchall()}

        # Insert missing migrations as pending
        new_pending = []
        for filename in migration_files:
            if filename not in existing_migrations:
                cursor.execute("INSERT INTO schema_migrations (migration, status, description) VALUES (?, 'pending', ?)",
                               (filename, "Detected in folder"))
                new_pending.append(filename)
        if new_pending:
            logger.info("🆕 Detected new migration files added to folder and marked as pending:")
            for fn in new_pending:
                logger.info(f" - {fn}")

        cursor.execute("SELECT migration, applied_at_utc FROM schema_migrations WHERE status='applied' ORDER BY applied_at_utc DESC LIMIT 1")
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
    if "--migrate" in sys.argv:
        import importlib.util
        from datetime import datetime
        migrations_folder = os.path.join(project_root, "migrations")
        cursor.execute("SELECT migration FROM schema_migrations WHERE status='pending'")
        pending_migrations = cursor.fetchall()
        if not pending_migrations:
            logger.info("✅ No unapplied migrations to run.")
        else:
            logger.info("🔧 Running pending migrations...")
            for (migration,) in pending_migrations:
                mig_path = os.path.join(migrations_folder, migration)
                try:
                    logger.info(f"▶️ Applying migration: {migration}")
                    spec = importlib.util.spec_from_file_location("migration", mig_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    module.run(conn)
                    cursor.execute("""
                        UPDATE schema_migrations
                        SET status='applied', applied_at_utc=datetime('now')
                        WHERE migration = ?
                    """, (migration,))
                    conn.commit()
                    logger.info(f"✅ Successfully applied migration: {migration}")
                except Exception as e:
                    logger.error(f"❌ Failed to apply migration {migration}: {e}")
                    logger.error("💥 Migration failed — exiting storage_status to prevent further actions.")
                    conn.close()
                    sys.exit(1)
    conn.close()

if __name__ == "__main__":
    main()