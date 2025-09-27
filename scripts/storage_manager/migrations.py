import os
import sys
from constants import LOG_PATH
from utils.logger import setup_logger

MODULE_TAG = "migrations"
logger = setup_logger(LOG_PATH, MODULE_TAG)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
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


def apply_pending_migrations(cursor, conn):
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
                logger.error("💥 Migration failed — exiting storage_manager to prevent further actions.")
                conn.close()
                sys.exit(1)