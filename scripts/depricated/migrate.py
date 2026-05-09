import os
import sqlite3
import argparse
from constants import MEDIA_ORGANIZER_DB_PATH as DB_PATH

def get_applied_migrations(conn):
    return {row[0] for row in conn.execute("SELECT migration FROM schema_migrations;")}

def apply_migration(conn, migrations_dir, filename):
    module = {}
    with open(os.path.join(migrations_dir, filename)) as f:
        exec(f.read(), module)
    module['run'](conn)
    conn.execute("INSERT INTO schema_migrations (migration, applied_at_utc) VALUES (?, datetime('now'));", (filename,))
    conn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="List pending migrations without applying them")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    migrations_dir = os.path.join(script_dir, "..", "migrations")
    conn = sqlite3.connect(DB_PATH)
    applied = get_applied_migrations(conn)

    pending = [fname for fname in sorted(os.listdir(migrations_dir))
               if fname.endswith(".py") and fname not in applied]

    if args.dry_run:
        if not pending:
            print("✅ No pending migrations.")
        else:
            print("📝 Pending migrations:")
            for fname in pending:
                print(f" - {fname}")
        return

    for fname in pending:
        print(f"🛠 Applying {fname}")
        apply_migration(conn, migrations_dir, fname)

    conn.close()

if __name__ == "__main__":
    main()