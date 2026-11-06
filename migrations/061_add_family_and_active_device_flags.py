def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 061: Adding is_family to owners and is_active to device_owners...")

    # 1. Add is_family column to owners table
    cursor.execute("PRAGMA table_info(owners)")
    owner_cols = {row[1] for row in cursor.fetchall()}
    if "is_family" not in owner_cols:
        cursor.execute("ALTER TABLE owners ADD COLUMN is_family INTEGER DEFAULT 0")
        print("  ✅ Added 'is_family' column to 'owners' table.")

    # Populate initial family members
    family_members = ('Niki', 'Benny', 'Cohens', 'Nickolay C.', 'Roberto', 'Eva', 'Cohens Bulgaria', 'Cohen Bulgaria')
    placeholders = ','.join('?' for _ in family_members)
    cursor.execute(f"UPDATE owners SET is_family = 1 WHERE name IN ({placeholders})", family_members)
    print(f"  ✅ Marked family members: {family_members}")

    # 2. Add is_active column to device_owners table
    cursor.execute("PRAGMA table_info(device_owners)")
    do_cols = {row[1] for row in cursor.fetchall()}
    if "is_active" not in do_cols:
        cursor.execute("ALTER TABLE device_owners ADD COLUMN is_active INTEGER DEFAULT 1")
        print("  ✅ Added 'is_active' column to 'device_owners' table.")

    # Update active status based on ownership dates
    cursor.execute("""
        UPDATE device_owners 
        SET is_active = CASE 
            WHEN (end_date IS NULL OR end_date >= date('now')) THEN 1 
            ELSE 0 
        END
    """)
    print("  ✅ Synchronized 'is_active' status in 'device_owners' based on current date.")

    conn.commit()
    print("✅ Migration 061 complete: Family and active device flags configured.")
