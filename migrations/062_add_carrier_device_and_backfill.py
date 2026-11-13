"""
Migration 062: Add carrier_device column to assets table and backfill based on import session
and active owner device migration chains.
"""

def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 062: Adding carrier_device column to assets and backfilling...")

    # 1. Add column if not exists
    cursor.execute("PRAGMA table_info(assets)")
    cols = {row[1] for row in cursor.fetchall()}
    if "carrier_device" not in cols:
        cursor.execute("ALTER TABLE assets ADD COLUMN carrier_device TEXT DEFAULT NULL")
        print("  ✅ Added 'carrier_device' column to 'assets' table.")

    # 2. Add index if not exists
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_carrier_device ON assets(carrier_device)")
    print("  ✅ Ensured index 'idx_assets_carrier_device'.")

    # 3. Build dominant camera per import session
    cursor.execute("""
        SELECT import_uuid, camera_model, assets_count
        FROM imports
        WHERE camera_model IS NOT NULL AND camera_model != 'Unknown' AND camera_model != ''
        ORDER BY import_uuid, assets_count DESC
    """)
    dom_by_session = {}
    for imp_id, cam, cnt in cursor.fetchall():
        if imp_id not in dom_by_session:
            dom_by_session[imp_id] = cam

    # 4. Active phone by owner
    cursor.execute("SELECT owner_name, camera_model FROM device_owners WHERE is_active = 1")
    active_phone_by_owner = dict(cursor.fetchall())

    # 5. Owner intervals
    cursor.execute("""
        SELECT camera_model, owner_name, start_date, end_date 
        FROM device_owners 
        WHERE notes IS NULL OR notes NOT LIKE 'Alias for %'
    """)
    owner_intervals = cursor.fetchall()

    def resolve_owner(cam, dt_str):
        if not dt_str:
            return None
        d = dt_str[:10]
        for c_model, o_name, s_date, e_date in owner_intervals:
            if c_model == cam:
                if (s_date is None or s_date <= d) and (e_date is None or e_date >= d):
                    return o_name
        return None

    # Import timestamps per session
    cursor.execute("SELECT import_uuid, import_timestamp_utc FROM imports GROUP BY import_uuid")
    session_timestamps = dict(cursor.fetchall())

    # Map each session to its resolved carrier device
    session_to_carrier = {}
    for imp_id, cam in dom_by_session.items():
        ts = session_timestamps.get(imp_id)
        owner = resolve_owner(cam, ts)
        carrier = cam
        if owner in active_phone_by_owner:
            active_phone = active_phone_by_owner[owner]
            if cam and ('iPhone' in cam or 'iPad' in cam or 'SCH-' in cam or 'SM-' in cam):
                carrier = active_phone
        session_to_carrier[imp_id] = carrier

    # 6. Batch update assets by import_id
    updated_sessions = 0
    for imp_id, carrier in session_to_carrier.items():
        cursor.execute("UPDATE assets SET carrier_device = ? WHERE import_id = ?", (carrier, imp_id))
        updated_sessions += 1

    print(f"  ✅ Backfilled carrier_device across {updated_sessions} import sessions.")

    # 7. Fallback for any assets where import_id was null or not in session_to_carrier
    cursor.execute("""
        UPDATE assets 
        SET carrier_device = (
            SELECT COALESCE(zea.ZCAMERAMODEL, 'Unknown')
            FROM ZASSET za 
            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON za.ZEXTENDEDATTRIBUTES = zea.Z_PK
            WHERE za.ZUUID = assets.asset_id
        )
        WHERE carrier_device IS NULL
    """)
    print("  ✅ Applied camera model fallback for remaining unmapped assets.")

    # 8. Record in schema_migrations if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'")
    if cursor.fetchone():
        cursor.execute("""
            INSERT OR REPLACE INTO schema_migrations (migration, status, applied_at_utc, description)
            VALUES ('062_add_carrier_device_and_backfill.py', 'applied', datetime('now'), 'Added carrier_device and backfilled via import sessions and device migration chains')
        """)

    conn.commit()
    print("✅ Migration 062 complete: carrier_device column added, indexed, and backfilled.")

