

def run(conn):
    cursor = conn.cursor()

    # Step 1: Create batch_status table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batch_status (
            code TEXT PRIMARY KEY,
            short_label TEXT,
            full_description TEXT,
            pipeline_stage TEXT,
            script_name TEXT
        );
    """)
    print("✅ Created batch_status table.")

    # Step 2: Populate batch_status from unique month_batches values
    cursor.execute("""
        SELECT DISTINCT status_code, status_label, pipeline_step
        FROM month_batches
        WHERE status_code IS NOT NULL
    """)
    rows = cursor.fetchall()
    for code, short_label, stage in rows:
        cursor.execute("""
            INSERT OR IGNORE INTO batch_status (code, short_label, full_description, pipeline_stage, script_name)
            VALUES (?, ?, ?, ?, ?)
        """, (code, short_label, short_label, stage, None))

    # Step 3: Rebuild month_batches without status_label and pipeline_step
    cursor.execute("ALTER TABLE month_batches RENAME TO month_batches_old;")
    cursor.execute("""
        CREATE TABLE month_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            batch_number INTEGER,
            assets_count INTEGER,
            created_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status_code TEXT,
            FOREIGN KEY (status_code) REFERENCES batch_status(code)
        );
    """)
    cursor.execute("""
        INSERT INTO month_batches (id, month, batch_number, assets_count, created_at_utc, updated_at_utc, status_code)
        SELECT id, month, batch_number, assets_count, created_at_utc, updated_at_utc, status_code
        FROM month_batches_old;
    """)
    cursor.execute("DROP TABLE month_batches_old;")

    print("✅ Recreated month_batches table with foreign key to batch_status.")

    conn.commit()