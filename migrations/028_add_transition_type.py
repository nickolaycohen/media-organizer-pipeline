def run(conn):
    cursor = conn.cursor()

    #cursor.execute("""
    #ALTER TABLE batch_status ADD COLUMN transition_type TEXT DEFAULT 'pipeline';
    #""") - already applied

    # Update existing transitions with correct transition_type
    # Normal sequential transitions: assume all except special ones remain 'pipeline'
    # Special 399->400 transition
    cursor.execute("""
    UPDATE batch_status
    SET transition_type = 'retryable'
    WHERE preceding_code = 399 AND code = 400;
    """)

    # 400->500 transition
    cursor.execute("""
    UPDATE batch_status
    SET transition_type = 'manual'
    WHERE preceding_code = 400 AND code = 500;
    """)

    # Insert 400->500 transition if it does not exist
    cursor.execute("""
    INSERT INTO batch_status (preceding_code, code, transition_type)
    SELECT 400, 500, 'manual'
    WHERE NOT EXISTS (
        SELECT 1 FROM batch_status WHERE preceding_code = 400 AND code = 500
    );
    """)

    conn.commit()
    print("✅ Added transition type ... ")