def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 059: Updating step 399 description in batch_status...")
    cursor.execute("""
        UPDATE batch_status
        SET short_label = 'uploading_to_google',
            full_description = 'Upload to Google Account from Staging Folder'
        WHERE code = '399'
    """)
    conn.commit()
    print("✅ Step 399 description updated in batch_status.")

