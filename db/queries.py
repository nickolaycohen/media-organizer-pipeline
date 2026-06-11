# Commenting out below as references to this routine are being depricated
# def get_month_batch(cursor, current_code):
from db.connections import close as close_conn
#     """Get the next eligible batch based on the preceding_code for the given status code."""
#     cursor.execute('''
#         SELECT mb.month
#         FROM month_batches mb
#         LEFT JOIN batch_status bs ON mb.status_code = bs.preceding_code
#         WHERE mb.status_code = ?
#         ORDER BY mb.month DESC
#         LIMIT 1;
#     ''', (current_code,))
#     row = cursor.fetchone()
#     return row[0] if row else None

def get_next_code(cursor, current_code):
    """Get the next code based on the preceding code relation."""
    cursor.execute('''
        select b.code
        from batch_status b 
        where b.preceding_code = ? and length(code) = 3
        LIMIT 1;
    ''', (current_code,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_planned_month(cursor):
    cursor.execute("""
        SELECT planned_month 
        FROM planned_execution 
        WHERE active = 1 
        LIMIT 1
    """)
    row = cursor.fetchone()
    return row[0] if row else None

# def get_month_batch_album_verified(cursor):
#     """Get the next pending batch from month_batches."""
#     cursor.execute('''
#         SELECT month FROM month_batches
#         WHERE status_code = '100'
#         ORDER BY month DESC
#         LIMIT 1;
#     ''')
#     next_batch = cursor.fetchone()
#     return next_batch[0] if next_batch else None

def get_stage_transitions(cursor):
    cursor.execute("""
        SELECT code, preceding_code, full_description, transition_type, short_label
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND code NOT LIKE '%E'
        ORDER BY code ASC
    """)
    return cursor.fetchall()

def get_batch_statuses(cursor):
    cursor.execute("""
        SELECT month, status_code
        FROM month_batches
        ORDER BY month DESC
    """)
    return cursor.fetchall()

def get_latest_import_and_month(cursor, transition_type="pipeline"):
    """
    Fetch the latest import and complete month for a given transition type.
    Default is 'pipeline'.
    """
    cursor.execute(f"""
        SELECT (
            SELECT i.import_uuid
            FROM assets a
            JOIN imports i ON a.import_id = i.import_uuid
            WHERE a.month = mb2.month
            ORDER BY i.import_uuid DESC
            LIMIT 1
        ) AS latest_import,
        mb2.month
        FROM month_batches mb2
        WHERE mb2.month < strftime('%Y-%m', 'now')
          AND mb2.status_code IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM batch_status bs
            WHERE bs.preceding_code = mb2.status_code
              AND bs.transition_type = ?
              AND bs.code NOT LIKE '%E'
          )
        ORDER BY mb2.month DESC
        LIMIT 1;
     """, (transition_type,))
    return cursor.fetchone()