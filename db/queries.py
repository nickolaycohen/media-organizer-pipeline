# Commenting out below as references to this routine are being depricated
# def get_month_batch(cursor, current_code):
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
    if not row:
        logger.info("No active planned month found. Exiting.")
        conn.close()
        exit(0)

    month = row[0]
    logger.info(f"Using planned month: {month}")
    return month

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