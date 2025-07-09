def get_month_batch(cursor, current_code):
    """Get the next eligible batch based on the preceding_code for the given status code."""
    cursor.execute('''
        SELECT mb.month
        FROM month_batches mb
        JOIN batch_status bs ON mb.status_code = bs.preceding_code
        WHERE bs.preceding_code = ?
        ORDER BY mb.month DESC
        LIMIT 1;
    ''', (current_code,))
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