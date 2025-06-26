def get_month_batch_added(cursor):
    """Get the next pending batch from month_batches."""
    cursor.execute('''
        SELECT month FROM month_batches
        WHERE status_code = '000'
        ORDER BY month DESC
        LIMIT 1;
    ''')
    next_batch = cursor.fetchone()
    return next_batch[0] if next_batch else None
