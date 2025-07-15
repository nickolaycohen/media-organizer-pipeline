"""Add batch_month_id FK

Revision ID: 019
Revises: 017
Create Date: 2023-09-01 12:00:00.000000
"""

def run(conn):
    cursor = conn.cursor()

    cursor.execute("""
        ALTER TABLE pipeline_executions
        ADD COLUMN batch_month_id INTEGER REFERENCES month_batches(id);
    """)

    conn.commit()
