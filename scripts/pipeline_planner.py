import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from utils.logger import setup_logger
from constants import LOG_PATH

def set_planned_month(cursor, month):
    cursor.execute("DELETE FROM planned_execution")
    cursor.execute("INSERT INTO planned_execution (planned_month, active) VALUES (?, 1)", (month,))

def main(auto_apply):
    logger = setup_logger(LOG_PATH, "pipeline_planner")

    # Example usage
    logger.info("Starting pipeline planning...")

    if auto_apply:
        logger.info("Auto apply is enabled.")
    else:
        logger.info("Auto apply is disabled.")

    # Rest of the main function code here
    logger.info("Pipeline planning completed.")

import argparse
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH

def get_stage_transitions(cursor):
    cursor.execute("""
        SELECT code, preceding_code, full_description
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND code NOT LIKE '%E'
    """)
    return cursor.fetchall()

def get_batch_statuses(cursor):
    cursor.execute("""
        SELECT month, status_code
        FROM month_batches
    """)
    return cursor.fetchall()

def get_latest_import_and_month(cursor):
    # Placeholder: replace with actual logic to fetch latest import and complete month
    cursor.execute("""
        select  distinct i.import_uuid, a.month
        from imports i 
        left join assets a on a.import_id = i.import_uuid
        left join month_batches m on m.month = a.month
        where latest_import_id < i.import_uuid or latest_import_id is null
        and m.status_code < (SELECT code
                                FROM batch_status
                                WHERE preceding_code IS NOT NULL
                                    and length(code) = 3
                                    order by code desc
                                limit 1) or m.status_code is null
        order by i.import_uuid desc, a.month desc
        limit 1;
    """)
    return cursor.fetchone()


def display_summary(transitions, batches, latest_import, latest_month):
    print("\n=== 📊 Stage Transitions ===")
    for code, prev, desc in transitions:
        print(f"{prev} ➜ {code}: {desc}")

    print("\n=== 📦 Batch Statuses ===")
    for month, status in batches:
        print(f"Month: {month}, Status: {status}")

    print("\n=== 🗓️ Latest Info ===")
    print(f"Latest Import Month: {latest_import}")
    print(f"Latest Complete Month: {latest_month}")

def main(auto_apply):
    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    cursor = conn.cursor()

    transitions = get_stage_transitions(cursor)
    batches = get_batch_statuses(cursor)
    latest_import, latest_month = get_latest_import_and_month(cursor)

    display_summary(transitions, batches, latest_import, latest_month)

    logger = setup_logger(LOG_PATH, "pipeline_planner")
    logger.info("=== ✅ Suggested Action ===")
    current_status = None
    for month, status in batches:
        if month == latest_month:
            current_status = status
            break

    next_steps = [code for code, prev, _ in transitions if prev == current_status]
    transitions_str = [f"{current_status}->{code}" for code in next_steps]
    logger.info(f"Run pipeline for: Month={latest_month}, Transitions={transitions_str}")

    if not auto_apply:
        proceed = input("Proceed with this plan? [y/N]: ")
        if proceed.strip().lower() != 'y':
            logger.info("Aborted by user.")
            conn.close()
            sys.exit(0)

    logger.info("🚀 Executing planned steps...")
    set_planned_month(cursor, latest_month)
    conn.commit()
    logger.info(f"📌 Month {latest_month} recorded in planned_execution for next pipeline run.")
    # TODO: trigger executor or store plan

    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-apply", action="store_true", help="Skip confirmation and apply plan immediately")
    args = parser.parse_args()
    main(auto_apply=args.auto_apply)