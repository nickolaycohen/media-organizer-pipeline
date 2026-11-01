#!/usr/bin/env python3
"""
Pipeline Executor Monitor & Backup Coordinator.

Displays real-time status of the pipeline executor, batch execution queue,
upload progress, and nightly backup coordination.
"""

import os
import sys
import time
import argparse
import sqlite3
import errno
import re
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
EXECUTOR_LOCK_FILE = os.path.join(PROJECT_ROOT, "db/executor.lock")
BACKUP_LOCK_FILE = os.path.join(PROJECT_ROOT, "db/backup.lock")
DB_PATH = os.path.join(PROJECT_ROOT, "db/media_organizer.db")
LOG_PATH = os.path.join(PROJECT_ROOT, "logs/media_organizer.log")
BACKUP_LOG_PATH = os.path.join(PROJECT_ROOT, "logs/backup.log")

def get_executor_status():
    if not os.path.exists(EXECUTOR_LOCK_FILE):
        return False, None
    try:
        with open(EXECUTOR_LOCK_FILE, "r") as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True, pid
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False, None
        return True, pid
    except Exception:
        return False, None

def get_backup_status():
    return os.path.exists(BACKUP_LOCK_FILE)

def display_dashboard():
    os.system('clear' if os.name == 'posix' else 'cls')
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 80)
    print(f"🚀 MEDIA ORGANIZER PIPELINE - EXECUTOR & BACKUP MONITOR  ({now_str})")
    print("=" * 80)

    # 1. Executor Status
    is_running, pid = get_executor_status()
    if is_running:
        status_banner = f"🟢 ACTIVE (PID: {pid})"
    else:
        status_banner = "⚪ IDLE (Not currently executing)"
    print(f"\n⚡ Executor Process: {status_banner}")

    # 2. Database Queue Status
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT id, planned_month, set_at_utc FROM planned_execution WHERE active = 1 ORDER BY id ASC")
        queue = cursor.fetchall()

        print(f"\n📋 Execution Queue ({len(queue)} batches queued):")
        if not queue:
            print("  (Queue is currently empty)")
        else:
            for idx, (qid, month, set_at) in enumerate(queue):
                cursor.execute("SELECT status_code, updated_at_utc FROM month_batches WHERE month = ?", (month,))
                brow = cursor.fetchone()
                status_code = brow[0] if brow else "Unknown"
                marker = "▶️ [IN PROGRESS]" if idx == 0 and is_running else "⏳ [QUEUED]"
                print(f"  {marker} Month: {month:<7} | Status: {status_code:<5} | Queue ID: {qid:<3} | Scheduled: {set_at}")
        conn.close()
    except Exception as e:
        print(f"  ⚠️ Error querying execution queue: {e}")

    # 3. Live Activity / Upload Progress from Log
    print("\n📊 Recent Executor Activity:")
    current_step = "—"
    upload_progress = None
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-30:]
            
            # Find latest heartbeat and upload progress
            for line in reversed(lines):
                if "⏱️ Heartbeat:" in line and current_step == "—":
                    current_step = line.strip().split("⏱️ Heartbeat:")[-1].strip()
                if "[upload_to_google_photos:" in line and "Uploading:" in line and not upload_progress:
                    m = re.search(r'\[(\d+/\d+)\] Uploading: ([^ ]+) \(([^)]+)\) - Est\. Completion: ([^ ]+) \(([^)]+)\)', line)
                    if m:
                        upload_progress = f"Asset {m.group(1)}: {m.group(2)} ({m.group(3)}) | ETA: {m.group(4)} ({m.group(5)})"

            if current_step != "—":
                print(f"  ⏱️ Current Step: {current_step}")
            if upload_progress:
                print(f"  📤 Upload Progress: {upload_progress}")

            print("  --- Latest Log Tail ---")
            for line in lines[-5:]:
                print("   " + line.strip()[:100])
        except Exception as e:
            print(f"  ⚠️ Could not read log file: {e}")
    else:
        print("  (No log output available)")

    # 4. Backup Safety & Nightly Schedule
    print("\n🛡️ Nightly Backup Schedule & Concurrency Safety:")
    print("  • 03:00 AM  - macOS Time Machine backup (`tmutil startbackup`)")
    print("  • 05:30 AM  - Apple Photos albums backup (`reconstruct_apple_albums.py --backup`)")
    print("  • 06:00 AM  - SQLite Database backup (`backup_database.py`)")
    
    backup_active = get_backup_status()
    if backup_active:
        print("  🔒 Backup Lock: 🟡 ACTIVE BACKUP IN PROGRESS")
    else:
        print("  🔒 Backup Lock: 🟢 CLEAR (No backup collision)")

    print("\n" + "=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Monitor pipeline executor and backup coordination.")
    parser.add_argument("--watch", "-w", action="store_true", help="Continuously refresh status every 5 seconds")
    parser.add_argument("--interval", "-i", type=int, default=5, help="Refresh interval in seconds (default: 5)")
    args = parser.parse_args()

    if args.watch:
        try:
            while True:
                display_dashboard()
                print(f"🔄 Auto-refreshing every {args.interval}s... Press Ctrl+C to exit.")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n👋 Monitor exited.")
    else:
        display_dashboard()

if __name__ == "__main__":
    main()

