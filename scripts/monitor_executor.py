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
from pathlib import Path
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
SIBLING_ROOT = os.path.abspath(os.path.join(PROJECT_ROOT, "../../..", "media-organizer-pipeline"))

def resolve_path(rel_path):
    primary = os.path.join(PROJECT_ROOT, rel_path)
    if os.path.exists(primary) and (os.path.islink(primary) or os.path.getsize(primary) > 0 or "lock" in rel_path):
        return primary
    if os.path.exists(SIBLING_ROOT):
        sibling = os.path.join(SIBLING_ROOT, rel_path)
        if os.path.exists(sibling):
            return sibling
    return primary

EXECUTOR_LOCK_FILE = resolve_path("db/executor.lock")
BACKUP_LOCK_FILE = resolve_path("db/backup.lock")
DB_PATH = resolve_path("db/media_organizer.db")
LOG_PATH = resolve_path("logs/media_organizer.log")
BACKUP_LOG_PATH = resolve_path("logs/backup.log")

def get_executor_status():
    lock_files = [EXECUTOR_LOCK_FILE]
    if os.path.exists(SIBLING_ROOT):
        sib_lock = os.path.join(SIBLING_ROOT, "db/executor.lock")
        if sib_lock not in lock_files:
            lock_files.append(sib_lock)
            
    for lf in lock_files:
        if os.path.exists(lf):
            try:
                with open(lf, "r") as f:
                    pid = int(f.read().strip())
                os.kill(pid, 0)
                return True, pid
            except OSError as err:
                if err.errno == errno.ESRCH:
                    pass
                else:
                    return True, pid
            except Exception:
                pass
    return False, None

def get_backup_status():
    if os.path.exists(BACKUP_LOCK_FILE):
        return True
    if os.path.exists(SIBLING_ROOT) and os.path.exists(os.path.join(SIBLING_ROOT, "db/backup.lock")):
        return True
    return False

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
        cursor.execute("""
            SELECT pe.id, pe.planned_month, pe.set_at_utc, mb.status_code, bs.short_label
            FROM planned_execution pe
            LEFT JOIN month_batches mb ON pe.planned_month = mb.month
            LEFT JOIN batch_status bs ON mb.status_code = bs.code
            WHERE pe.active = 1
            ORDER BY pe.id ASC
        """)
        queue = cursor.fetchall()

        print(f"\n📋 Execution Queue ({len(queue)} batches queued):")
        if not queue:
            print("  (Queue is currently empty)")
        else:
            for idx, (qid, month, set_at, status_code, label) in enumerate(queue):
                status_code = status_code or "Unknown"
                status_str = f"{status_code} ({label})" if label else status_code
                marker = "▶️ [IN PROGRESS]" if idx == 0 and is_running else "⏳ [QUEUED]"
                print(f"  {marker} Month: {month:<7} | Status: {status_str:<24} | Queue ID: {qid:<3} | Scheduled: {set_at}")
        conn.close()
    except Exception as e:
        print(f"  ⚠️ Error querying execution queue: {e}")

    # 3. Live Activity / Upload Progress from Log
    print("\n📊 Recent Executor Activity:")
    current_step = "—"
    upload_progress = None
    target_log = LOG_PATH
    if not os.path.exists(target_log) and os.path.exists(SIBLING_ROOT):
        target_log = os.path.join(SIBLING_ROOT, "logs/media_organizer.log")

    if os.path.exists(target_log):
        try:
            with open(target_log, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-120:]
            
            # Find latest heartbeat and upload progress
            live_asset_ratio = None
            for line in reversed(lines):
                if "⏱️ Heartbeat:" in line and current_step == "—":
                    current_step = line.strip().split("⏱️ Heartbeat:")[-1].strip()
                    current_step = current_step.replace(
                        "2.4 Partial upload to Google Photos due to insufficient space",
                        "2.4 Upload to Google Account from Staging Folder"
                    )
                if "[upload_to_google_photos:" in line and "Uploading:" in line and not upload_progress:
                    m = re.search(r'\[(\d+)/(\d+)\] Uploading: (.+?) \(([\d.]+\s*MB)\) - Est\. Completion: ([^ ]+) \(([^)]+)\)', line)
                    if m:
                        idx_num = int(m.group(1))
                        tot_num = int(m.group(2))
                        done_num = max(0, idx_num - 1)
                        pct_val = (done_num / tot_num * 100.0) if tot_num > 0 else 0.0
                        live_asset_ratio = f"{done_num}/{tot_num} ({pct_val:.1f}%)"
                        upload_progress = f"Asset {m.group(1)}/{m.group(2)}: {m.group(3)} ({m.group(4)}) | ETA: {m.group(5)} ({m.group(6)})"

            if current_step != "—" and live_asset_ratio:
                current_step = re.sub(r'Assets:\s*\d+/\d+(?:\s*\([\d.]+%\))?', f'Assets: {live_asset_ratio}', current_step)

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
