import os
import sys
import json
import socket
import errno
import subprocess
from datetime import datetime

# Setup script path imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.constants import APPLE_PHOTOS_DB_LOCK_PATH

def get_current_utc_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def is_pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        # EPERM (Permission denied) means the process is alive but belongs to someone else
        return True

def read_lock_file():
    if not os.path.exists(APPLE_PHOTOS_DB_LOCK_PATH):
        return None
    try:
        with open(APPLE_PHOTOS_DB_LOCK_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Error reading lock file: {e}")
        return None

def write_lock_file(status, pid, started_at=None, latest_successful_refresh_utc="—"):
    try:
        lock_data = {
            "status": status,
            "pid": pid,
            "started_at": started_at,
            "host": socket.gethostname(),
            "latest_successful_refresh_utc": latest_successful_refresh_utc
        }
        with open(APPLE_PHOTOS_DB_LOCK_PATH, "w") as f:
            json.dump(lock_data, f, indent=2)
    except Exception as e:
        print(f"❌ Error writing lock file: {e}")

def main():
    print(f"[{get_current_utc_str()}] 🔄 Background database copy & sync service started.")
    
    # 1. Check existing lock
    lock = read_lock_file()
    last_refresh_timestamp = "—"
    
    if lock:
        last_refresh_timestamp = lock.get("latest_successful_refresh_utc", "—")
        status = lock.get("status")
        lock_pid = lock.get("pid")
        
        if status in ["refreshing", "planner_active"]:
            if is_pid_alive(lock_pid):
                print(f"[{get_current_utc_str()}] ℹ️ Database lock is currently active (Status: {status}, PID: {lock_pid}). Exiting.")
                return 0
            else:
                print(f"[{get_current_utc_str()}] ⚠️ Found stale lock file from dead PID {lock_pid}. Overriding lock.")
    
    # 2. Acquire Lock
    current_pid = os.getpid()
    start_time_str = get_current_utc_str()
    print(f"[{get_current_utc_str()}] 🔐 Acquiring lock...")
    write_lock_file(
        status="refreshing",
        pid=current_pid,
        started_at=start_time_str,
        latest_successful_refresh_utc=last_refresh_timestamp
    )
    
    # 3. Execute processing sequence
    script_dir = os.path.dirname(os.path.abspath(__file__))
    steps = [
        ("0.0 Copy Apple Photos Database Copy", ["copy_all_media_photos_db.py"]),
        ("0.1 Storage Manager Migrations", ["storage_manager_main.py", "--migrate"]),
        ("0.2 Sync Raw Assets", ["sync_photos_raw.py"]),
        ("0.3 Sync Derived Metadata", ["sync_photos_derived.py", "--force"])
    ]
    
    success = True
    for step_name, args in steps:
        script_path = os.path.join(script_dir, args[0])
        print(f"\n[{get_current_utc_str()}] 🚀 Running step: {step_name}")
        
        try:
            res = subprocess.run([sys.executable, script_path] + args[1:], check=False)
            if res.returncode != 0:
                print(f"[{get_current_utc_str()}] ❌ Step failed: {step_name} (Exit code {res.returncode})")
                success = False
                break
            print(f"[{get_current_utc_str()}] ✅ Step completed successfully: {step_name}")
        except Exception as step_err:
            print(f"[{get_current_utc_str()}] ❌ Execution exception in step {step_name}: {step_err}")
            success = False
            break

    # 4. Release Lock
    print(f"\n[{get_current_utc_str()}] 🔓 Releasing lock...")
    if success:
        new_refresh_timestamp = get_current_utc_str()
        print(f"[{get_current_utc_str()}] 🎉 Database refresh and metadata sync completed successfully.")
        write_lock_file(
            status="available",
            pid=None,
            latest_successful_refresh_utc=new_refresh_timestamp
        )
    else:
        print(f"[{get_current_utc_str()}] ⚠️ Database copy and sync pipeline failed. Reverting lock.")
        write_lock_file(
            status="available",
            pid=None,
            latest_successful_refresh_utc=last_refresh_timestamp
        )
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
