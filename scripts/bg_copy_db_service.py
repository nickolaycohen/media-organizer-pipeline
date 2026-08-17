import os
import sys
import json
import socket
import errno
import subprocess
import time
import functools
from datetime import datetime

# Force unbuffered prints for background logging
print = functools.partial(print, flush=True)

# Setup script path imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.constants import APPLE_PHOTOS_DB_LOCK_PATH, APPLE_PHOTOS_DB_PATH

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

def is_refresh_needed(last_refresh_str):
    if not os.path.exists(APPLE_PHOTOS_DB_PATH):
        return False, "Source DB path not found (is the drive unmounted?)"
        
    src_mod_time = os.path.getmtime(APPLE_PHOTOS_DB_PATH)
    src_wal_path = APPLE_PHOTOS_DB_PATH + "-wal"
    if os.path.exists(src_wal_path):
        src_mod_time = max(src_mod_time, os.path.getmtime(src_wal_path))
        
    if not last_refresh_str or last_refresh_str == "—":
        return True, "No prior successful refresh recorded."
        
    try:
        last_refresh_dt = datetime.strptime(last_refresh_str, "%Y-%m-%d %H:%M:%S")
        last_refresh_ts = last_refresh_dt.timestamp()
        
        # 2.0-second tolerance threshold for filesystem comparison precision
        if src_mod_time > (last_refresh_ts + 2.0):
            return True, f"Source DB modified time ({datetime.fromtimestamp(src_mod_time).strftime('%Y-%m-%d %H:%M:%S')}) is newer than last refresh ({last_refresh_str})."
        else:
            return False, "Database copy is up to date."
    except Exception as e:
        return True, f"Failed to parse last refresh time: {e}"

def main():
    print(f"[{get_current_utc_str()}] 🔄 Background database copy & sync service started.")
    
    # Check if another service instance is already running
    lock = read_lock_file()
    if lock:
        status = lock.get("status")
        lock_pid = lock.get("pid")
        if status == "refreshing" and is_pid_alive(lock_pid) and lock_pid != os.getpid():
            print(f"[{get_current_utc_str()}] ℹ️ Another background service is already running (PID: {lock_pid}). Exiting.")
            return 0

    loop_interval = 60 # Check every 60 seconds
    
    try:
        while True:
            # 1. Read lock file to get latest state
            lock = read_lock_file()
            last_refresh_timestamp = "—"
            status = "available"
            lock_pid = None
            
            if lock:
                last_refresh_timestamp = lock.get("latest_successful_refresh_utc", "—")
                status = lock.get("status")
                lock_pid = lock.get("pid")
                
            # 2. Check if a refresh is needed
            needed, reason = is_refresh_needed(last_refresh_timestamp)
            
            if needed:
                # 3. Check if planner is active
                if status == "planner_active":
                    if is_pid_alive(lock_pid):
                        print(f"[{get_current_utc_str()}] ℹ️ Planner is currently active (PID: {lock_pid}). Delaying database refresh...")
                        time.sleep(loop_interval)
                        continue
                    else:
                        print(f"[{get_current_utc_str()}] ⚠️ Found stale planner active lock from dead PID {lock_pid}. Overriding lock.")
                
                # 4. Acquire Lock
                current_pid = os.getpid()
                start_time_str = get_current_utc_str()
                print(f"[{get_current_utc_str()}] 🔐 Refresh needed: {reason}")
                print(f"[{get_current_utc_str()}] 🔐 Acquiring lock...")
                write_lock_file(
                    status="refreshing",
                    pid=current_pid,
                    started_at=start_time_str,
                    latest_successful_refresh_utc=last_refresh_timestamp
                )
                
                # 5. Execute processing sequence
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

                # 6. Release Lock
                print(f"\n[{get_current_utc_str()}] 🔓 Releasing lock...")
                if success:
                    last_refresh_timestamp = get_current_utc_str()
                    print(f"[{get_current_utc_str()}] 🎉 Database refresh and metadata sync completed successfully.")
                else:
                    print(f"[{get_current_utc_str()}] ⚠️ Database copy and sync pipeline failed.")
                
                write_lock_file(
                    status="available",
                    pid=None,
                    latest_successful_refresh_utc=last_refresh_timestamp
                )
            else:
                print(f"[{get_current_utc_str()}] 💤 Database copy is up to date (Last sync: {last_refresh_timestamp}). Sleeping {loop_interval}s...")
                
            time.sleep(loop_interval)
            
    except KeyboardInterrupt:
        print(f"\n[{get_current_utc_str()}] 🛑 Background service stopped by user.")
        # Ensure we release lock if we hold it
        lock = read_lock_file()
        if lock and lock.get("pid") == os.getpid() and lock.get("status") == "refreshing":
            print(f"[{get_current_utc_str()}] 🔓 Releasing lock before exit...")
            write_lock_file(
                status="available",
                pid=None,
                latest_successful_refresh_utc=lock.get("latest_successful_refresh_utc", "—")
            )
        return 0

if __name__ == "__main__":
    sys.exit(main())
