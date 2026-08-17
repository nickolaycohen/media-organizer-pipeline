import os
import sys
import json
import socket
import errno
import subprocess
import time
from datetime import datetime, timezone

# Setup script path imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import setup_logger
from scripts.constants import APPLE_PHOTOS_DB_LOCK_PATH, APPLE_PHOTOS_DB_PATH, BG_SERVICE_LOG_PATH

# Set up dedicated logger for the service
logger = setup_logger(BG_SERVICE_LOG_PATH, "bg_copy_db_service")

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
        logger.warning(f"Error reading lock file: {e}")
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
        logger.error(f"Error writing lock file: {e}")

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
        last_refresh_dt = datetime.strptime(last_refresh_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        last_refresh_ts = last_refresh_dt.timestamp()
        
        # 2.0-second tolerance threshold for filesystem comparison precision
        if src_mod_time > (last_refresh_ts + 2.0):
            src_utc_str = datetime.fromtimestamp(src_mod_time, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            return True, f"Source DB modified time ({src_utc_str} UTC) is newer than last refresh ({last_refresh_str} UTC)."
        else:
            return False, "Database copy is up to date."
    except Exception as e:
        return True, f"Failed to parse last refresh time: {e}"

def main():
    logger.info("🔄 Background database copy & sync service started.")
    
    # Check if another service instance is already running
    lock = read_lock_file()
    if lock:
        status = lock.get("status")
        lock_pid = lock.get("pid")
        if status == "refreshing" and is_pid_alive(lock_pid) and lock_pid != os.getpid():
            logger.info(f"ℹ️ Another background service is already running (PID: {lock_pid}). Exiting.")
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
                        logger.info(f"ℹ️ Planner is currently active (PID: {lock_pid}). Delaying database refresh...")
                        time.sleep(loop_interval)
                        continue
                    else:
                        logger.warning(f"⚠️ Found stale planner active lock from dead PID {lock_pid}. Overriding lock.")
                
                # 4. Acquire Lock
                current_pid = os.getpid()
                start_time_str = get_current_utc_str()
                logger.info(f"🔐 Refresh needed: {reason}")
                logger.info("🔐 Acquiring lock...")
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
                    logger.info(f"🚀 Running step: {step_name}")
                    
                    try:
                        p = subprocess.Popen(
                            [sys.executable, script_path] + args[1:],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            bufsize=1
                        )
                        
                        for line in p.stdout:
                            logger.info(line.rstrip('\r\n'))
                            
                        p.wait()
                        
                        if p.returncode != 0:
                            logger.error(f"❌ Step failed: {step_name} (Exit code {p.returncode})")
                            success = False
                            break
                        logger.info(f"✅ Step completed successfully: {step_name}")
                    except Exception as step_err:
                        logger.error(f"❌ Execution exception in step {step_name}: {step_err}")
                        success = False
                        break

                # 6. Release Lock
                logger.info("🔓 Releasing lock...")
                if success:
                    last_refresh_timestamp = get_current_utc_str()
                    logger.info("🎉 Database refresh and metadata sync completed successfully.")
                else:
                    logger.error("⚠️ Database copy and sync pipeline failed.")
                
                write_lock_file(
                    status="available",
                    pid=None,
                    latest_successful_refresh_utc=last_refresh_timestamp
                )
            else:
                logger.info(f"💤 Database copy is up to date (Last sync: {last_refresh_timestamp}). Sleeping {loop_interval}s...")
                
            time.sleep(loop_interval)
            
    except KeyboardInterrupt:
        logger.info("🛑 Background service stopped by user.")
        # Ensure we release lock if we hold it
        lock = read_lock_file()
        if lock and lock.get("pid") == os.getpid() and lock.get("status") == "refreshing":
            logger.info("🔓 Releasing lock before exit...")
            write_lock_file(
                status="available",
                pid=None,
                latest_successful_refresh_utc=lock.get("latest_successful_refresh_utc", "—")
            )
        return 0

if __name__ == "__main__":
    sys.exit(main())
