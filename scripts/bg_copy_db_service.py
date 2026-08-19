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
from scripts.constants import APPLE_PHOTOS_DB_LOCK_PATH, APPLE_PHOTOS_DB_PATH, BG_SERVICE_LOG_PATH, BG_SERVICE_PID_PATH

# Set up dedicated logger for the service
logger = setup_logger(BG_SERVICE_LOG_PATH, "bg_copy_db_service")

def check_and_write_service_pid():
    if os.path.exists(BG_SERVICE_PID_PATH):
        try:
            with open(BG_SERVICE_PID_PATH, "r") as f:
                pid = int(f.read().strip())
            if is_pid_alive(pid) and pid != os.getpid():
                logger.info(f"ℹ️ Background service is already running (PID: {pid}). Exiting.")
                sys.exit(0)
        except (ValueError, OSError):
            pass
            
    # Write current pid
    try:
        with open(BG_SERVICE_PID_PATH, "w") as f:
            f.write(str(os.getpid()))
        import atexit
        atexit.register(cleanup_service_pid)
        logger.info(f"📝 Wrote service PID file: {BG_SERVICE_PID_PATH} (PID: {os.getpid()})")
    except Exception as e:
        logger.error(f"Error writing service PID file: {e}")

def cleanup_service_pid():
    try:
        if os.path.exists(BG_SERVICE_PID_PATH):
            with open(BG_SERVICE_PID_PATH, "r") as f:
                pid = int(f.read().strip())
            if pid == os.getpid():
                os.remove(BG_SERVICE_PID_PATH)
                logger.info("🗑️ Removed service PID file.")
    except Exception as e:
        logger.warning(f"Error cleaning up service PID file: {e}")

def get_current_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

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
    check_and_write_service_pid()

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
                # 3. Check if planner or executor is active
                if status in ["planner_active", "executor_active"]:
                    if is_pid_alive(lock_pid):
                        friendly_name = "Planner" if status == "planner_active" else "Executor"
                        logger.info(f"ℹ️ {friendly_name} is currently active (PID: {lock_pid}). Delaying database refresh...")
                        time.sleep(loop_interval)
                        continue
                    else:
                        logger.warning(f"⚠️ Found stale {status} lock from dead PID {lock_pid}. Overriding lock.")
                
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
                    ("0.1 Storage Manager Migrations", ["storage_manager_main.py", "--migrate"]),
                    ("0.2 Sync Raw Assets", ["sync_photos_raw.py"]),
                    ("0.3 Sync Derived Metadata", ["sync_photos_derived.py", "--force"])
                ]
                
                success = True
                sync_start_time = time.time()
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
                    elapsed = time.time() - sync_start_time
                    logger.info(f"🎉 Database refresh and metadata sync completed successfully in {elapsed:.2f} seconds.")
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
