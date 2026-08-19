import sys
import os
import subprocess
import re
import time
import json
import socket
import errno
import atexit

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from utils.logger import setup_logger
from constants import LOG_PATH, STAGING_ROOT
from utils.utils import get_full_transition_path, human_readable_size
from google_photos import check_google_quota, authenticate, get_all_favorites
import argparse
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_LOCK_PATH, APPLE_PHOTOS_DB_PATH, LOG_PATH, GOOGLE_PHOTOS_READONLY_SCOPES, GOOGLE_DRIVE_READ_ONLY_SCOPES, PLANNER_REQUIRED_SCOPES, CURATION_THRESHOLD_LOG_PATH, SCORING_BREAKDOWN_LOG_PATH, MEDIA_CLEANUP_LOG_PATH, MAX_UPLOAD_FILE_SIZE_BYTES, MAX_UPLOAD_FILE_SIZE_MB, BG_SERVICE_PID_PATH
from constants import ACTIVE_CAMERA_MODELS, DEVICE_OWNER_MAPPING
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_stage_transitions, get_batch_statuses, get_latest_import_and_month
import requests
from datetime import timezone, datetime, timedelta
 

SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.heic', '.mov', '.mp4'}

logger = setup_logger(LOG_PATH, "pipeline_planner")
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

def set_planned_month(cursor, month):
    cursor.execute("SELECT id FROM planned_execution WHERE planned_month = ? AND active = 1", (month,))
    existing = cursor.fetchone()
    if existing:
        cursor.execute("UPDATE planned_execution SET set_at_utc = datetime('now') WHERE id = ?", (existing[0],))
        logger.info(f"Updated existing active plan for month {month} in queue (Queue ID: {existing[0]}).")
    else:
        cursor.execute("INSERT INTO planned_execution (planned_month, active, set_at_utc) VALUES (?, 1, datetime('now'))", (month,))
        logger.info(f"Added month {month} to planned_execution queue.")

def is_pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        return True

def read_lock_file():
    if not os.path.exists(APPLE_PHOTOS_DB_LOCK_PATH):
        return None
    try:
        with open(APPLE_PHOTOS_DB_LOCK_PATH, "r") as f:
            return json.load(f)
    except Exception:
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
        logger.warning(f"Failed to write lock file: {e}")

def release_planner_lock():
    lock = read_lock_file()
    if lock and lock.get("pid") == os.getpid() and lock.get("status") == "planner_active":
        logger.info("🔓 Releasing planner active lock.")
        write_lock_file(
            status="available",
            pid=None,
            latest_successful_refresh_utc=lock.get("latest_successful_refresh_utc", "—")
        )

def acquire_planner_lock():
    while True:
        lock = read_lock_file()
        last_refresh = "—"
        
        if lock:
            last_refresh = lock.get("latest_successful_refresh_utc", "—")
            status = lock.get("status")
            lock_pid = lock.get("pid")
            
            if status == "refreshing":
                if is_pid_alive(lock_pid):
                    print(f"\rℹ️  Apple Photos database copy is currently being refreshed in the background (PID: {lock_pid}). Waiting for lock release...", end="", flush=True)
                    time.sleep(10)
                    continue
                else:
                    print(f"\n⚠️ Found stale refreshing lock file from dead PID {lock_pid}. Overriding lock.")
            elif status == "planner_active":
                if is_pid_alive(lock_pid) and lock_pid != os.getpid():
                    logger.error(f"❌ Another instance of the pipeline planner is currently active (PID: {lock_pid}). Exiting to prevent DB write contention.")
                    sys.exit(1)
                elif lock_pid == os.getpid():
                    return
                else:
                    print(f"\n⚠️ Found stale planner active lock file from dead PID {lock_pid}. Overriding lock.")
        
        # Lock is available, acquire it
        print(f"\n🔐 Acquiring planner lock (PID: {os.getpid()}).")
        write_lock_file(
            status="planner_active",
            pid=os.getpid(),
            started_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            latest_successful_refresh_utc=last_refresh
        )
        atexit.register(release_planner_lock)
        break

def ensure_bg_service_running():
    service_running = False
    service_pid = None
    if os.path.exists(BG_SERVICE_PID_PATH):
        try:
            with open(BG_SERVICE_PID_PATH, "r") as f:
                service_pid = int(f.read().strip())
            if is_pid_alive(service_pid):
                service_running = True
        except (ValueError, OSError):
            pass

    if service_running:
        logger.info(f"ℹ️ Background sync service is running (PID: {service_pid}).")
        print(f"ℹ️ Background sync service is running (PID: {service_pid}).")
    else:
        logger.info("⚙️ Background sync service is not running. Starting it automatically...")
        print("⚙️ Background sync service is not running. Starting it automatically...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        service_script = os.path.join(script_dir, "bg_copy_db_service.py")
        try:
            # Spawn the background service in a detached process
            subprocess.Popen(
                [sys.executable, service_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            # Wait up to 2 seconds for it to create the PID file
            for _ in range(20):
                time.sleep(0.1)
                if os.path.exists(BG_SERVICE_PID_PATH):
                    try:
                        with open(BG_SERVICE_PID_PATH, "r") as f:
                            new_pid = int(f.read().strip())
                        if is_pid_alive(new_pid):
                            logger.info(f"✅ Started background sync service (PID: {new_pid}).")
                            print(f"✅ Started background sync service (PID: {new_pid}).")
                            return
                    except (ValueError, OSError):
                        pass
            logger.warning("⚠️ Background sync service was spawned but PID file could not be verified.")
            print("⚠️ Background sync service was spawned but PID file could not be verified.")
        except Exception as e:
            logger.error(f"❌ Failed to start background sync service: {e}")
            print(f"❌ Failed to start background sync service: {e}")

def check_if_refresh_needed():
    if not os.path.exists(APPLE_PHOTOS_DB_PATH):
        return
        
    src_mod_time = os.path.getmtime(APPLE_PHOTOS_DB_PATH)
    src_wal_path = APPLE_PHOTOS_DB_PATH + "-wal"
    if os.path.exists(src_wal_path):
        src_mod_time = max(src_mod_time, os.path.getmtime(src_wal_path))
        
    lock = read_lock_file()
    last_refresh_timestamp = 0
    last_refresh_str = "—"
    if lock and lock.get("latest_successful_refresh_utc") and lock.get("latest_successful_refresh_utc") != "—":
        try:
            last_refresh_str = lock.get("latest_successful_refresh_utc")
            last_refresh_dt = datetime.strptime(last_refresh_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            last_refresh_timestamp = last_refresh_dt.timestamp()
        except Exception:
            pass
            
    # Check if src_mod_time is newer than last successful refresh with a 2.0 second tolerance
    if src_mod_time > (last_refresh_timestamp + 2.0):
        src_utc_str = datetime.fromtimestamp(src_mod_time, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print("\n" + "!" * 100)
        print("⚠️  WARNING: Apple Photos database has new changes since the last sync.")
        print(f"   • Last Sync Time: {last_refresh_str} UTC")
        print(f"   • Source DB Time: {src_utc_str} UTC")
        
        service_pid = None
        if os.path.exists(BG_SERVICE_PID_PATH):
            try:
                with open(BG_SERVICE_PID_PATH, "r") as f:
                    service_pid = int(f.read().strip())
            except Exception:
                pass
                
        if service_pid and is_pid_alive(service_pid):
            print(f"ℹ️  Background sync service is running (PID: {service_pid}) and will automatically sync these changes.")
        else:
            print("👉 Please run 'python3 scripts/bg_copy_db_service.py' in a separate background window to refresh.")
        print("!" * 100 + "\n")

# Helper to run bootstrap steps
def run_bootstrap_steps(auto_apply, logger):
    """
    Run the bootstrap steps: only 1.0 Generate Batches is synchronous now.
    """
    steps = [
        ("1.0 Generate Batches", "generate_month_batches.py", [])
    ]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for step_name, script_file, step_args in steps:
        script_path = os.path.join(script_dir, script_file)
        logger.info(f"🔧 Running bootstrap step: {step_name} ({script_file})")
        try:
            subprocess.run([sys.executable, script_path] + step_args, check=True)
            logger.info(f"✅ Completed: {step_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error in bootstrap step {step_name}: {e}")
            sys.exit(1)

def print_assets_table(assets):
    """
    Prints a list of assets as a formatted table.
    Each asset is a tuple or list: (filename, date_created_utc, uuid)
    """
    if not assets:
        print("  No involved assets found.")
        return
    # Find max length of filenames for padding
    max_len = max(len(row[0]) if row[0] else 8 for row in assets)
    max_len = max(max_len, 8) # minimum width for "Filename"
    
    header = f"  {'Filename':<{max_len}} | {'Date Created':<19} | {'Asset UUID':<36}"
    separator = "-" * (max_len + 3 + 19 + 3 + 36)
    print(f"  {separator}")
    print(header)
    print(f"  {separator}")
    for fname, dt, uuid in assets:
        fname_str = fname if fname else "None"
        dt_str = dt if dt else "None"
        uuid_str = uuid if uuid else "None"
        print(f"  {fname_str:<{max_len}} | {dt_str:<19} | {uuid_str:<36}")
    print(f"  {separator}")

def prompt_asset_level_triage(cursor, conn, import_uuids, camera_model, camera_make, month):
    """
    Prompts the user to ignore assets one by one for a given import/month/camera.
    """
    placeholders = ','.join(['?' for _ in import_uuids])
    cursor.execute(f"""
        SELECT a.original_filename, a.date_created_utc, a.asset_id
        FROM assets a
        JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        WHERE a.import_id IN ({placeholders})
          AND a.month = ?
          AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = ?
          AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = ?
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
        ORDER BY a.date_created_utc
    """, import_uuids + [month, camera_model, camera_make])
    
    assets = cursor.fetchall()
    if not assets:
        print(f"No active assets found to triage for {camera_make} {camera_model} in {month}.")
        return True

    print(f"\n--- Asset-level Triage for {camera_make} {camera_model} ({month}) ---")
    ignored_count = 0
    for fname, dt, asset_id in assets:
        choice = input(f"  Ignore {fname} ({dt})? [y/N]: ").strip().lower()
        if choice == 'y':
            cursor.execute("UPDATE assets SET ignore_continuity_check = 1 WHERE asset_id = ?", (asset_id,))
            ignored_count += 1
            print(f"  ✅ Asset {fname} ignored.")
    
    if ignored_count > 0:
        cursor.execute(f"""
            UPDATE imports
            SET sequencing_confirmed = 1
            WHERE import_uuid IN ({placeholders})
              AND camera_model = ?
        """, import_uuids + [camera_model])
        conn.commit()
        print(f"\n✅ Ignored {ignored_count} asset(s). Continuing with planner...")
        return True
    else:
        print("\nNo assets were marked as ignored.")
        return False

def ignore_all_assets_for_batch(cursor, conn, import_uuids, camera_model, camera_make, month):
    """
    Marks all assets in the specified import sessions / camera model for the month as ignored for continuity checks.
    Also marks the import sessions as confirmed so planning can continue seamlessly without restarting.
    """
    placeholders = ','.join(['?' for _ in import_uuids])
    cursor.execute(f"""
        UPDATE assets
        SET ignore_continuity_check = 1
        WHERE import_id IN ({placeholders})
          AND month = ?
          AND (ignore_continuity_check = 0 OR ignore_continuity_check IS NULL)
          AND asset_id IN (
              SELECT za.ZUUID
              FROM ZASSET za
              LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
              WHERE za.ZIMPORTSESSION IN ({placeholders})
                AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = ?
                AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = ?
          )
    """, import_uuids + [month] + import_uuids + [camera_model, camera_make])
    count = cursor.rowcount

    cursor.execute(f"""
        UPDATE imports
        SET sequencing_confirmed = 1
        WHERE import_uuid IN ({placeholders})
          AND camera_model = ?
    """, import_uuids + [camera_model])

    conn.commit()
    logger.info(f"✅ Marked all {count} assets for {camera_make} {camera_model} ({month}) as ignored for continuity checks.")
    print(f"\n✅ Marked {count} asset(s) as ignored for {camera_model} in {month}. Continuing with planner...")
    return True

def handle_reasonability_rejection(cursor, conn, import_uuids, camera_model, camera_make, month, label=""):
    """
    Handles user rejecting reasonability for an import session or batch.
    Prompts the user to triage assets one by one or ignore the whole batch.
    """
    print(f"\n❌ Reasonability rejected for {camera_model or 'Unknown'}{f' ({label})' if label else ''}.")
    print("\nHow would you like to handle these assets?")
    print("  [1] Triage assets one by one (select individual assets to ignore)")
    print("  [2] Ignore the whole batch (ignore all involved assets for this month)")
    print("  [Q] Quit / abort execution")
    
    choice = input("\nSelection [1/2/Q]: ").strip().lower()
    
    if choice == '1':
        return prompt_asset_level_triage(cursor, conn, import_uuids, camera_model or "Unknown", camera_make or "Unknown", month)
    elif choice == '2':
        return ignore_all_assets_for_batch(cursor, conn, import_uuids, camera_model or "Unknown", camera_make or "Unknown", month)
    else:
        logger.error("Execution halted by user. Source data needs fixing.")
        close_conn()
        sys.exit(1)

def check_active_sources_import_status(cursor, conn, month, auto_apply):
    """
    Checks if all active camera models have imported assets for the proposed month.
    Prompts user if any active source is missing.
    """
    if not ACTIVE_CAMERA_MODELS:
        logger.info("No active camera models configured. Skipping active source check.")
        return True

    months_to_check = [month]

    try:
        cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
        logger.debug("Attached Photos.sqlite database read-only for active source check.")

        for month_str in months_to_check:
            source_metadata = []
            # We use conditional aggregation (CASE WHEN) to get the range for the target month 
            # while still being able to group by camera model.
            query = """
                SELECT 
                    xa.ZCAMERAMODEL,
                    xa.ZCAMERAMAKE,
                    COUNT(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN 1 END) AS assets_in_month,
                    MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN aaa.ZORIGINALFILENAME END) AS min_filename,
                    MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN aaa.ZORIGINALFILENAME END) AS max_filename,
                    MIN(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime') END) AS min_date,
                    MAX(CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? THEN datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime') END) AS max_date,
                    GROUP_CONCAT(DISTINCT CASE WHEN strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ? 
                                               THEN a.ZIMPORTSESSION END) AS involved_import_ids
                FROM photos_db.ZASSET a
                JOIN photos_db.ZEXTENDEDATTRIBUTES xa ON xa.ZASSET = a.Z_PK
                JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                JOIN imports i ON i.import_uuid = a.ZIMPORTSESSION 
                              AND i.camera_model = xa.ZCAMERAMODEL
                LEFT JOIN assets loc ON loc.asset_id = a.ZUUID
                WHERE a.ZTRASHEDSTATE = 0
                  AND (loc.ignore_continuity_check = 0 OR loc.ignore_continuity_check IS NULL)
                  AND xa.ZCAMERAMODEL IN ({})
                  AND a.ZDATECREATED >= (strftime('%s', date(?, 'start of month', '-12 month')) - 978307200)
                  AND a.ZDATECREATED < (strftime('%s', date(?, 'start of month', '+2 month')) - 978307200)
                GROUP BY xa.ZCAMERAMODEL, xa.ZCAMERAMAKE
            """.format(','.join(['?' for _ in ACTIVE_CAMERA_MODELS]))

            cursor.execute(query, [month_str] * 6 + ACTIVE_CAMERA_MODELS + [month_str + "-01", month_str + "-01"])
            results = cursor.fetchall()
            found_models = set()

            for row in results:
                model, make, count, f_min, f_max, d_min, d_max, involved_import_ids = row
                num_min = None
                num_max = None
                gap_info = ""
                if count > 0:
                    found_models.add(model)

                    # Reasonability check: parse numeric part from filenames (ignoring extensions)
                    # We only attempt this if the filename looks like a standard sequential pattern (Prefix + Digits)
                    seq_pattern = r'^([a-zA-Z_-]+)(\d+)$'
                    if f_min:
                        m = re.match(seq_pattern, os.path.splitext(f_min)[0])
                        if m: num_min = int(m.group(2))
                    if f_max:
                        m = re.match(seq_pattern, os.path.splitext(f_max)[0])
                        if m: num_max = int(m.group(2))

                    if num_min is not None and num_max is not None:
                        # We use abs because string MIN/MAX might flip if sequence is not zero-padded
                        expected_range = abs(num_max - num_min) + 1
                        if expected_range > count:
                            gap_info = f" | ⚠️ Reasonability: {expected_range} expected vs {count} found (gap of {expected_range - count})"

                # Continuity check with previous month's confirmed imports
                continuity_info = ""
                previous_month = (datetime.strptime(month_str, '%Y-%m').replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
                cursor.execute("""
                    SELECT MAX(max_filename), MAX(max_date)
                    FROM imports
                    WHERE camera_model = ? AND months_detected LIKE ? AND sequencing_confirmed = 1
                """, (model, f'%{previous_month}%')) # Use LIKE for months_detected as it's comma-separated
                prev_month_data = cursor.fetchone()
                prev_max_filename, prev_max_date = prev_month_data if prev_month_data else (None, None)

                if prev_max_filename and num_min is not None:
                    prev_num_max = None
                    prev_nums = re.findall(r'(\d+)', os.path.splitext(prev_max_filename)[0])
                    if prev_nums: prev_num_max = int(prev_nums[-1])

                    if prev_num_max is not None and num_min > prev_num_max + 1:
                        continuity_info += f" | ⚠️ Filename gap from {previous_month}: {prev_max_filename} -> {f_min}"
                    elif prev_num_max is not None and num_min <= prev_num_max:
                        continuity_info += f" | ⚠️ Filename overlap/reset from {previous_month}: {prev_max_filename} -> {f_min}"
                
                if prev_max_date and d_min:
                    # Simple date string comparison for YYYY-MM-DD HH:MM:SS
                    if d_min < prev_max_date:
                        continuity_info += f" | ⚠️ Date overlap from {previous_month}: {prev_max_date} -> {d_min}"

                logger.info(f"📸 Month: {month_str} | Source: {model:20} | Count: {count:4} | Files: {f_min} -> {f_max} | Dates: {d_min} to {d_max}{gap_info}{continuity_info}")
                source_metadata.append({
                    'row': row,
                    'gap_info': gap_info,
                    'continuity_info': continuity_info
                })

            missing_models = set(ACTIVE_CAMERA_MODELS) - found_models

            if missing_models:
                logger.warning(f"⚠️ Missing imports for active camera models in {month_str}: {', '.join(missing_models)}")
                if not auto_apply:
                    proceed_input = input(f"Some active sources are missing imports for {month_str}. Do you want to proceed? [y/N]: ")
                    if proceed_input.strip().lower() != 'y':
                        logger.info("Operation aborted by user due to missing active source imports.")
                        sys.exit(0)
                else:
                    logger.error(f"❌ Auto-apply aborted: Missing active source imports for {month_str}. Manual intervention required.")
                    sys.exit(1)

            # Per-source sequencing confirmation
            if not auto_apply:
                for entry in source_metadata:
                    row = entry['row']
                    gap_info = entry['gap_info']
                    continuity_info = entry['continuity_info']
                    model, make, count, f_min, f_max, d_min, d_max, involved_import_ids = row

                    if count == 0 or not involved_import_ids:
                        continue
                    
                    # Extract individual import IDs from the concatenated string
                    import_id_list = involved_import_ids.split(',')
                    placeholders = ','.join(['?' for _ in import_id_list])
                    
                    # Check which of these involved imports are still unconfirmed in our local table
                    cursor.execute("""
                        SELECT COUNT(*) FROM imports
                        WHERE import_uuid IN ({}) AND camera_model = ?
                          AND (sequencing_confirmed = 0 OR sequencing_confirmed IS NULL)
                    """.format(placeholders), import_id_list + [model])
                    unconfirmed_count = cursor.fetchone()[0]

                    #   TODO: Before the promt we should check confirmed months for each source in comparison to months in the past or in the future relative to the proposed month
                    if unconfirmed_count > 0:
                        # Determine naming pattern to filter context to relevant conventions
                        pattern = "*"
                        if f_min:
                            # Only use a prefix filter if it looks like a standard sequence (Prefix + Digits)
                            stem = os.path.splitext(f_min)[0]
                            m = re.match(r'^([a-zA-Z_-]+)\d+$', stem)
                            if m:
                                pattern = m.group(1) + "*"

                        # Fetch global boundaries for this model before and after the current month
                        cursor.execute("""
                            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
                            FROM imports
                            WHERE camera_model = ? AND max_date < ? AND min_filename GLOB ?
                        """, (model, f"{month_str}-01 00:00:00", pattern))
                        b = cursor.fetchone()
                        before_str = f"  Before:  {b[0]} -> {b[1]} ({b[2]} to {b[3]})" if b and b[1] else "  Before:  None"

                        cursor.execute("""
                            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
                            FROM imports
                            WHERE camera_model = ? AND min_date >= date(?, 'start of month', '+1 month') AND min_filename GLOB ?
                        """, (model, f"{month_str}-01", pattern))
                        a = cursor.fetchone()
                        after_str = f"  After:   {a[0]} -> {a[1]} ({a[2]} to {a[3]})" if a and a[0] else "  After:   None"

                        # Fetch involved assets to print table before prompt
                        cursor.execute(f"""
                            SELECT aaa.ZORIGINALFILENAME, datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime'), a.ZUUID
                            FROM photos_db.ZASSET a
                            JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                            LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
                            WHERE a.ZIMPORTSESSION IN ({placeholders})
                              AND COALESCE(ea.ZCAMERAMODEL, 'Unknown') = ?
                              AND COALESCE(ea.ZCAMERAMAKE, 'Unknown') = ?
                              AND strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ?
                            ORDER BY aaa.ZORIGINALFILENAME
                        """, import_id_list + [model, make or "Unknown", month_str])
                        involved_assets = cursor.fetchall()

                        print(f"Verifying {model} for {month_str}:")
                        print(before_str)
                        print(f"  Current: {f_min} -> {f_max} ({d_min} to {d_max}){gap_info}{continuity_info}")
                        print(after_str)
                        print(f"\n  Involved assets ({len(involved_assets)} items):")
                        print_assets_table(involved_assets)

                        choice = input(
                            f"Mark as reasonable? [I/n]: "
                        ).strip().upper()

                        if choice == 'I':
                            for import_uuid in import_id_list:
                                # Calculate metadata specific to this individual import_uuid for the month
                                cursor.execute("""
                                    SELECT 
                                        MIN(aaa.ZORIGINALFILENAME),
                                        MAX(aaa.ZORIGINALFILENAME),
                                        MIN(datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
                                        MAX(datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime'))
                                    FROM photos_db.ZASSET a
                                    JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                                    LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
                                    LEFT JOIN assets loc ON loc.asset_id = a.ZUUID
                                    WHERE a.ZIMPORTSESSION = ?
                                      AND (loc.ignore_continuity_check = 0 OR loc.ignore_continuity_check IS NULL)
                                      AND strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) = ?
                                      AND COALESCE(ea.ZCAMERAMODEL, 'Unknown') = ?
                                      AND COALESCE(ea.ZCAMERAMAKE, 'Unknown') = ?
                                """, (import_uuid, month_str, model, make or "Unknown"))
                                res = cursor.fetchone()
                                if res:
                                    s_f_min, s_f_max, s_d_min, s_d_max = res
                                    cursor.execute("""
                                        UPDATE imports 
                                        SET sequencing_confirmed = 1,
                                            min_filename = COALESCE(min_filename, ?),
                                            max_filename = COALESCE(max_filename, ?),
                                            min_date = COALESCE(min_date, ?),
                                            max_date = COALESCE(max_date, ?)
                                    WHERE import_uuid = ? AND camera_model = ?
                                """, (s_f_min, s_f_max, s_d_min, s_d_max, import_uuid, model))
                            conn.commit()
                            logger.info(f"✅ Marked involved imports for {model} in {month_str} as reasonable and updated metadata individually.")
                        else:
                            handle_reasonability_rejection(
                                cursor, conn, import_id_list, model, make or "Unknown", month_str, label=f"month {month_str}"
                            )
    finally:
        cursor.execute("DETACH DATABASE photos_db;")
        logger.debug("Detached Photos.sqlite database.")
    return True

def check_favorites_count(cursor, month, check_remote=False, all_favs=None, creds=None, verbose=True):
    """
    Checks for favorites in local DB or optionally Google Photos API.
    Used to verify readiness for manual transitions or pull/ranking steps.
    """
    cursor.execute("SELECT original_filename FROM assets WHERE month = ? AND google_favorite = 1", (month,))
    local_fav_names = [row[0] for row in cursor.fetchall()]
    local_count = len(local_fav_names)
    
    if local_count > 0 or not check_remote:
        if verbose:
            logger.info(f"📊 Favorites check for {month}: Found {local_count} starred assets in local database.")
        return local_count, "local", local_fav_names
        
    try:
        if verbose: logger.info(f"🌐 Local database has 0 favorites for {month}. Calling Google Photos API to verify curation status...")
        if all_favs is None:
            if creds is None:
                creds = authenticate(scopes=GOOGLE_PHOTOS_READONLY_SCOPES)
            all_favs = get_all_favorites(creds)
        else:
            if verbose: logger.info(f"Using {len(all_favs)} cached favorites from current session.")
        if verbose: logger.info(f"✅ API Response: {len(all_favs)} total favorites retrieved from account.")
        
        cursor.execute("SELECT original_filename, date_created_utc FROM assets WHERE month = ?", (month,))
        local_assets = cursor.fetchall()
        
        fav_signatures = set()
        for f in all_favs:
            fname = f.get('filename')
            q_time = f.get('mediaMetadata', {}).get('creationTime', '')
            if fname and q_time:
                ts = q_time.replace('T', ' ').split('.')[0]
                fav_signatures.add((fname, ts))
        
        remote_count = 0
        matched_files = []
        for fname, ts in local_assets:
            if (fname, ts) in fav_signatures:
                remote_count += 1
                matched_files.append(fname)
        if matched_files and verbose:
            logger.info(f"✨ Successfully matched remote favorites: {matched_files}")
        if verbose:
            logger.info(f"📊 Cross-reference result for {month}: Found {remote_count} assets matching global favorites list.")
        return remote_count, "remote", matched_files
    except Exception as e:
        logger.warning(f"Could not verify remote favorites: {e}")
        return 0, "error", []

def verify_sequencing_for_planned_month(cursor, conn, month, auto_apply):
    """
    Checks if imports associated with the planned month have sequencing confirmed.
    Prompts the user if confirmation is missing.
    """
    cursor.execute("""
        SELECT DISTINCT i.import_uuid, i.camera_model, i.camera_make
        FROM imports i
        JOIN ZASSET za ON za.ZIMPORTSESSION = i.import_uuid
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        LEFT JOIN assets a ON a.asset_id = za.ZUUID
        WHERE a.month = ?
          AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(i.camera_model, 'Unknown')
          AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(i.camera_make, 'Unknown')
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
          AND (i.sequencing_confirmed = 0 OR i.sequencing_confirmed IS NULL)
    """, (month,))
    unconfirmed = cursor.fetchall()

    if not unconfirmed:
        return True

    logger.info(f"🧐 Found {len(unconfirmed)} import sessions for {month} requiring sequencing confirmation.")
    for uuid, model, make in unconfirmed:
        # Recalculate metrics based on non-ignored assets matching this specific import row's camera
        cursor.execute("""
            SELECT MIN(aaa.ZORIGINALFILENAME), MAX(aaa.ZORIGINALFILENAME), COUNT(za.Z_PK),
                   MIN(datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
                   MAX(datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime'))
            FROM ZASSET za
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
            LEFT JOIN assets a ON a.asset_id = za.ZUUID
            WHERE za.ZIMPORTSESSION = ?
              AND a.month = ?
              AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(?, 'Unknown')
              AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(?, 'Unknown')
              AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
        """, (uuid, month, model, make))
        
        f_min, f_max, count, d_min, d_max = cursor.fetchone()

        if not count:
            continue

        if not model:
            model = "Unknown Model"

        # Reasonability check: parse numeric part from filenames
        seq_pattern = r'^([a-zA-Z_-]+)(\d+)$'
        num_min = None
        if f_min:
            m = re.match(seq_pattern, os.path.splitext(f_min)[0])
            if m: num_min = int(m.group(2))
            
        num_max = None
        if f_max:
            m = re.match(seq_pattern, os.path.splitext(f_max)[0])
            if m: num_max = int(m.group(2))

        gap_str = ""
        if num_min is not None and num_max is not None:
            expected = abs(num_max - num_min) + 1
            gap = expected - count if expected > count else 0
            if gap > 0:
                gap_str = f" | ⚠️ Gap detected: {gap} items"
        logger.info(f"   - Session {uuid} ({model}): {f_min} -> {f_max} ({d_min} to {d_max}) ({count} files){gap_str}")

        if auto_apply:
            continue

        # Determine naming pattern to filter context to relevant conventions
        pattern = "*"
        if f_min:
            # Only use a prefix filter if it looks like a standard sequence (Prefix + Digits)
            stem = os.path.splitext(f_min)[0]
            m = re.match(r'^([a-zA-Z_-]+)\d+$', stem)
            if m:
                pattern = m.group(1) + "*"

        # Fetch global boundaries for this model before and after the current month
        cursor.execute("""
            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
            FROM imports
            WHERE camera_model = ? AND max_date < ? AND min_filename GLOB ?
        """, (model, f"{month}-01 00:00:00", pattern))
        b = cursor.fetchone()
        before_str = f"  Before:  {b[0]} -> {b[1]} ({b[2]} to {b[3]})" if b and b[1] else "  Before:  None"

        cursor.execute("""
            SELECT MIN(min_filename), MAX(max_filename), MIN(min_date), MAX(max_date)
            FROM imports
            WHERE camera_model = ? AND min_date >= date(?, 'start of month', '+1 month') AND min_filename GLOB ?
        """, (model, f"{month}-01", pattern))
        a = cursor.fetchone()
        after_str = f"  After:   {a[0]} -> {a[1]} ({a[2]} to {a[3]})" if a and a[0] else "  After:   None"

        # Fetch involved assets to print table before prompt
        cursor.execute("""
            SELECT aaa.ZORIGINALFILENAME, datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime'), za.ZUUID
            FROM ZASSET za
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
            LEFT JOIN assets a ON a.asset_id = za.ZUUID
            WHERE za.ZIMPORTSESSION = ?
              AND COALESCE(zea.ZCAMERAMODEL, 'Unknown') = COALESCE(?, 'Unknown')
              AND COALESCE(zea.ZCAMERAMAKE, 'Unknown') = COALESCE(?, 'Unknown')
              AND a.month = ?
              AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
            ORDER BY aaa.ZORIGINALFILENAME
        """, (uuid, model, make, month))
        involved_assets = cursor.fetchall()

        print(f"Verifying {model} session {uuid} for {month}:")
        print(before_str)
        print(f"  Current: {f_min} -> {f_max} ({d_min} to {d_max}) ({count} files){gap_str}")
        print(after_str)
        print(f"\n  Involved assets ({len(involved_assets)} items):")
        print_assets_table(involved_assets)

        choice = input(
            f"Mark as reasonable? [I/n]: "
        ).strip().upper()

        if choice == 'I':
            # Calculate missing metadata from assets table
            cursor.execute("""
                SELECT MIN(original_filename), MAX(original_filename), MIN(date_created_utc), MAX(date_created_utc)
                FROM assets WHERE import_id = ?
            """, (uuid,))
            calc_f_min, calc_f_max, calc_d_min, calc_d_max = cursor.fetchone()

            cursor.execute("""
                UPDATE imports 
                SET sequencing_confirmed = 1,
                    min_filename = COALESCE(min_filename, ?),
                    max_filename = COALESCE(max_filename, ?),
                    min_date = COALESCE(min_date, ?),
                    max_date = COALESCE(max_date, ?)
                WHERE import_uuid = ? AND camera_model = ?
            """, (calc_f_min, calc_f_max, calc_d_min, calc_d_max, uuid, model))
            conn.commit()
            logger.info(f"✅ Marked import {uuid} for {model} as reasonable and updated metadata.")
        else:
            handle_reasonability_rejection(
                cursor, conn, [uuid], model or "Unknown", make or "Unknown", month, label=f"session {uuid}"
            )

    if auto_apply:
        return True

    # Final check: are there any remaining unconfirmed sessions for this month?
    cursor.execute("""
        SELECT COUNT(*)
        FROM assets a
        JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        JOIN imports i ON i.import_uuid = a.import_id 
          AND COALESCE(i.camera_model, 'Unknown') = COALESCE(zea.ZCAMERAMODEL, 'Unknown')
        WHERE a.month = ? 
          AND (a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL)
          AND (i.sequencing_confirmed = 0 OR i.sequencing_confirmed IS NULL)
    """, (month,))
    remaining = cursor.fetchone()[0]
    return remaining == 0


def display_summary(transitions, batches, cursor, remote_favs_cache=None):
    print("\n=== 📊 Stage Transitions ===")
    for code, prev, desc, ttype, label in transitions:
        print(f"{prev} ➜ {code}: {desc} (Type: {ttype})")

    print("\n=== 📦 Batch Statuses ===")
    for month, status in batches:
        print(f"Month: {month}, Status: {status}")

    if remote_favs_cache:
        # Build a lookup of (filename, timestamp) -> local batch month
        cursor.execute("SELECT original_filename, date_created_utc, month FROM assets")
        local_mapping = {(row[0], row[1]): row[2] for row in cursor.fetchall()}
        
        fav_counts = {}
        for item in remote_favs_cache:
            fname = item.get('filename')
            creation_time = item.get('mediaMetadata', {}).get('creationTime')
            
            if creation_time:
                # Convert Google format '2026-04-18T23:00:00Z' to local format '2026-04-18 23:00:00'
                ts = creation_time.replace('T', ' ').split('.')[0]
                
                # Group by the local batch month if the asset is recognized, 
                # otherwise fallback to Google's raw month metadata.
                month_key = local_mapping.get((fname, ts), creation_time[:7])
                fav_counts[month_key] = fav_counts.get(month_key, 0) + 1
        
        if fav_counts:
            print("\n=== ⭐ Remote Favorites Matched to Local Batches ===")
            for month in sorted(fav_counts.keys(), reverse=True):
                print(f"Month: {month}, Favorites: {fav_counts[month]}")

def run_memory_publishing_flow(cursor=None, conn=None):
    logger.info("🎨 Starting Memory Feature & Publishing session...")
    from constants import CURATED_LACIE_DIR, TO_BE_CURATED_DIR
    import math

    # Acquire lock and get connection for initialization
    acquire_planner_lock()
    init_conn = get_connection()
    init_conn.execute("PRAGMA busy_timeout = 30000")
    init_cursor = get_cursor()

    # Create threshold_history table if it doesn't exist
    try:
        init_cursor.execute("""
            CREATE TABLE IF NOT EXISTS threshold_history (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at_utc    TEXT NOT NULL DEFAULT (datetime('now')),
                threshold_score     REAL NOT NULL,
                notes               TEXT
            )
        """)
        init_conn.commit()
    except Exception as e:
        logger.warning(f"Could not initialize threshold_history table: {e}")

    # Fetch historical minimum threshold
    historical_min = 0.0
    try:
        init_cursor.execute("SELECT MIN(threshold_score) FROM threshold_history WHERE threshold_score > 0.0")
        row = init_cursor.fetchone()
        if row and row[0] is not None:
            historical_min = row[0]
            logger.info(f"Loaded historical minimum threshold from DB: {historical_min:.4f}")
    except Exception as e:
        logger.warning(f"Could not fetch historical minimum threshold: {e}")

    # Release for the first menu listing/user input wait
    close_conn()
    release_planner_lock()
    
    while True:
        acquire_planner_lock()
        conn = get_connection()
        conn.execute("PRAGMA busy_timeout = 30000")
        cursor = get_cursor()

        displayed_moments_map = {}
        # Clear/rollback any open transactions to get a fresh snapshot of the database
        try:
            conn.rollback()
        except Exception:
            pass

        # Try attaching Apple Photos DB copy to fetch Apple's auto-generated moments and filter ignored items
        photos_db_attached = False
        try:
            cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
            photos_db_attached = True
        except Exception as e:
            logger.warning(f"Could not attach Photos.sqlite for Apple moment lookup: {e}")

        # Fetch the cutoff threshold score (dynamically on each loop iteration, excluding Ignore folder items)
        cutoff_score = 0.0
        if photos_db_attached:
            try:
                cursor.execute("""
                    SELECT v.score_normalized FROM ranked_assets_view v
                    JOIN month_batches mb ON v.month = mb.month
                    LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                    WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '') 
                      AND (a.Z_PK IS NULL OR NOT EXISTS (
                          SELECT 1 FROM photos_db.Z_30ASSETS aa
                          JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                          WHERE aa.Z_3ASSETS = a.Z_PK
                            AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                            AND ga.ZTRASHEDSTATE = 0
                      ))
                    ORDER BY v.score_normalized DESC LIMIT 1
                """)
                row = cursor.fetchone()
                cutoff_score = row[0] if row and row[0] is not None else 0.0
            except Exception as e:
                logger.warning(f"Error querying cutoff score with photos_db: {e}")
        
        if cutoff_score == 0.0:
            try:
                cursor.execute("""
                    SELECT v.score_normalized FROM ranked_assets_view v
                    JOIN month_batches mb ON v.month = mb.month
                    WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '') 
                    ORDER BY v.score_normalized DESC LIMIT 1
                """)
                row = cursor.fetchone()
                cutoff_score = row[0] if row and row[0] is not None else 0.0
            except Exception:
                pass
                
        logger.info(f"Cutoff threshold score: {cutoff_score:.4f}")

        # Record cutoff score in threshold_history if it is a valid positive value
        if cutoff_score > 0.0:
            try:
                cursor.execute("INSERT INTO threshold_history (threshold_score) VALUES (?)", (cutoff_score,))
                conn.commit()
                # Update running historical_min if this is the first recorded threshold or it is smaller
                if historical_min == 0.0 or cutoff_score < historical_min:
                    historical_min = cutoff_score
            except Exception as e:
                logger.warning(f"Could not record threshold in history: {e}")

        # Build and write Curation Threshold Status & Unassigned High-Rank Assets report to dedicated log
        threshold_report = []
        threshold_report.append("==================================================")
        threshold_report.append("📊 Curation Threshold Status")
        threshold_report.append("==================================================")
        threshold_report.append(f" - Current dynamic threshold:  {cutoff_score:.4f}")
        if historical_min > 0.0:
            threshold_report.append(f" - Historical minimum target:  {historical_min:.4f}")
            if cutoff_score > historical_min:
                threshold_report.append(f"👉 Note: Please assign moments to assets in new batches until the threshold reaches {historical_min:.4f} again.")
            else:
                threshold_report.append("🎉 Threshold aligned! Current threshold matches or is below historical minimum.")
        else:
            threshold_report.append(" - Historical minimum target:  None (No history recorded yet)")
            threshold_report.append("👉 Note: Once you begin assigning moments, the lowest dynamic threshold reached will be tracked.")
        threshold_report.append("==================================================\n")

        # Determine effective cutoff threshold to use for selecting qualified moments in the table
        effective_threshold = cutoff_score
        if historical_min > 0.0:
            effective_threshold = min(cutoff_score, historical_min) if cutoff_score > 0.0 else historical_min

        # Check for highly ranked assets that do not belong to any Moment in Apple Photos (excluding Ignore items)
        if photos_db_attached:
            cursor.execute("""
                SELECT 
                    v.original_filename, 
                    v.score_normalized, 
                    v.month, 
                    v.date_created_utc,
                    m.ZTITLE,
                    m.ZSUBTITLE
                FROM ranked_assets_view v
                JOIN month_batches mb ON v.month = mb.month
                LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                LEFT JOIN photos_db.ZMOMENT m ON a.ZMOMENT = m.Z_PK
                WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '')
                  AND v.score_normalized > 0.50
                  AND (a.Z_PK IS NULL OR NOT EXISTS (
                      SELECT 1 FROM photos_db.Z_30ASSETS aa
                      JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                      WHERE aa.Z_3ASSETS = a.Z_PK
                        AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                        AND ga.ZTRASHEDSTATE = 0
                  ))
                ORDER BY v.score_normalized DESC
                LIMIT 10
            """)
        else:
            cursor.execute("""
                SELECT v.original_filename, v.score_normalized, v.month, v.date_created_utc, NULL, NULL
                FROM ranked_assets_view v
                JOIN month_batches mb ON v.month = mb.month
                WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '')
                  AND v.score_normalized > 0.50
                ORDER BY v.score_normalized DESC
                LIMIT 10
            """)

        unassigned = cursor.fetchall()

        if unassigned:
            threshold_report.append("==================================================")
            threshold_report.append("⚠️  Unassigned High-Rank Assets (Need Moment Naming Decision)")
            threshold_report.append("==================================================")
            threshold_report.append("The following highly-ranked assets are not assigned to any Moment album in Apple Photos:")
            for fname, score, month, date_created, moment_title, moment_subtitle in unassigned:
                captured_str = "—"
                if date_created:
                    try:
                        dt_utc = None
                        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                            try:
                                dt_utc = datetime.strptime(date_created, fmt)
                                break
                            except ValueError:
                                continue
                        if dt_utc:
                            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                            dt_local = dt_utc.astimezone()
                            captured_str = dt_local.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            captured_str = date_created[:19]
                    except Exception:
                        captured_str = date_created[:19]

                moment_parts = []
                if moment_title:
                    moment_parts.append(moment_title.replace('\xa0', ' ').strip())
                if moment_subtitle:
                    moment_parts.append(moment_subtitle.replace('\xa0', ' ').strip())

                suggested_info = ""
                if moment_parts:
                    captured_date = captured_str[:10] if captured_str != "—" else (date_created[:10] if date_created else month)
                    suggested_name = f"{captured_date} - {' - '.join(moment_parts)}"
                    suggested_info = f", Suggested Album: {suggested_name}"

                threshold_report.append(f" - {fname:<25} (Score: {score:.4f}, Captured: {captured_str}, Month: {month}{suggested_info})")
            threshold_report.append("👉 Please consider creating a corresponding album under 'Media Organizer on LaCie / Moments' in Apple Photos (creating the album is sufficient, no need to place the files inside).\n")

        # Query published moments / folders with stats
        cursor.execute("""
            SELECT 
                p.moment_name,
                MAX(p.published_at_utc) AS last_published_at,
                COUNT(DISTINCT p.asset_id) AS published_count,
                AVG(v.score_normalized) AS avg_score,
                MIN(v.score_normalized) AS min_score,
                MAX(v.score_normalized) AS max_score,
                MIN(a.date_created_utc) AS min_captured,
                MAX(a.date_created_utc) AS max_captured,
                GROUP_CONCAT(DISTINCT COALESCE(zea.ZCAMERAMODEL, i.camera_model, 'Unknown')) AS camera_sources,
                GROUP_CONCAT(DISTINCT p.platform) AS platforms
            FROM publications p
            JOIN assets a ON p.asset_id = a.asset_id
            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
            LEFT JOIN imports i ON a.import_id = i.import_uuid
            LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
            GROUP BY p.moment_name
            ORDER BY MAX(p.published_at_utc) DESC, p.moment_name ASC
        """)
        published_folders = cursor.fetchall()

        threshold_report.append("==================================================================================================================================")
        threshold_report.append("🌟 Published Moments / Folders & Stats")
        threshold_report.append("==================================================================================================================================")
        if not published_folders:
            threshold_report.append("ℹ️  No published moments recorded yet in database.\n")
        else:
            threshold_report.append("The following moments have been curated and published:")
            pub_header = f"{'No.':<4} {'Moment Name':<30} {'Published At (Local)':<22} {'Assets':<8} {'Avg Score':<11} {'Score Range':<17} {'Capture Dates':<24} {'Camera Sources'}"
            threshold_report.append(pub_header)
            threshold_report.append("-" * len(pub_header))
            for idx, p_row in enumerate(published_folders, 1):
                p_name = p_row[0] or "—"
                p_date_raw = p_row[1]
                p_date_str = "—"
                if p_date_raw:
                    try:
                        dt_utc = None
                        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                            try:
                                dt_utc = datetime.strptime(p_date_raw, fmt)
                                break
                            except ValueError:
                                continue
                        if dt_utc:
                            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                            dt_local = dt_utc.astimezone()
                            p_date_str = dt_local.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            p_date_str = p_date_raw[:19]
                    except Exception:
                        p_date_str = p_date_raw[:19]

                p_count = str(p_row[2])
                p_avg = f"{p_row[3]:.4f}" if p_row[3] is not None else "—"
                p_min = f"{p_row[4]:.4f}" if p_row[4] is not None else "—"
                p_max = f"{p_row[5]:.4f}" if p_row[5] is not None else "—"
                score_rng = f"{p_min} - {p_max}" if p_row[4] is not None else "—"
                d_min = (p_row[6][:10] if p_row[6] else "—")
                d_max = (p_row[7][:10] if p_row[7] else "—")
                date_rng = f"{d_min} to {d_max}" if d_min != d_max else d_min
                c_srcs = (p_row[8] or "Unknown").replace(',', ', ')
                threshold_report.append(f"{idx:<4} {p_name:<30} {p_date_str:<22} {p_count:<8} {p_avg:<11} {score_rng:<17} {date_rng:<24} {c_srcs}")
            threshold_report.append("==================================================================================================================================\n")

        # Save to logs/curation_threshold_status.log and print to console
        try:
            with open(CURATION_THRESHOLD_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(threshold_report) + '\n')
        except Exception as e:
            logger.warning(f"Could not write curation threshold log: {e}")

        print('\n' + '\n'.join(threshold_report))

        # 2. Query assets that have Moments and are in status >= 600
        if photos_db_attached:
            query = """
                SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, v.original_filename,
                       v.aesthetic_score, v.google_favorite, v.mobile_apple_photos_featured_photos, v.apple_photos_monthly_selection,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'to_be_curated') as is_proposed,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'curated') as is_curated,
                       (SELECT album_name FROM moment_exports me WHERE me.asset_id = v.asset_id ORDER BY exported_at_utc DESC LIMIT 1) as exported_album_name,
                       ast.curated_album,
                       (SELECT 1 FROM publications p WHERE p.asset_id = v.asset_id LIMIT 1) as is_published
                FROM ranked_assets_view v
                JOIN assets ast ON v.asset_id = ast.asset_id
                JOIN month_batches mb ON v.month = mb.month
                LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                WHERE mb.status_code >= '600' AND v.MomentsAlbumName IS NOT NULL AND v.MomentsAlbumName != ''
                  AND LOWER(v.MomentsAlbumName) NOT IN ('skippublishing', 'ignore')
                  AND v.score_normalized > ?
                  AND (a.Z_PK IS NULL OR NOT EXISTS (
                      SELECT 1 FROM photos_db.Z_30ASSETS aa
                      JOIN photos_db.ZGENERICALBUM ga ON aa.Z_30ALBUMS = ga.Z_PK
                      WHERE aa.Z_3ASSETS = a.Z_PK
                        AND LOWER(ga.ZTITLE) IN ('ignore', 'skippublishing')
                        AND ga.ZTRASHEDSTATE = 0
                  ))
                ORDER BY v.score_normalized DESC
            """
        else:
            query = """
                SELECT v.asset_id, v.MomentsAlbumName, v.score_normalized, v.original_filename,
                       v.aesthetic_score, v.google_favorite, v.mobile_apple_photos_featured_photos, v.apple_photos_monthly_selection,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'to_be_curated') as is_proposed,
                       (SELECT 1 FROM moment_exports me WHERE me.asset_id = v.asset_id AND me.curation_stage = 'curated') as is_curated,
                       (SELECT album_name FROM moment_exports me WHERE me.asset_id = v.asset_id ORDER BY exported_at_utc DESC LIMIT 1) as exported_album_name,
                       ast.curated_album,
                       (SELECT 1 FROM publications p WHERE p.asset_id = v.asset_id LIMIT 1) as is_published
                FROM ranked_assets_view v
                JOIN assets ast ON v.asset_id = ast.asset_id
                JOIN month_batches mb ON v.month = mb.month
                WHERE mb.status_code >= '600' AND v.MomentsAlbumName IS NOT NULL AND v.MomentsAlbumName != ''
                  AND LOWER(v.MomentsAlbumName) NOT IN ('skippublishing', 'ignore')
                  AND v.score_normalized > ?
                ORDER BY v.score_normalized DESC
            """
        cursor.execute(query, (effective_threshold,))
        rows = cursor.fetchall()

        # Calculate counts of assets in each assigned album
        album_counts = {}
        processed_rows = []
        for row in rows:
            assigned_album = row[10] if row[10] else (row[11] if row[11] else "—")
            processed_rows.append((row, assigned_album))
            album_counts[assigned_album] = album_counts.get(assigned_album, 0) + 1
            
        # Sort by: 1. not unassigned ('—' at bottom), 2. album size descending, 3. album name ascending, 4. normalized score descending
        processed_rows.sort(
            key=lambda x: (
                x[1] == "—",
                -album_counts[x[1]],
                x[1],
                -(x[0][2] if x[0][2] is not None else 0.0)
            )
        )

        # Build Qualified Assets Scoring Breakdown report for file logging only (not printed to console)
        scoring_report = []
        scoring_report.append("=========================================================================================================================")
        scoring_report.append("📸 Qualified Assets Scoring Breakdown")
        scoring_report.append("=========================================================================================================================")
        scoring_report.append(f"{'No.':<4} {'Filename':<25} {'Assigned Album':<30} {'Norm Score':<12} {'Aesthetic':<12} {'Google Fav':<12} {'Apple Feat':<12} {'Monthly Sel':<12}")
        scoring_report.append("-" * 125)
        
        for idx, (row, assigned_album) in enumerate(processed_rows, 1):
            filename = row[3] if row[3] else "—"
            score_normalized_val = row[2]
            score_normalized_str = f"{score_normalized_val:.4f}" if score_normalized_val is not None else "—"
            aesthetic_score_val = row[4]
            aesthetic_score_str = f"{aesthetic_score_val:.4f}" if aesthetic_score_val is not None else "—"
            google_fav = "✅ Yes" if row[5] else "❌ No"
            apple_feat = "✅ Yes" if row[6] else "❌ No"
            monthly_sel = "✅ Yes" if row[7] else "❌ No"
            scoring_report.append(f"{idx:<4} {filename:<25} {assigned_album:<30} {score_normalized_str:<12} {aesthetic_score_str:<12} {google_fav:<12} {apple_feat:<12} {monthly_sel:<12}")
        scoring_report.append("=========================================================================================================================\n")

        try:
            with open(SCORING_BREAKDOWN_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(scoring_report) + '\n')
            print(f"📄 Qualified Assets Scoring Breakdown ({len(processed_rows)} assets) saved to: {SCORING_BREAKDOWN_LOG_PATH}\n")
        except Exception as e:
            logger.warning(f"Could not write scoring breakdown log: {e}")
        
        # Group by moment name
        moments_data = {}
        for row in rows:
            asset_id, moment_name, score, filename = row[0], row[1], row[2], row[3]
            is_proposed, is_curated = row[8], row[9]
            is_published = row[12] if len(row) > 12 else None
            if moment_name not in moments_data:
                moments_data[moment_name] = {
                    'total_qualified': 0,
                    'proposed_count': 0,
                    'curated_count': 0,
                    'scores': [],
                    'unpublished_scores': []
                }
            moments_data[moment_name]['total_qualified'] += 1
            if is_proposed:
                moments_data[moment_name]['proposed_count'] += 1
            if is_curated:
                moments_data[moment_name]['curated_count'] += 1
            moments_data[moment_name]['scores'].append(score)
            if not is_published:
                moments_data[moment_name]['unpublished_scores'].append(score)

        # 3. Query Apple Photos albums and folders inside Curated and ToBeCurated (to match existence and get counts)
        applescript_code = """
        tell application "Photos"
            set results to {}
            set parentFolderNames to {"Curated", "ToBeCurated"}
            repeat with fName in parentFolderNames
                if exists folder fName of folder "Media Organizer on LaCie" then
                    set subFolder to folder fName of folder "Media Organizer on LaCie"
                    set subAlbums to albums of subFolder
                    repeat with anAlbum in subAlbums
                        set aName to name of anAlbum
                        try
                            set aCount to count of media items of anAlbum
                        on error
                            set aCount to 0
                        end try
                        copy (fName & "|" & aName & "|" & (aCount as string)) to end of results
                    end repeat
                    set subFolders to folders of subFolder
                    repeat with aFolder in subFolders
                        set aName to name of aFolder
                        copy (fName & "|" & aName & "|0") to end of results
                    end repeat
                end if
            end repeat
            
            set oldDelims to AppleScript's text item delimiters
            set AppleScript's text item delimiters to "\\n"
            set resultsString to results as string
            set AppleScript's text item delimiters to oldDelims
            return resultsString
        end tell
        """
        to_be_curated_albums = {}
        curated_albums = {}
        try:
            process = subprocess.Popen(['osascript', '-e', applescript_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if stdout:
                parts = [p.strip() for p in stdout.strip().split('\n')]
                for p in parts:
                    if '|' in p:
                        subparts = p.split('|')
                        if len(subparts) >= 2:
                            folder_name_clean = subparts[0].strip()
                            album_name_clean = subparts[1].strip()
                            item_count = 0
                            if len(subparts) >= 3:
                                try:
                                    item_count = int(subparts[2].strip())
                                except ValueError:
                                    pass
                            
                            if folder_name_clean == 'ToBeCurated':
                                to_be_curated_albums[album_name_clean] = item_count
                            elif folder_name_clean == 'Curated':
                                curated_albums[album_name_clean] = item_count
        except Exception as e:
            logger.warning(f"Could not list Apple Photos albums: {e}")

        # 4. Fetch memory_stage from curated_moments table
        cursor.execute("SELECT moment_name, memory_stage FROM curated_moments")
        stages = dict(cursor.fetchall())

        # 4.5 Fetch publication information with score stats
        cursor.execute("""
            SELECT 
                p.moment_name,
                MAX(p.published_at_utc) AS last_published_at,
                COUNT(DISTINCT p.asset_id) AS pub_count,
                AVG(v.score_normalized) AS pub_avg_score,
                MIN(v.score_normalized) AS pub_min_score,
                MAX(v.score_normalized) AS pub_max_score
            FROM publications p
            JOIN assets a ON p.asset_id = a.asset_id
            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
            GROUP BY p.moment_name
        """)
        pub_info = {
            row[0]: {
                'last_pub_utc': row[1],
                'pub_count': row[2],
                'pub_avg': row[3],
                'pub_min': row[4],
                'pub_max': row[5]
            }
            for row in cursor.fetchall()
        }

        # 5. Format and display status report
        print("\n==================================================")
        print("🌟 Weekly Memory Feature & Publishing (Mode [M])")
        print("==================================================")
        
        ranked_moments = []
        for name, data in moments_data.items():
            target_scores = data['unpublished_scores']
            avg_score = sum(target_scores) / len(target_scores) if target_scores else 0.0
            stage = stages.get(name, 'M100')
            
            # Check Apple Photos existence
            to_be_curated_exists = (name in to_be_curated_albums)
            curated_exists = (name in curated_albums)
            
            # Check filesystem curated directory existence
            fs_curated_exists = os.path.exists(os.path.join(CURATED_LACIE_DIR, name))
            
            # Count-weighted rank score to prevent small/single-asset moments from dominating
            rank_score = avg_score * math.log(data['total_qualified'] + 1)
            
            p_data = pub_info.get(name, {})
            last_pub_raw = p_data.get('last_pub_utc')
            pub_count = p_data.get('pub_count', 0)
            pub_avg = p_data.get('pub_avg')
            pub_min = p_data.get('pub_min')
            pub_max = p_data.get('pub_max')

            last_pub_str = "—"
            if last_pub_raw:
                try:
                    dt_utc = None
                    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            dt_utc = datetime.strptime(last_pub_raw, fmt)
                            break
                        except ValueError:
                            continue
                    if dt_utc:
                        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                        dt_local = dt_utc.astimezone()
                        last_pub_str = dt_local.strftime("%Y-%m-%d %H:%M")
                    else:
                        last_pub_str = last_pub_raw[:16]
                except Exception:
                    last_pub_str = last_pub_raw[:16]
            
            # Check if featured/published in less than a month (30 days)
            too_recent = False
            if last_pub_raw:
                try:
                    pub_dt = datetime.strptime(last_pub_raw.split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        pub_dt = datetime.strptime(last_pub_raw, "%Y-%m-%d")
                    except ValueError:
                        pub_dt = None
                
                if pub_dt:
                    diff = datetime.now() - pub_dt
                    if diff.days < 30:
                        too_recent = True
            
            has_unpublished = (fs_curated_exists and data['curated_count'] > 0 and data['curated_count'] > pub_count)
            if not has_unpublished:
                can_publish_str = "❌ No"
            elif too_recent:
                can_publish_str = "❌ Recent (<30d)"
            else:
                can_publish_str = "✅ Yes"
            
            # Determine asset count to display (use filesystem count if curated folder exists,
            # fallback to database curated count if present, otherwise total qualified proposed assets)
            fs_curated_path = os.path.join(CURATED_LACIE_DIR, name)
            fs_count = 0
            fs_bases = set()
            if os.path.exists(fs_curated_path):
                try:
                    all_files = [f for f in os.listdir(fs_curated_path) 
                                 if os.path.isfile(os.path.join(fs_curated_path, f)) 
                                 and not f.startswith('.')]
                    # Group by base name to treat Live Photos (HEIC + MOV) as a single asset
                    fs_bases = set(os.path.splitext(f)[0].lower() for f in all_files)
                    fs_count = len(fs_bases)
                except Exception:
                    pass

            if fs_count > 0:
                assets_display = str(fs_count)
            elif name in curated_albums:
                # Use count from Apple Photos Curated album if available (before filesystem export)
                assets_display = str(curated_albums[name])
            elif data['curated_count'] > 0:
                assets_display = str(data['curated_count'])
            else:
                assets_display = str(data['total_qualified'])

            # Compare Apple Photos Curated album assets with local filesystem folder contents
            curated_str = "❌ No"
            if curated_exists and fs_curated_exists:
                # Retrieve Apple Photos Curated album asset base names from Photos DB
                photos_bases = set()
                if photos_db_attached:
                    try:
                        cursor.execute("""
                            SELECT DISTINCT aaa.ZORIGINALFILENAME
                            FROM photos_db.ZGENERICALBUM ga
                            JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = ga.Z_PK
                            JOIN photos_db.ZASSET a ON aa.Z_3ASSETS = a.Z_PK
                            JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                            LEFT JOIN photos_db.ZGENERICALBUM p ON ga.ZPARENTFOLDER = p.Z_PK
                            WHERE ga.ZTITLE = ? AND ga.ZTRASHEDSTATE = 0 AND ga.ZKIND <> 1507
                              AND p.ZTITLE = 'Curated'
                        """, (name,))
                        photos_bases = set(os.path.splitext(row[0])[0].lower() for row in cursor.fetchall() if row[0])
                    except Exception as e:
                        logger.warning(f"Error querying Photos curated album assets for {name}: {e}")

                if photos_db_attached and photos_bases:
                    if photos_bases == fs_bases:
                        curated_str = "✅ Yes"
                    else:
                        curated_str = "⚠️  Mismatch"
                else:
                    curated_str = "✅ Yes"
            elif curated_exists and not fs_curated_exists:
                curated_str = "📁 Needs Folder"
            elif not curated_exists and fs_curated_exists:
                curated_str = "📁 Local Only"

            pub_display = str(pub_count) if pub_count > 0 else "—"
            pub_avg_str = f"{pub_avg:.4f}" if pub_avg is not None else "—"
            pub_range_str = f"{pub_min:.4f} - {pub_max:.4f}" if pub_min is not None else "—"

            ranked_moments.append({
                'name': name,
                'total_qualified': data['total_qualified'],
                'proposed_count': data['proposed_count'],
                'curated_count': data['curated_count'],
                'avg_score': avg_score,
                'min_score': min(target_scores) if target_scores else 0.0,
                'max_score': max(target_scores) if target_scores else 0.0,
                'rank_score': rank_score,
                'stage': stage,
                'to_be_curated_exists': to_be_curated_exists,
                'curated_exists': curated_exists,
                'fs_curated_exists': fs_curated_exists,
                'pub_count': pub_count,
                'pub_display': pub_display,
                'pub_avg_str': pub_avg_str,
                'pub_range_str': pub_range_str,
                'last_pub_str': last_pub_str,
                'can_publish_str': can_publish_str,
                'assets_display': assets_display,
                'curated_str': curated_str
            })

        # Detach photos_db now that we are done querying it for moments list
        if photos_db_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db;")
            except Exception as e:
                logger.warning(f"Could not detach Photos.sqlite: {e}")

        # Sort by: 
        # 1. Needs update (proposed + curated < total_qualified)
        # 2. If needs update: rank score descending; if up-to-date: average score descending
        ranked_moments.sort(key=lambda x: (
            (x['proposed_count'] + x['curated_count']) < x['total_qualified'],
            x['rank_score'] if ((x['proposed_count'] + x['curated_count']) < x['total_qualified']) else x['avg_score']
        ), reverse=True)

        header_m = f"{'No.':<4} {'Moment Name':<30} {'Rank Score':<12} {'Avg Score':<10} {'Min Score':<10} {'Max Score':<10} {'Assets':<8} {'Pub.':<6} {'Pub. Avg':<10} {'Pub. Range':<17} {'ToBeCurated?':<13} {'Curated?':<15} {'Published?':<13} {'Can Publish?':<18} {'Last Published':<18}"
        print(header_m)
        print("-" * len(header_m))
        divider_printed = False
        for idx, m in enumerate(ranked_moments, 1):
            displayed_moments_map[idx] = {'name': m['name'], 'type': 'ranked_moment'}
            is_needs_update = (m['proposed_count'] + m['curated_count']) < m['total_qualified']
            if not is_needs_update and not divider_printed:
                if idx > 1:
                    print("-" * len(header_m))
                    print(f"--- Up-To-Date Moments " + "-" * (len(header_m) - 23))
                    print("-" * len(header_m))
                divider_printed = True

            to_be_curated_str = "✅ Yes" if m['to_be_curated_exists'] else "❌ No"
            if (m['proposed_count'] + m['curated_count']) < m['total_qualified'] and m['to_be_curated_exists']:
                to_be_curated_str = "🔄 Update needed"
            
            curated_str = m['curated_str']
            if m['pub_count'] == 0:
                published_str = "❌ No"
            elif m['pub_count'] >= m['total_qualified'] or (m['stage'] == 'M500' and m['pub_count'] >= m['curated_count']):
                published_str = "✅ Yes"
            else:
                published_str = f"🔄 Part ({m['pub_count']})"

            print(f"{idx:<4} {m['name']:<30} {m['rank_score']:<12.4f} {m['avg_score']:<10.4f} {m['min_score']:<10.4f} {m['max_score']:<10.4f} {m['assets_display']:<8} {m['pub_display']:<6} {m['pub_avg_str']:<10} {m['pub_range_str']:<17} {to_be_curated_str:<13} {curated_str:<15} {published_str:<13} {m['can_publish_str']:<18} {m['last_pub_str']:<18}")

        # Build timeline map of moments to find closest merge suggestions for disjoint moments
        cursor.execute("""
            SELECT a.curated_album, v.MomentsAlbumName, a.date_created_utc
            FROM assets a
            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
            WHERE a.date_created_utc IS NOT NULL
        """)
        all_asset_dates = cursor.fetchall()
        moment_dates_map = {}
        for cur_alb, mom_name, dt_str in all_asset_dates:
            dt = None
            for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    break
                except ValueError:
                    pass
            if not dt:
                continue
            for m_key in set([cur_alb, mom_name]):
                if m_key:
                    if m_key not in moment_dates_map:
                        moment_dates_map[m_key] = []
                    moment_dates_map[m_key].append(dt)

        # Fallback date parsing from folder names on disk
        if os.path.exists(CURATED_LACIE_DIR):
            for d in os.listdir(CURATED_LACIE_DIR):
                if not d.startswith('.') and os.path.isdir(os.path.join(CURATED_LACIE_DIR, d)):
                    if d not in moment_dates_map:
                        moment_dates_map[d] = []
                    if not moment_dates_map[d]:
                        m_match = re.match(r'(\d{4}-\d{2}-\d{2})', d)
                        if m_match:
                            try:
                                moment_dates_map[d].append(datetime.strptime(m_match.group(1), '%Y-%m-%d'))
                            except ValueError:
                                pass
                        else:
                            m_match = re.match(r'(\d{4}-\d{2})', d)
                            if m_match:
                                try:
                                    moment_dates_map[d].append(datetime.strptime(m_match.group(1) + '-01', '%Y-%m-%d'))
                                except ValueError:
                                    pass

        moment_summary = {}
        for m_name, dts in moment_dates_map.items():
            if not dts:
                continue
            fs_path_m = os.path.join(CURATED_LACIE_DIR, m_name)
            fs_cnt = len([f for f in os.listdir(fs_path_m) if not f.startswith('.')]) if os.path.exists(fs_path_m) else len(dts)
            min_dt = min(dts)
            max_dt = max(dts)
            mid_dt = min_dt + (max_dt - min_dt) / 2
            moment_summary[m_name] = {
                'mid': mid_dt,
                'count': fs_cnt
            }

        def find_closest_merge_candidate(target_name):
            if target_name not in moment_summary:
                return None
            t_mid = moment_summary[target_name]['mid']
            best_candidate = None
            best_diff_days = None
            for other_name, o_info in moment_summary.items():
                if other_name == target_name:
                    continue
                if o_info['count'] < 2:
                    continue
                diff_sec = (o_info['mid'] - t_mid).total_seconds()
                diff_days = diff_sec / 86400.0
                abs_days = abs(diff_days)
                if best_diff_days is None or abs_days < abs(best_diff_days):
                    best_diff_days = diff_days
                    best_candidate = other_name
            if best_candidate:
                days_int = round(abs(best_diff_days))
                time_rel = f'+{days_int}d' if best_diff_days > 0 else f'-{days_int}d' if best_diff_days < 0 else '0d'
                return f"💡 Suggest merge with: '{best_candidate}' ({time_rel})"
            return None

        # Display Weekly Memory Publishing Recommendations
        cursor.execute("SELECT asset_id, moment_name FROM publications")
        published_assets_by_moment = {}
        for aid, mom_name in cursor.fetchall():
            if mom_name not in published_assets_by_moment:
                published_assets_by_moment[mom_name] = set()
            published_assets_by_moment[mom_name].add(aid)

        recommendations = []
        for m in ranked_moments:
            name = m['name']
            p_data = pub_info.get(name, {})
            last_pub_date = p_data.get('last_pub_utc')
            pub_count = p_data.get('pub_count', 0)
            
            # Check for Disjoint Moment (<2 qualified assets)
            if m['total_qualified'] < 2:
                suggested_merge = find_closest_merge_candidate(name)
                recommendations.append({
                    'name': name,
                    'avg_score': m['avg_score'],
                    'total_unique': m['total_qualified'],
                    'pub_count': pub_count,
                    'rec_count': 0,
                    'action': "Disjoint: Merge needed (<2 assets)",
                    'suggested_merge': suggested_merge,
                    'rec_bases': [],
                    'base_to_files': {}
                })
                continue

            fs_curated_path = os.path.join(CURATED_LACIE_DIR, name)
            
            # Check files in folder if it exists
            files = []
            if m['fs_curated_exists']:
                try:
                    files = [f for f in os.listdir(fs_curated_path) 
                             if os.path.isfile(os.path.join(fs_curated_path, f)) 
                             and not f.startswith('.')]
                except Exception:
                    pass
            
            if not files:
                continue
                
            # Group by base name (Live Photos)
            base_to_files = {}
            for f in files:
                base, ext = os.path.splitext(f)
                base_lower = base.lower()
                if base_lower not in base_to_files:
                    base_to_files[base_lower] = []
                base_to_files[base_lower].append(f)
                
            unique_bases = list(base_to_files.keys())
            total_unique = len(unique_bases)
            
            # Check publication timing info
            too_recent = False
            if last_pub_date:
                try:
                    pub_dt = datetime.strptime(last_pub_date.split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        pub_dt = datetime.strptime(last_pub_date, "%Y-%m-%d")
                    except ValueError:
                        pub_dt = None
                if pub_dt:
                    diff = datetime.now() - pub_dt
                    if diff.days < 30:
                        too_recent = True
            
            # Only recommend if not too recent and we have unpublished assets
            if too_recent or total_unique <= pub_count:
                continue
                
            # Query database scores for all assets strictly assigned to this moment under Moments
            cursor.execute("""
                SELECT original_filename, score_normalized, asset_id 
                FROM ranked_assets_view 
                WHERE MomentsAlbumName = ?
            """, (name,))
            db_assets = cursor.fetchall()
            
            # Map base name to highest score and keep asset ID
            base_scores = {}
            base_asset_ids = {}
            for orig_fname, score, asset_id in db_assets:
                if orig_fname:
                    base_orig = os.path.splitext(orig_fname)[0].lower()
                    if score > base_scores.get(base_orig, -1.0):
                        base_scores[base_orig] = score
                        base_asset_ids[base_orig] = asset_id
                    
            # Filter unique bases in folder strictly to assets that belong to this moment
            valid_bases = [b for b in unique_bases if b in base_scores]
            scored_bases = [(b, base_scores[b]) for b in valid_bases]
            scored_bases.sort(key=lambda x: (x[1], x[0]), reverse=True)
            total_unique = len(valid_bases)
            
            # Determine recommendation based on publication count and remaining assets
            # We want to publish a batch of up to 9 assets.
            # If pub_count == 0 (first publication):
            suggested_merge = None
            if pub_count == 0:
                if total_unique < 2:
                    action = "Disjoint: Merge needed (<2 assets)"
                    rec_count = 0
                    rec_bases_list = []
                    rec_avg_score = m['avg_score']
                    suggested_merge = find_closest_merge_candidate(name)
                elif 2 <= total_unique <= 9:
                    action = "Publish Whole Album"
                    rec_count = total_unique
                    rec_bases_list = [b[0] for b in scored_bases]
                    rec_avg_score = sum(b[1] for b in scored_bases) / len(scored_bases) if scored_bases else 0.0
                else: # total_unique > 9
                    action = f"Publish Top 9 Assets (out of {total_unique})"
                    rec_count = 9
                    rec_bases_list = [b[0] for b in scored_bases[:9]]
                    rec_avg_score = sum(b[1] for b in scored_bases[:9]) / 9
            else: # pub_count > 0 (republishing next batch)
                published_set = published_assets_by_moment.get(name, set())
                rem_bases = [
                    (b, score) for b, score in scored_bases
                    if base_asset_ids.get(b) not in published_set
                ]
                if not rem_bases:
                    continue
                rec_count = min(9, len(rem_bases))
                action = f"Republish: Next {rec_count} Assets (regulation passed)"
                rec_bases_list = [b[0] for b in rem_bases[:rec_count]]
                rec_avg_score = sum(b[1] for b in rem_bases[:rec_count]) / rec_count
                
            recommendations.append({
                'name': name,
                'avg_score': rec_avg_score,
                'total_unique': total_unique,
                'pub_count': pub_count,
                'rec_count': rec_count,
                'action': action,
                'suggested_merge': suggested_merge,
                'rec_bases': rec_bases_list,
                'base_to_files': base_to_files
            })
            
        # Partition into actionable and disjoint groups
        actionable_recs = [r for r in recommendations if not r['action'].startswith("Disjoint")]
        disjoint_recs = [r for r in recommendations if r['action'].startswith("Disjoint")]
        
        # Sort each group by average score descending
        actionable_recs.sort(key=lambda x: -x['avg_score'])
        disjoint_recs.sort(key=lambda x: -x['avg_score'])
        
        # Select top 12 actionable and top 10 disjoint
        top_recommendations = actionable_recs[:12] + disjoint_recs[:10]
        
        if top_recommendations:
            print("\n==================================================================================================================================================================")
            print("📢 Publishing Recommendations (Top 12 Actionable & Top 10 Disjoint Candidates)")
            print("==================================================================================================================================================================")
            print(f"{'No.':<4} {'Moment Name':<30} {'Avg Score':<10} {'Files':<6} {'Pub.':<5} {'Rec.':<5} {'Recommendation/Action':<40} {'Recommended Assets / Suggested Merge'}")
            print("-" * 168)
            divider_printed = False
            start_idx = len(ranked_moments) + 1
            for idx, rec in enumerate(top_recommendations, start_idx):
                displayed_moments_map[idx] = {'name': rec['name'], 'type': 'recommendation', 'rec_bases': rec['rec_bases'], 'action': rec['action']}
                if rec['action'].startswith("Disjoint") and not divider_printed:
                    print("-" * 168)
                    print(f"--- Disjoint Moments (Need Merge) " + "-" * 134)
                    print("-" * 168)
                    divider_printed = True
                
                if rec['action'].startswith("Disjoint"):
                    assets_str = rec.get('suggested_merge') or "—"
                elif rec['rec_bases']:
                    if len(rec['rec_bases']) <= 5:
                        assets_str = ", ".join(rec['rec_bases'])
                    else:
                        assets_str = ", ".join(rec['rec_bases'][:4]) + f" (+{len(rec['rec_bases'])-4} more)"
                else:
                    assets_str = "—"
                print(f"{idx:<4} {rec['name']:<30} {rec['avg_score']:<10.4f} {rec['total_unique']:<6} {rec['pub_count']:<5} {rec['rec_count']:<5} {rec['action']:<40} {assets_str}")
            print("==================================================================================================================================================================\n")

            # Sync folders and files to 'Publishing Recommendation' directory
            PUBLISHING_RECOMMENDATION_DIR = "/Volumes/LaCie/Media Organizer/Publishing Recommendation"
            os.makedirs(PUBLISHING_RECOMMENDATION_DIR, exist_ok=True)
            active_rec_names = set()
            
            print("📂 Syncing files to 'Publishing Recommendation' folder...")
            import shutil
            
            for rec in top_recommendations:
                if not rec['rec_bases'] or rec['rec_count'] == 0:
                    continue
                    
                moment_name = rec['name']
                active_rec_names.add(moment_name)
                
                src_folder = os.path.join(CURATED_LACIE_DIR, moment_name)
                dest_folder = os.path.join(PUBLISHING_RECOMMENDATION_DIR, moment_name)
                os.makedirs(dest_folder, exist_ok=True)
                
                # Gather all files that should be in the destination
                expected_files = []
                for base in rec['rec_bases']:
                    expected_files.extend(rec['base_to_files'].get(base, []))
                    
                expected_files_set = set(expected_files)
                
                # Delete extra/stale files in dest_folder
                try:
                    dest_files = os.listdir(dest_folder)
                    for f in dest_files:
                        if f.startswith('.'):
                            continue
                        if f not in expected_files_set:
                            file_path = os.path.join(dest_folder, f)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                logger.info(f"Deleted outdated recommended file: {moment_name}/{f}")
                except Exception as e:
                    logger.warning(f"Error cleaning folder {dest_folder}: {e}")
                    
                # Copy missing files from src_folder to dest_folder
                for f in expected_files:
                    src_file = os.path.join(src_folder, f)
                    dest_file = os.path.join(dest_folder, f)
                    if os.path.exists(src_file) and not os.path.exists(dest_file):
                        try:
                            shutil.copy2(src_file, dest_file)
                            logger.info(f"Copied recommended asset: {moment_name}/{f}")
                        except Exception as e:
                            logger.error(f"Error copying {src_file} to {dest_file}: {e}")
            
            # Clean up old folders in PUBLISHING_RECOMMENDATION_DIR that are no longer recommended
            try:
                for d in os.listdir(PUBLISHING_RECOMMENDATION_DIR):
                    d_path = os.path.join(PUBLISHING_RECOMMENDATION_DIR, d)
                    if os.path.isdir(d_path) and not d.startswith('.'):
                        if d not in active_rec_names:
                            shutil.rmtree(d_path)
                            logger.info(f"Deleted outdated recommendation folder: {d}")
            except Exception as e:
                logger.warning(f"Error cleaning up outdated recommendation folders: {e}")
                
            print("✅ 'Publishing Recommendation' folder is up to date!")

        # Close database connection and release lock before action prompt
        if photos_db_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db")
            except Exception:
                pass
        close_conn()
        release_planner_lock()

        print("\n--- Actions ---")
        print(" [1] Sync proposed assets to ToBeCurated albums in Apple Photos")
        print(" [2] Export Curated album from Apple Photos to LaCie filesystem")
        print(" [3] Record publication in the database (Mark as Published to Shutterfly/YouTube)")
        print(" [R] Restart the planner")
        print(" [E] Exit")
        
        choice = input("\nSelect action: ").strip().lower()
        if choice == 'r':
            logger.info("Restarting planner...")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            release_planner_lock()
            os.execv(sys.executable, [sys.executable] + sys.argv)
        elif choice == '1':
            acquire_planner_lock()
            script_dir = os.path.dirname(os.path.abspath(__file__))
            logger.info("Syncing proposed assets to Apple Photos...")
            try:
                subprocess.run([sys.executable, os.path.join(script_dir, "create_apple_moments_albums.py")], check=True)
                logger.info("Sync complete.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Sync failed: {e}")
            release_planner_lock()
        elif choice == '2':
            moment_name = input("Enter Moment Name to export (or index from list): ").strip()
            if moment_name.isdigit():
                idx = int(moment_name)
                if idx in displayed_moments_map:
                    moment_name = displayed_moments_map[idx]['name']
            
            if not moment_name:
                continue
                
            dest_folder = os.path.join(CURATED_LACIE_DIR, moment_name)
            if not os.path.exists(dest_folder):
                create_confirm = input(f"📁 Folder '{dest_folder}' does not exist. Do you want to create it? [y/N]: ").strip().lower()
                if create_confirm == 'y':
                    os.makedirs(dest_folder, exist_ok=True)
                    logger.info(f"Created folder: {dest_folder}")
                else:
                    logger.warning("Aborted export.")
                    continue
                    
            acquire_planner_lock()
            script_dir = os.path.dirname(os.path.abspath(__file__))
            try:
                subprocess.run([sys.executable, os.path.join(script_dir, "export_curated_album.py"), moment_name], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Export failed: {e}")
            release_planner_lock()
        elif choice == '3':
            moment_name = input("Enter Moment Name to publish (or index from list): ").strip()
            selected_rec = None
            if moment_name.isdigit():
                idx = int(moment_name)
                if idx in displayed_moments_map:
                    item_info = displayed_moments_map[idx]
                    moment_name = item_info['name']
                    if item_info['type'] == 'recommendation':
                        selected_rec = item_info
            
            if not moment_name:
                continue
                
            if selected_rec and selected_rec['action'].startswith("Disjoint"):
                print(f"⚠️ Cannot publish '{moment_name}': {selected_rec['action']}")
                continue

            acquire_planner_lock()
            conn = get_connection()
            conn.execute("PRAGMA busy_timeout = 30000")
            cursor = get_cursor()

            cursor.execute("""
                SELECT me.asset_id, a.original_filename
                FROM moment_exports me
                JOIN assets a ON me.asset_id = a.asset_id
                WHERE me.album_name = ? AND me.curation_stage = 'curated'
            """, (moment_name,))
            curated_assets_info = cursor.fetchall()
            
            if not curated_assets_info:
                print(f"⚠️ No curated assets found in the DB for '{moment_name}'. Please export the Curated album first.")
                close_conn()
                release_planner_lock()
                continue
                
            cursor.execute("SELECT asset_id FROM publications WHERE moment_name = ?", (moment_name,))
            already_published = set(row[0] for row in cursor.fetchall())

            # Filter target assets to publish
            if selected_rec:
                rec_bases = set(selected_rec['rec_bases'])
                target_assets = []
                for asset_id, orig_fname in curated_assets_info:
                    if orig_fname:
                        base = os.path.splitext(orig_fname)[0].lower()
                        if base in rec_bases:
                            target_assets.append(asset_id)
            else:
                target_assets = [row[0] for row in curated_assets_info]

            # Exclude already published
            target_assets = [aid for aid in target_assets if aid not in already_published]

            if not target_assets:
                print(f"ℹ️ All selected assets for '{moment_name}' are already marked as published.")
                close_conn()
                release_planner_lock()
                continue

            confirm = input(f"Confirm publication of {len(target_assets)} assets of '{moment_name}' to Shutterfly/YouTube? [y/N]: ").strip().lower()
            if confirm == 'y':
                try:
                    pub_data = [(aid, moment_name, 'Shutterfly/YouTube') for aid in target_assets]
                    cursor.executemany("""
                        INSERT INTO publications (asset_id, moment_name, platform, published_at_utc)
                        VALUES (?, ?, ?, datetime('now'))
                    """, pub_data)
                    
                    # Calculate new stage
                    total_pub_after = len(already_published) + len(target_assets)
                    new_stage = 'M500' if total_pub_after >= len(curated_assets_info) else 'M400'

                    cursor.execute("""
                        INSERT INTO curated_moments (moment_name, memory_stage)
                        VALUES (?, ?)
                        ON CONFLICT(moment_name) DO UPDATE SET memory_stage = excluded.memory_stage
                    """, (moment_name, new_stage))
                    
                    conn.commit()
                    print(f"✅ Recorded publication of {len(target_assets)} assets for '{moment_name}' in database (Stage: {new_stage}).")
                except Exception as e:
                    logger.warning(f"Failed to record publication: {e}")
                    conn.rollback()
            close_conn()
            release_planner_lock()
        elif choice == 'e':
            break

def resolve_device_owner(cursor, camera_model):
    """
    Looks up the owner of a camera model. Checks database overrides first,
    then defaults to DEVICE_OWNER_MAPPING, then 'Shared/Other'.
    """
    try:
        cursor.execute("SELECT owner_name FROM device_owners WHERE camera_model = ?", (camera_model,))
        row = cursor.fetchone()
        if row:
            return row[0], "Database Override"
    except Exception:
        pass
    
    if camera_model in DEVICE_OWNER_MAPPING:
        return DEVICE_OWNER_MAPPING[camera_model], "Default Mapping"
    
    return "Shared/Other", "Default Fallback"

def manage_device_owners_flow(cursor=None, conn=None):
    """
    Interactive flow to view and edit primary owners of camera devices.
    Lists devices ordered by their total asset count in the database copy.
    """
    from constants import DEVICE_OWNER_MAPPING
    
    while True:
        acquire_planner_lock()
        conn = get_connection()
        conn.execute("PRAGMA busy_timeout = 30000")
        cursor = get_cursor()

        # Attach photos_db for counting assets by camera model
        photos_db_attached = False
        try:
            cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db")
            photos_db_attached = True
        except Exception:
            pass

        # Get all distinct camera models from imports/assets and count/timestamp them
        counts_dict = {}
        try:
            cursor.execute("""
                WITH model_stats AS (
                    SELECT 
                        zea.ZCAMERAMODEL AS model,
                        COUNT(a.asset_id) AS total_count
                    FROM assets a
                    JOIN photos_db.ZASSET za ON za.ZUUID = a.asset_id
                    JOIN photos_db.ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                    WHERE zea.ZCAMERAMODEL IS NOT NULL AND zea.ZCAMERAMODEL != ''
                    GROUP BY model
                ),
                ranked_assets AS (
                    SELECT 
                        zea.ZCAMERAMODEL AS model,
                        a.original_filename,
                        date(za.ZDATECREATED + 978307200, 'unixepoch') AS created_time,
                        ROW_NUMBER() OVER(PARTITION BY zea.ZCAMERAMODEL ORDER BY za.ZDATECREATED ASC, a.original_filename ASC) as rn_asc,
                        ROW_NUMBER() OVER(PARTITION BY zea.ZCAMERAMODEL ORDER BY za.ZDATECREATED DESC, a.original_filename DESC) as rn_desc
                    FROM assets a
                    JOIN photos_db.ZASSET za ON za.ZUUID = a.asset_id
                    JOIN photos_db.ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                    WHERE zea.ZCAMERAMODEL IS NOT NULL AND zea.ZCAMERAMODEL != ''
                      AND date(za.ZDATECREATED + 978307200, 'unixepoch') > '1970-01-01'
                      AND date(za.ZDATECREATED + 978307200, 'unixepoch') NOT LIKE '0001-%'
                )
                SELECT 
                    ms.model,
                    ms.total_count,
                    MAX(case when ra.rn_asc = 1 then ra.original_filename end) as min_filename,
                    MAX(case when ra.rn_asc = 1 then ra.created_time end) as min_created,
                    MAX(case when ra.rn_desc = 1 then ra.original_filename end) as max_filename,
                    MAX(case when ra.rn_desc = 1 then ra.created_time end) as max_created
                FROM model_stats ms
                LEFT JOIN ranked_assets ra ON ms.model = ra.model AND (ra.rn_asc = 1 OR ra.rn_desc = 1)
                GROUP BY ms.model, ms.total_count
            """)
            for r in cursor.fetchall():
                counts_dict[r[0]] = {
                    'count': r[1],
                    'min_filename': r[2],
                    'min_created': r[3],
                    'max_filename': r[4],
                    'max_created': r[5]
                }
        except Exception as e:
            logger.debug(f"Could not count assets by device model: {e}")

        # Merge with constants DEVICE_OWNER_MAPPING
        db_models = list(counts_dict.keys())
        all_unique_models = list(set(db_models + list(DEVICE_OWNER_MAPPING.keys())))
        
        models_list = []
        for model in all_unique_models:
            if model == 'Unknown' or not model:
                continue
            item_data = counts_dict.get(model, {})
            count = item_data.get('count', 0)
            min_filename = item_data.get('min_filename', '—') or '—'
            min_created = item_data.get('min_created', '—') or '—'
            max_filename = item_data.get('max_filename', '—') or '—'
            max_created = item_data.get('max_created', '—') or '—'
            owner, src_type = resolve_device_owner(cursor, model)
            models_list.append({
                'model': model,
                'count': count,
                'min_filename': min_filename,
                'min_created': min_created,
                'max_filename': max_filename,
                'max_created': max_created,
                'owner': owner,
                'src_type': src_type
            })

        # Sort by asset count ascending, then model name ascending to keep most-used at the bottom
        models_list.sort(key=lambda x: (x['count'], x['model']))

        print("\n" + "=" * 161)
        print("👤  MANAGE DEVICE PRIMARY OWNERS")
        print("=" * 161)
        print(f"{'No.':<4} {'Device Camera Model':<36} {'Asset Count':<13} {'Earliest Created Asset (Filename & Date)':<38} {'Latest Created Asset (Filename & Date)':<38} {'Current Owner':<16} {'Source Type':<16}")
        print("-" * 161)

        model_owners = []
        for idx, item in enumerate(models_list, 1):
            earliest_str = f"{item['min_filename']} ({item['min_created']})" if item['min_filename'] != '—' else '—'
            latest_str = f"{item['max_filename']} ({item['max_created']})" if item['max_filename'] != '—' else '—'
            print(f"{idx:<4} {item['model']:<36} {item['count']:<13,} {earliest_str:<38} {latest_str:<38} {item['owner']:<16} {item['src_type']:<16}")
            model_owners.append((item['model'], item['owner']))

        print("-" * 185)

        # Filter and group overrides
        overrides_by_owner = {}
        for item in models_list:
            if item['src_type'] == 'Database Override':
                owner = item['owner']
                if owner not in overrides_by_owner:
                    overrides_by_owner[owner] = []
                overrides_by_owner[owner].append(item)

        if overrides_by_owner:
            print("\n" + "=" * 100)
            print("👤  DEVICE TIMELINE BY OWNER (DATABASE OVERRIDES ONLY)")
            print("=" * 100)
            print(f"{'Primary Owner':<16} {'Device Camera Model':<36} {'Asset Count':<14} {'Earliest Date':<15} {'Latest Date':<15}")
            print("-" * 100)

            for owner in sorted(overrides_by_owner.keys()):
                devices = overrides_by_owner[owner]
                devices.sort(key=lambda x: x['min_created'] if x['min_created'] != '—' else '9999-12-31')
                
                first_row = True
                for dev in devices:
                    owner_col = owner if first_row else ""
                    print(f"{owner_col:<16} {dev['model']:<36} {dev['count']:<14,} {dev['min_created']:<15} {dev['max_created']:<15}")
                    first_row = False
            print("-" * 100)

        # Detach and release before waiting for user action prompts
        if photos_db_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db")
            except Exception:
                pass
        close_conn()
        release_planner_lock()

        choice = input("\nOptions: [E]it an owner | [B]ack to main menu: ").strip().lower()
        if choice == 'b' or not choice:
            break
        elif choice == 'e':
            num_input = input(f"Enter device number to edit (1-{len(models_list)}) or Q to cancel: ").strip()
            if num_input.lower() == 'q':
                continue
            try:
                num = int(num_input)
                if num < 1 or num > len(models_list):
                    print("⚠️ Invalid number selection.")
                    continue
                selected_model, current_owner = model_owners[num - 1]
                new_owner = input(f"Enter new owner name for '{selected_model}' (leave empty to reset to default): ").strip()
                
                # Now perform the update, acquire lock and connect
                acquire_planner_lock()
                conn = get_connection()
                conn.execute("PRAGMA busy_timeout = 30000")
                cursor = get_cursor()
                
                # Check if empty, then delete override
                if not new_owner:
                    cursor.execute("DELETE FROM device_owners WHERE camera_model = ?", (selected_model,))
                    conn.commit()
                    print(f"✅ Reset '{selected_model}' to its default configuration.")
                else:
                    cursor.execute("""
                        INSERT OR REPLACE INTO device_owners (camera_model, owner_name)
                        VALUES (?, ?)
                    """, (selected_model, new_owner))
                    conn.commit()
                    print(f"✅ Updated owner of '{selected_model}' to '{new_owner}'.")
                
                close_conn()
                release_planner_lock()
            except ValueError:
                print("⚠️ Please enter a valid number.")
            except Exception as e:
                print(f"⚠️ Failed to update database: {e}")
                try:
                    close_conn()
                except Exception:
                    pass
                release_planner_lock()

    # Detach database safely at exit of flow
    try:
        cursor.execute("DETACH DATABASE photos_db")
    except Exception:
        pass

def display_media_cleanup_recommendations(cursor, verbose=True):
    """
    Generates and displays media cleanup recommendations for source cameras based on published albums.
    Groups recommendations by device model. For each recommendation row, queries Apple Photos DB copy
    for the total asset count and size within the corresponding date range to quantify storage gains.
    """
    # First, attach photos_db to query full camera/source libraries
    try:
        cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db")
        logger.debug("Attached Photos.sqlite database read-only for cleanup scan.")
    except Exception as e:
        logger.warning(f"Could not attach Photos.sqlite: {e}")

    # Query published moments and their camera/file metrics grouped by calendar month
    cursor.execute("""
        SELECT 
            a.month,
            MAX(p.published_at_utc) AS last_published_at,
            zea.ZCAMERAMAKE AS camera_make,
            zea.ZCAMERAMODEL AS camera_model,
            COUNT(DISTINCT a.asset_id) AS total_published_assets,
            MIN(a.original_filename) AS min_filename,
            MAX(a.original_filename) AS max_filename,
            MIN(a.date_created_utc) AS min_date,
            MAX(a.date_created_utc) AS max_date
        FROM publications p
        JOIN assets a ON p.asset_id = a.asset_id
        LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
        WHERE zea.ZCAMERAMODEL IS NOT NULL AND zea.ZCAMERAMODEL != ''
        GROUP BY a.month, zea.ZCAMERAMAKE, zea.ZCAMERAMODEL
        ORDER BY MAX(p.published_at_utc) DESC, a.month ASC
    """)
    rows = cursor.fetchall()

    cleanup_report = []
    cleanup_report.append("==================================================================================================================================")
    cleanup_report.append("🧹 Media Cleanup Recommendations (Safe to Delete from Source Cameras)")
    cleanup_report.append("==================================================================================================================================")

    if not rows:
        cleanup_report.append("ℹ️  No published moments found in database.")
        cleanup_report.append("👉 Once moments are published in Mode [M], safe deletion recommendations for your camera SD cards will appear here.")
        cleanup_report.append("==================================================================================================================================\n")
    else:
        cleanup_report.append("The following events/moments have been curated and published.")
        cleanup_report.append("You can safely format or delete these files from your source cameras / SD cards (grouped by device):\n")

        # Group rows by device owner and then device model
        # owner_groups = { owner: { device: [rows] } }
        owner_groups = {}
        for row in rows:
            c_make = row[2] or ""
            c_model = row[3] or "Unknown"
            c_source = f"{c_model}" if (c_model != "Unknown" and c_model) else (c_make or "Unknown")
            
            # Look up owner dynamically via database or defaults
            owner_resolved, _ = resolve_device_owner(cursor, c_source)
            if owner_resolved == "Shared/Other" and c_model != c_source:
                owner_resolved, _ = resolve_device_owner(cursor, c_model)
            owner = owner_resolved
            
            if owner not in owner_groups:
                owner_groups[owner] = {}
            if c_source not in owner_groups[owner]:
                owner_groups[owner][c_source] = []
            owner_groups[owner][c_source].append(row)

        global_idx = 1
        # Process each owner group
        for owner_name in sorted(owner_groups.keys()):
            cleanup_report.append(f"👤 Primary Owner: {owner_name}")
            cleanup_report.append("=" * 135)
            
            owner_total_files = 0
            owner_total_bytes = 0
            
            # Process each device for this owner
            for device_name, group_rows in sorted(owner_groups[owner_name].items()):
                cleanup_report.append(f"  📷 Device: {device_name}")
                cleanup_report.append("  " + "-" * 133)
                header = f"  {'No.':<4} {'Month':<12} {'Published':<12} {'Filename Range':<32} {'Date Range':<24} {'Reclaimable from SD Card (Whole Month)':<30}"
                cleanup_report.append(header)
                cleanup_report.append("  " + "-" * 133)

                device_total_files = 0
                device_total_bytes = 0

                # Gather data and query photos_db for all rows first
                processed_rows = []
                for row in group_rows:
                    month_val = row[0] or "—"
                    c_make = row[2] or ""
                    c_model = row[3] or "Unknown"
                    file_count = str(row[4])
                    f_min = row[5] or "—"
                    f_max = row[6] or "—"
                    f_range = f"{f_min} -> {f_max}" if f_min != f_max else f_min
                    d_min = (row[7][:10] if row[7] else "—")
                    d_max = (row[8][:10] if row[8] else "—")
                    d_range = f"{d_min} to {d_max}" if d_min != d_max else d_min

                    # Fetch whole-month count and size of all files on this device
                    total_scan_count = 0
                    total_scan_bytes = 0
                    if row[0] and device_name != "Unknown":
                        try:
                            cursor.execute("""
                                SELECT COUNT(a.Z_PK), SUM(r.ZDATALENGTH)
                                FROM photos_db.ZASSET a
                                JOIN photos_db.ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = a.Z_PK
                                LEFT JOIN photos_db.ZINTERNALRESOURCE r ON r.ZASSET = a.Z_PK AND r.ZRESOURCETYPE = 0
                                WHERE COALESCE(ea.ZCAMERAMODEL, 'Unknown') = ?
                                  AND strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch')) = ?
                            """, (c_model, month_val))
                            res = cursor.fetchone()
                            if res:
                                total_scan_count = res[0] or 0
                                total_scan_bytes = res[1] or 0
                        except Exception as e:
                            logger.debug(f"Could not scan files size range: {e}")

                    processed_rows.append({
                        "month_val": month_val,
                        "file_count": file_count,
                        "f_range": f_range,
                        "d_range": d_range,
                        "total_scan_count": total_scan_count,
                        "total_scan_bytes": total_scan_bytes
                    })

                # Sort processed_rows by total_scan_bytes descending
                processed_rows.sort(key=lambda x: x["total_scan_bytes"], reverse=True)

                for item in processed_rows:
                    device_total_files += item["total_scan_count"]
                    device_total_bytes += item["total_scan_bytes"]

                    scan_range_str = f"{item['total_scan_count']} files ({human_readable_size(item['total_scan_bytes'])})" if item['total_scan_count'] > 0 else "—"

                    line = f"  {global_idx:<4} {item['month_val']:<12} {item['file_count'] + ' files':<12} {item['f_range']:<32} {item['d_range']:<24} {scan_range_str:<30}"
                    cleanup_report.append(line)
                    global_idx += 1

                owner_total_files += device_total_files
                owner_total_bytes += device_total_bytes

                cleanup_report.append("  " + "-" * 133)
                cleanup_report.append(f"  💰 Subtotal reclaimable space on {device_name}: {device_total_files} files ({human_readable_size(device_total_bytes)})")
                cleanup_report.append("")

            cleanup_report.append("-" * 135)
            cleanup_report.append(f"💰 Total reclaimable space for owner {owner_name}: {owner_total_files} files ({human_readable_size(owner_total_bytes)})")
            cleanup_report.append("==================================================================================================================================\n")

    # Detach database safely
    try:
        cursor.execute("DETACH DATABASE photos_db")
        logger.debug("Detached Photos.sqlite database after cleanup scan.")
    except Exception:
        pass

    # Write to dedicated log file
    try:
        with open(MEDIA_CLEANUP_LOG_PATH, 'w', encoding='utf-8') as f:
            f.write('\n'.join(cleanup_report) + '\n')
        if verbose:
            logger.info(f"📄 Media cleanup recommendations written to {MEDIA_CLEANUP_LOG_PATH}")
    except Exception as e:
        logger.warning(f"Could not write media cleanup log: {e}")

    # Print to console
    print('\n' + '\n'.join(cleanup_report))

def main(auto_apply, no_sync=False):
    # Set up logger with line number in format
    if not no_sync:
        ensure_bg_service_running()
    check_if_refresh_needed()

    # Check for active planned executions in queue
    check_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    check_cursor = check_conn.cursor()
    check_cursor.execute("SELECT id, planned_month, set_at_utc FROM planned_execution WHERE active = 1 ORDER BY id ASC")
    active_plans = check_cursor.fetchall()
    check_conn.close()

    if active_plans:
        logger.info(f"📋 Current execution queue ({len(active_plans)} batch(es) pending):")
        for plan_id, p_month, p_time in active_plans:
            logger.info(f"   • Queue ID {plan_id}: Batch {p_month} (queued at {p_time})")

        if not auto_apply:
            print("\n📋 Active execution queue:")
            for plan_id, p_month, p_time in active_plans:
                print(f"   • Queue ID {plan_id}: Batch {p_month} (queued at {p_time})")
            
            queue_choice = input("\nOptions: [E]xecute queue now | [C]ontinue planning next batch | [R]eset/clear queue | [Q]uit [E/c/r/q]: ").strip().lower()
            if queue_choice == 'e':
                executor_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_executor.py")
                logger.info(f"🚀 Launching pipeline_executor for queued batches: {executor_path}")
                os.execv(sys.executable, [sys.executable, executor_path])
            elif queue_choice == 'r':
                reset_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
                reset_cursor = reset_conn.cursor()
                reset_cursor.execute("UPDATE planned_execution SET active = 0 WHERE active = 1")
                reset_conn.commit()
                reset_conn.close()
                logger.info("🗑️ Active execution queue cleared.")
                active_plans = []
            elif queue_choice == 'q':
                logger.info("Exiting planner.")
                sys.exit(0)
            else:
                logger.info("Proceeding to plan next batch...")

    # Run bootstrap steps before proceeding
    if not no_sync:
        acquire_planner_lock()
        run_bootstrap_steps(auto_apply, logger)
        release_planner_lock()
    else:
        logger.info("⚡ Fast Mode: Skipping bootstrap sync steps.")

    # Prompt for session mode: Batch Management, Memory Feature & Publishing, Media Cleanup, or Manage Device Owners
    if not auto_apply:
        print("\n--- 🛠️  Session Mode ---")
        mode = input("Select mode: [B] Batch Management (default) | [M] Memory Feature & Publishing | [C] Media Cleanup | [O] Manage Device Owners: ").strip().lower()
        if mode == 'm':
            run_memory_publishing_flow(None, None)
            sys.exit(0)
        elif mode == 'c':
            acquire_planner_lock()
            conn = get_connection()
            conn.execute("PRAGMA busy_timeout = 30000")
            cursor = get_cursor()
            display_media_cleanup_recommendations(cursor, verbose=True)
            close_conn()
            release_planner_lock()
            sys.exit(0)
        elif mode == 'o':
            manage_device_owners_flow(None, None)
            # Restart the script to return to the main menu clean
            os.execv(sys.executable, [sys.executable] + sys.argv)

    acquire_planner_lock()
    conn = get_connection()
    conn.execute("PRAGMA busy_timeout = 30000")
    cursor = get_cursor()

    # Check for completed batches that have new assets imported since their last update
    cursor.execute("""
        SELECT mb.month, mb.updated_at_utc, MAX(a.imported_date_utc), COUNT(a.asset_id)
        FROM month_batches mb
        JOIN assets a ON a.month = mb.month
        WHERE mb.status_code >= '600'
        GROUP BY mb.month
        HAVING MAX(a.imported_date_utc) > mb.updated_at_utc
    """)
    outdated_batches = cursor.fetchall()
    if outdated_batches:
        print("\n==================================================")
        print("🔄 Detected New Assets in Completed Batches")
        print("==================================================")
        print("The following processed/finalized batches have new imported photos:")
        for month, finalized_at, newest_import, asset_count in outdated_batches:
            print(f" - {month}: Finalized on {finalized_at}, Newest import: {newest_import}")
        
        for month, finalized_at, newest_import, asset_count in outdated_batches:
            if not auto_apply:
                reset_input = input(f"\nDo you want to reset batch {month} to status '000' (added) to re-process new assets? [y/N]: ").strip().lower()
                if reset_input == 'y':
                    cursor.execute("UPDATE month_batches SET status_code = '000', updated_at_utc = CURRENT_TIMESTAMP WHERE month = ?", (month,))
                    conn.commit()
                    logger.info(f"✅ Reset batch {month} to status '000'.")

    # Shared credentials for all Google API calls in this planner session
    creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)

    # Pre-fetch remote favorites to avoid repeated API calls during analysis
    remote_favs_cache = None
    try:
        logger.info("🌐 Fetching remote favorites from Google Photos API to verify curation status...")
        remote_favs_cache = get_all_favorites(creds)
        
        # Auto-run retroactive favorites sync for bypassed months
        try:
            cursor.execute("SELECT month FROM month_batches WHERE is_bypassed = 1")
            byp_months = [row[0] for row in cursor.fetchall()]
            if byp_months:
                logger.info(f"🔄 Auto-syncing late favorites for bypassed months: {byp_months}...")
                from google_photos import create_or_get_album
                from pull_google_favorites import get_album_items
                
                favorite_set = {(f.get('filename'), f.get('mediaMetadata', {}).get('creationTime')) for f in remote_favs_cache}
                for bm in byp_months:
                    album_title = f"Currently Curating - {bm}"
                    album_id = create_or_get_album(creds, album_title)
                    if album_id:
                        album_items = get_album_items(creds, album_id)
                        matched = [item for item in album_items
                                   if (item.get('filename'), item.get('mediaMetadata', {}).get('creationTime')) in favorite_set]
                        
                        update_count = 0
                        for item in matched:
                            filename = item.get('filename')
                            raw_creation_time = item.get('mediaMetadata', {}).get('creationTime', '')
                            creation_time = raw_creation_time.replace('T', ' ').split('.')[0] if raw_creation_time else ''
                            if filename and creation_time:
                                cursor.execute("""
                                    UPDATE assets
                                    SET google_favorite = 1, updated_at_utc = datetime('now')
                                    WHERE original_filename = ? AND date_created_utc = ? AND month = ? AND MomentsAlbumName IS NOT NULL AND google_favorite = 0
                                """, (filename, creation_time, bm))
                                if cursor.rowcount:
                                    update_count += 1
                        if update_count > 0:
                            conn.commit()
                            logger.info(f"⭐️ Retroactively synced {update_count} favorites for bypassed month {bm}.")
        except Exception as ex:
            logger.error(f"Error during bypassed batch auto-sync: {ex}")
            
    except Exception as e:
        logger.warning(f"Could not pre-fetch remote favorites: {e}")

    transitions = get_stage_transitions(cursor)
    batches = get_batch_statuses(cursor)

    display_summary(transitions, batches, cursor, remote_favs_cache)

    # Media cleanup recommendations for source cameras
    display_media_cleanup_recommendations(cursor, verbose=False)

    # Proactive check for new month readiness
    if batches:
        latest_month_str, latest_status = batches[0]  # Ordered DESC
        if str(latest_status) >= '600':
            now = datetime.now()
            current_month_str = now.strftime('%Y-%m')
            if latest_month_str < current_month_str:
                latest_dt = datetime.strptime(latest_month_str, '%Y-%m')
                next_dt = (latest_dt + timedelta(days=32)).replace(day=1)
                next_month_str = next_dt.strftime('%Y-%m')
                
                # Only suggest if the next month hasn't even started (not in batches)
                if next_month_str not in [b[0] for b in batches]:
                    logger.info(f"✨ Current pipeline progress: {latest_month_str} is complete.")
                    logger.info(f"💡 Suggestion: Ready to start {next_month_str}. Ensure all active sources ({', '.join(ACTIVE_CAMERA_MODELS)}) are imported into Apple Photos.")

    logger.info("=== ✅ Suggested Action ===")

    # Fetch all months in descending order, excluding the current calendar month
    # as it is considered incomplete for processing.
    # TODO - month selection also should be done after the transition type is determined 
    current_month_str = datetime.now().strftime('%Y-%m')
    cursor.execute("SELECT DISTINCT month FROM month_batches WHERE month < ? ORDER BY month DESC", (current_month_str,))
    months_descending = [row[0] for row in cursor.fetchall()]

    # Collect candidates for each transition type, across all months
    manual_candidates = []
    retryable_candidates = []
    pipeline_candidates = []

    for month in months_descending:
        month_status = None
        for m, s in batches:
            if m == month:
                month_status = s
                break
        if month_status is None:
            continue

        # Filter the pre-fetched transitions list for this month's status using string comparison
        transitions_for_month = [
            t for t in transitions 
            if str(t[1]) == str(month_status)
        ]

        # If in an error state (e.g., 400E), find the transition that was attempted (code=400)
        # so we can suggest a retry.
        if not transitions_for_month and str(month_status).endswith('E'):
            failed_code = str(month_status)[:-1]
            # Find the transition where the target code is the one that failed
            retry_candidates = [t for t in transitions if str(t[0]) == failed_code]
            for t in retry_candidates:
                logger.info(f"Found error state '{month_status}' for {month}. Suggesting retry of step {failed_code}.")
                # Treat as a retryable candidate to prioritize resolving the failure
                retryable_candidates.append((month, t))

        logger.debug(f"Inspecting transitions for month {month} with status {month_status}")
        for t in transitions_for_month:
            if t[3] == 'manual':
                logger.debug(f"Found manual transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                manual_candidates.append((month, t))
            elif t[3] == 'retryable':
                logger.debug(f"Found retryable transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                retryable_candidates.append((month, t))
            elif t[3] == 'pipeline':
                logger.debug(f"Found pipeline transition candidate for month {month}: {t[2]} (code {t[1]}) -> (code {t[0]})")
                
                is_delay = False
                if t[0] == '650':
                    cursor.execute("SELECT is_bypassed, bypass_timestamp FROM month_batches WHERE month = ?", (month,))
                    row_byp = cursor.fetchone()
                    if row_byp and row_byp[0] == 1 and row_byp[1]:
                        try:
                            bypass_dt = datetime.strptime(row_byp[1], "%Y-%m-%d %H:%M:%S")
                            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                            elapsed = (now_utc - bypass_dt).total_seconds() / 86400
                            if elapsed < 14:
                                is_delay = True
                                logger.info(f"⏳ Cleanup delayed for bypassed batch {month}. {14 - elapsed:.1f} days remaining in grace period.")
                        except Exception as e:
                            logger.error(f"Error parsing bypass_timestamp for {month}: {e}")
                
                if not is_delay:
                    pipeline_candidates.append((month, t))

    # Precedence: manual > retryable > pipeline. Sort by month descending to prioritize newer batches.
    manual_candidates.sort(key=lambda x: x[0], reverse=True)
    retryable_candidates.sort(key=lambda x: x[0], reverse=True)
    pipeline_candidates.sort(key=lambda x: x[0], reverse=True)

    selected_month = None
    selected_transition = None

    # Check active queue to skip already-queued candidates
    cursor.execute("SELECT planned_month FROM planned_execution WHERE active = 1")
    active_planned_set = set(row[0] for row in cursor.fetchall())

    logger.info("🔍 Evaluating manual transition candidates...")
    for month, transition in manual_candidates:
        if month in active_planned_set:
            logger.info(f"  ⏭️ Skipping {month} (already in planned execution queue).")
            continue
        selected_code, selected_prev, selected_desc, selected_type, short_label = transition
        logger.info(f"  Checking {month} ({selected_desc}, status {selected_prev})...")

        cursor.execute("SELECT MAX(updated_at_utc) FROM assets WHERE uploaded_to_google = 1 AND month = ?", (month,))
        result = cursor.fetchone()
        last_completed_at = result[0] if result else None
        elapsed_days = None
        if last_completed_at:
            try:
                last_dt = datetime.strptime(last_completed_at, "%Y-%m-%d %H:%M:%S")
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                elapsed_days = (now_utc - last_dt).total_seconds() / 86400
            except: pass

        if elapsed_days is not None and elapsed_days < 3:
            logger.info(f"    ⏸️ Too soon: Only {elapsed_days:.1f} days since upload. Need 3 days for Google AI curation.")
            continue

        fav_count, source, _ = check_favorites_count(cursor, month, check_remote=True, all_favs=remote_favs_cache, creds=creds)
        if fav_count == 0:
            if selected_prev == '500':
                logger.info(f"    ⏸️ Manual transition blocked: No favorites in Google Photos.")
                continue
            else:
                logger.warning(f"    ⚠️ No favorites found for {month}. Starring may not be complete.")
        else:
            logger.info(f"    ✨ Detected {fav_count} favorites ({source}).")

        if not auto_apply:
            if fav_count == 0:
                proceed_input = input(f"\nPlease confirm: has '{short_label}' task been completed for {month}? [y/N/bypass] (Choose 'bypass' to proceed with aesthetic ranking only): ").strip().lower()
            else:
                proceed_input = input(f"\nPlease confirm: has '{short_label}' task been completed for {month}? [y/N]: ").strip().lower()

            if proceed_input == 'y':
                cursor.execute("UPDATE month_batches SET status_code = ?, is_bypassed = 0, bypass_timestamp = NULL WHERE month = ?", (selected_code, month))
                conn.commit()
                logger.info(f"✅ Month {month} status updated to {selected_code}.")
                close_conn(); sys.exit(0)
            elif proceed_input == 'bypass' and fav_count == 0:
                now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("UPDATE month_batches SET status_code = ?, is_bypassed = 1, bypass_timestamp = ? WHERE month = ?", (selected_code, now_str, month))
                conn.commit()
                logger.info(f"✅ Month {month} status updated to {selected_code} (Direct-Rank Bypassed).")
                close_conn(); sys.exit(0)
            else:
                logger.info(f"  Skipped manual transition for {month}. Checking next candidate...")
                continue
        else:
            logger.info(f"  Auto-apply enabled; skipping manual task {month} for safety. Checking next candidate...")
            continue

    logger.info("🔍 Evaluating retryable transition candidates...")
    for month, transition in retryable_candidates:
        if month in active_planned_set:
            logger.info(f"  ⏭️ Skipping {month} (already in planned execution queue).")
            continue
        selected_code, selected_prev, selected_desc, selected_type, short_label = transition

        # Only perform space-based analysis and branching for batches that haven't finished 
        # their primary upload (stage < 400) and are targeting an upload operation.
        is_upload_retry = selected_code in ['399', '400'] and int(selected_prev) < 400
        if is_upload_retry:
            free_space = check_google_quota(creds=creds)
            if free_space is None:
                logger.error("❌ Error: Could not retrieve Google Drive quota."); close_conn(); sys.exit(1)

            import glob
            matched_folders = glob.glob(os.path.join(STAGING_ROOT, f"*{month}*"))
            if matched_folders:
                staging_folder = matched_folders[0]
                staging_size = 0
                staging_eligible_count = 0
                staging_oversized_count = 0
                for root, dirs, files in os.walk(staging_folder):
                    for f in files:
                        ext = os.path.splitext(f)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            fp = os.path.join(root, f)
                            fp_size = os.path.getsize(fp)
                            if fp_size <= MAX_UPLOAD_FILE_SIZE_BYTES:
                                staging_size += fp_size
                                staging_eligible_count += 1
                            else:
                                staging_oversized_count += 1
                logger.info(f"Staging folder content for {month}: {human_readable_size(staging_size)} eligible upload files ({staging_eligible_count} items <= {MAX_UPLOAD_FILE_SIZE_MB}MB, {staging_oversized_count} skipped).")
            else:
                staging_folder = None; staging_size = 0; logger.warning(f"No staging folder found for {month}")

            cursor.execute("SELECT COUNT(*), COUNT(CASE WHEN uploaded_to_google = 1 THEN 1 END) FROM assets WHERE month = ?", (month,))
            row_cnt = cursor.fetchone()
            total_db_assets = row_cnt[0] if row_cnt else 0
            uploaded_db_count = row_cnt[1] if row_cnt else 0

            cursor.execute("SELECT original_filename FROM assets WHERE month = ? AND uploaded_to_google = 1", (month,))
            uploaded_assets = cursor.fetchall()
            latest_upload_size = 0
            if uploaded_assets and staging_folder:
                for filename_tuple in uploaded_assets:
                    file_path = os.path.join(staging_folder, filename_tuple[0])
                    if os.path.exists(file_path):
                        latest_upload_size += os.path.getsize(file_path)
            logger.info(f"Upload progress: {uploaded_db_count}/{total_db_assets} assets ({human_readable_size(latest_upload_size)}) of {month} in Google Photos.")

            if staging_folder and staging_size > 0:
                remaining_to_upload = max(0, staging_size - latest_upload_size)
            else:
                remaining_to_upload = 0 if (total_db_assets > 0 and (uploaded_db_count >= total_db_assets or (staging_folder and (uploaded_db_count + staging_oversized_count) >= total_db_assets))) else 999999999999

            if total_db_assets > 0 and (uploaded_db_count >= total_db_assets or (staging_folder and (uploaded_db_count + staging_oversized_count) >= total_db_assets)) and remaining_to_upload == 0:
                logger.info(f"✅ All eligible assets for {month} appear to be uploaded already ({uploaded_db_count} uploaded, {staging_oversized_count} skipped > {MAX_UPLOAD_FILE_SIZE_MB}MB).")
                if auto_apply: proceed_transition = True
                else:
                    ans = input(f"All assets uploaded - transition {month} to 400 status? [y/N]: ").strip().lower()
                    proceed_transition = ans == 'y'
                if proceed_transition:
                    cursor.execute("UPDATE month_batches SET status_code = '400' WHERE month = ?", (month,))
                    conn.commit(); logger.info(f"Month {month} updated to 400."); close_conn(); sys.exit(0)
            elif staging_folder and free_space >= remaining_to_upload:
                logger.info(f"🚀 Found {human_readable_size(remaining_to_upload)} left to upload for {month}. "
                            f"Available space: {human_readable_size(free_space)}. Priority given to finishing this batch.")
                selected_month = month
                selected_transition = transition
                break
            else:
                logger.warning(f"⚠️ Insufficient space for {month}. Free: {human_readable_size(free_space)}, Need: {human_readable_size(remaining_to_upload)}.")
                
                # Branch and suggest cleanup for months at stage 600
                cleanup_candidates = [m for m, s in batches if str(s) == '600']
                if cleanup_candidates:
                    logger.info(f"💡 Suggestion: Drive cleanup available for processed months: {', '.join(cleanup_candidates)}")
                    for m_c, t_c in pipeline_candidates:
                        if m_c in cleanup_candidates and str(t_c[1]) == '600':
                            selected_month = m_c
                            selected_transition = t_c
                            logger.info(f"🔄 Branching to cleanup transition (600->650) for {selected_month} to free up space.")
                            break
                    if selected_month:
                        break
                continue

    if not selected_month:
        logger.info("🔍 Evaluating pipeline transition candidates...")
        available_pipeline_candidates = [
            (m, t) for m, t in pipeline_candidates if m not in active_planned_set
        ]
        if not available_pipeline_candidates:
            if active_planned_set:
                logger.info(f"All available pipeline candidates are already in the execution queue ({sorted(list(active_planned_set))}). Exiting.")
            else:
                logger.info("No pipeline transitions available. Exiting.")
            close_conn(); sys.exit(0)
        selected_month, selected_transition = available_pipeline_candidates[0]

    latest_month = selected_month
    selected_code, selected_prev, selected_desc, selected_type, short_label = selected_transition
    current_status = selected_prev

    if True:
        # Build the full transition path from current status, only including pipeline transitions
        full_transition_list = get_full_transition_path(
            [t for t in transitions if t[3] in ['pipeline', 'retryable']],
            str(current_status)
        )

        # Filter out delayed cleanup (600->650) for bypassed batches
        cursor.execute("SELECT is_bypassed, bypass_timestamp FROM month_batches WHERE month = ?", (latest_month,))
        row_byp = cursor.fetchone()
        if row_byp and row_byp[0] == 1 and row_byp[1]:
            try:
                bypass_dt = datetime.strptime(row_byp[1], "%Y-%m-%d %H:%M:%S")
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                elapsed = (now_utc - bypass_dt).total_seconds() / 86400
                if elapsed < 14:
                    # Delay cleanup
                    full_transition_list = [t for t in full_transition_list if not t.endswith('->650')]
            except Exception as e:
                logger.error(f"Error parsing bypass_timestamp for {latest_month} in full path build: {e}")

        # Only perform import continuity and sequencing checks for batches that haven't reached the upload stage (400)
        # This prevents redundant prompts for batches that are already processed or being curated.
        if current_status and str(current_status) < '400':
            # Check if Apple Photos Smart Album exists before proposing migration off of 000 / 100E
            if str(current_status) in ('000', '100E'):
                cursor.execute("SELECT COUNT(*) FROM smart_albums WHERE LOWER(album_name) = ?", (latest_month.lower(),))
                if cursor.fetchone()[0] == 0:
                    logger.warning(f"⚠️ Smart Album '{latest_month}' does not exist in Apple Photos.")
                    logger.info(f"👉 Please create the Smart Album '{latest_month}' inside 'Media Organizer on LaCie > Google Photos Pipeline > MonthlyExports' in Apple Photos first.")
                    if not auto_apply:
                        close_conn()
                        release_planner_lock()
                        ans = input("\nPress [Enter] once created to resync and restart the planner (or [Q] to quit): ").strip().lower()
                        if ans != 'q':
                            acquire_planner_lock()
                            conn = get_connection()
                            conn.execute("PRAGMA busy_timeout = 30000")
                            cursor = get_cursor()
                            logger.info("🔄 Forcing metadata resync and restarting planner...")
                            cursor.execute("UPDATE db_updates SET raw_synced = 0, derived_synced = 0")
                            conn.commit()
                            close_conn()
                            release_planner_lock()
                            os.execv(sys.executable, [sys.executable] + sys.argv)
                    logger.info("Then, re-run the pipeline planner to sync the changes and proceed.")
                    close_conn()
                    release_planner_lock()
                    sys.exit(0)

            check_active_sources_import_status(cursor, conn, latest_month, auto_apply)

            # --- Check sequencing before recording the plan ---
            if not verify_sequencing_for_planned_month(cursor, conn, latest_month, auto_apply):
                logger.warning(f"Sequencing not confirmed for {latest_month}. Aborting plan recording.")
                close_conn()
                sys.exit(0)

        logger.info(f"Run pipeline for: Month={latest_month}, Transitions={full_transition_list}")

        # --- Check for favorites readiness if transition involves pulling or ranking ---
        is_favorites_pull = any('550' in str(t) or 'Pull Google' in str(t) for t in full_transition_list)
        is_after_pull = any('Rank Assets' in str(t) or 'Ranking' in str(t) for t in full_transition_list)
        
        # Check if the batch is bypassed before printing favorites warnings
        cursor.execute("SELECT is_bypassed FROM month_batches WHERE month = ?", (latest_month,))
        row_byp = cursor.fetchone()
        is_byp = row_byp[0] if row_byp else 0

        if is_favorites_pull:
            if is_byp:
                logger.info(f"ℹ️ Batch {latest_month} is running in Direct-Rank Bypass mode (no remote favorites expected).")
            else:
                fav_count, source, fav_names = check_favorites_count(
                    cursor, latest_month, check_remote=True, 
                    all_favs=remote_favs_cache, creds=creds
                )
                if fav_count == 0:
                    logger.warning(f"⚠️ Suggested batch {latest_month} has no favorites in Google Photos yet.")
                else:
                    logger.info(f"✨ Batch {latest_month} is ready with {fav_count} favorites in Google Photos (Source: {source}).")
        elif is_after_pull:
            if is_byp:
                logger.info(f"ℹ️ Batch {latest_month} is running in Direct-Rank Bypass mode (no local favorites expected).")
            else:
                fav_count, source, fav_names = check_favorites_count(
                    cursor, latest_month, check_remote=False, 
                    all_favs=remote_favs_cache, creds=creds
                )
                if fav_count == 0:
                    logger.warning(f"⚠️ Suggested batch {latest_month} has 0 favorites in local DB (Source: {source}). Ranking steps may be skipped.")

        # --- Begin Google quota check for upload transitions ---
        # Determine if any transition in the pipeline represents an upload to Google (e.g., '210->399')
        quota_check_needed = any(
            isinstance(transition, str) and '210->399' in transition
            or (isinstance(transition, (list, tuple)) and len(transition) >= 1 and '210->399' in str(transition))
            for transition in full_transition_list
        )
        # --- End Google quota check (defer actual check to after user confirmation) ---

        if not auto_apply:
            proceed = input("Proceed with this plan? [y/N]: ")
            if proceed.strip().lower() != 'y':
                logger.info("Aborted by user.")
                close_conn()
                sys.exit(0)

        # Now, if quota check is needed, perform the actual quota check before executing
        if quota_check_needed:
            import glob
            matched_folders = glob.glob(os.path.join(STAGING_ROOT, f"*{latest_month}*"))
            if matched_folders:
                staging_folder = matched_folders[0]
                staging_size = 0
                staging_oversized_count = 0
                for root, dirs, files in os.walk(staging_folder):
                    for f in files:
                        ext = os.path.splitext(f)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            fp = os.path.join(root, f)
                            fp_size = os.path.getsize(fp)
                            if fp_size <= MAX_UPLOAD_FILE_SIZE_BYTES:
                                staging_size += fp_size
                            else:
                                staging_oversized_count += 1
                logger.info(f"Detected staging folder for month {latest_month}: {staging_folder}, eligible upload size: {human_readable_size(staging_size)} (skipped {staging_oversized_count} files > {MAX_UPLOAD_FILE_SIZE_MB}MB)")
            else:
                staging_folder = None
                staging_size = 0
                logger.warning(f"No staging folder found for month {latest_month}")
            free_space = check_google_quota(creds=creds)
            if free_space is None:
                logger.error("❌ Aborting: Could not retrieve Google Drive quota before upload.")
                close_conn()
                sys.exit(1)
            if free_space < staging_size:
                logger.warning(f"⚠️ Insufficient space: {human_readable_size(free_space)} available vs {human_readable_size(staging_size)} required.")
                
                # Perform estimation of how many assets will fit based on aesthetic score
                cursor.execute("SELECT original_filename, aesthetic_score FROM assets WHERE month = ?", (latest_month,))
                db_scores = {row[0].lower(): (row[1] or -1) for row in cursor.fetchall()}
                
                staging_files = []
                for root, _, fnames in os.walk(staging_folder):
                    for f in fnames:
                        fp = os.path.join(root, f)
                        staging_files.append((f, os.path.getsize(fp), db_scores.get(f.lower(), -1)))
                
                # Sort by score descending (highest ranked first)
                staging_files.sort(key=lambda x: x[2], reverse=True)
                
                can_upload_count = 0
                simulated_sum = 0
                for _, size, _ in staging_files:
                    if simulated_sum + size <= free_space:
                        simulated_sum += size
                        can_upload_count += 1
                    else:
                        break
                
                logger.warning(f"📊 Estimate: Only {can_upload_count} out of {len(staging_files)} assets will fit.")
                
                if not auto_apply:
                    partial_confirm = input(f"Proceed with a partial upload of the highest-ranked assets for {latest_month}? [y/N]: ")
                    if partial_confirm.strip().lower() != 'y':
                        logger.info("Pipeline transition aborted by user.")
                        close_conn()
                        sys.exit(0)
                    logger.info("User confirmed partial upload. Proceeding with plan...")
                else:
                    logger.error("❌ Auto-apply aborted: Insufficient space for full upload. Manual confirmation required for partial sync.")
                    close_conn()
                    sys.exit(1)
            else:
                logger.info(f"Enough Google Drive space available for upload. Free space: {human_readable_size(free_space)}, Staging size: {human_readable_size(staging_size)}")

        logger.info("🚀 Executing planned steps...")
        set_planned_month(cursor, latest_month)
        conn.commit()
        logger.info(f"📌 Month {latest_month} recorded in planned_execution for next pipeline run.")

        close_conn()
        release_planner_lock()

        if not auto_apply:
            exec_now = input("\n🚀 Would you like to start the pipeline executor now? [y/N]: ").strip().lower()
            if exec_now == 'y':
                executor_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_executor.py")
                logger.info(f"🚀 Launching pipeline_executor: {executor_path}")
                os.execv(sys.executable, [sys.executable, executor_path])

        if selected_type not in ['manual', 'retryable', 'pipeline']:
            logger.warning(f"Unknown transition type '{selected_type}' for current status {current_status}.")

    # TODO: trigger executor or store plan
    # TODO: Decide whether to implement quota filler strategy (partial month uploads).
    # Current pipeline assumes full-month atomicity (399 -> 400).

    close_conn()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-apply", action="store_true", help="Skip confirmation and apply plan immediately")
    parser.add_argument("--no-sync", action="store_true", help="Skip database copy and sync steps (fast mode)")
    args = parser.parse_args()
    main(auto_apply=args.auto_apply, no_sync=args.no_sync)
