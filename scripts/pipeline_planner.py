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
from constants import MEDIA_ORGANIZER_DB_PATH, APPLE_PHOTOS_DB_LOCK_PATH, APPLE_PHOTOS_DB_PATH, LOG_PATH, GOOGLE_PHOTOS_READONLY_SCOPES, GOOGLE_DRIVE_READ_ONLY_SCOPES, PLANNER_REQUIRED_SCOPES, CURATION_THRESHOLD_LOG_PATH, PUBLISHED_MOMENTS_LOG_PATH, SCORING_BREAKDOWN_LOG_PATH, MEDIA_CLEANUP_LOG_PATH, QUARTILE_CLEANUP_LOG_PATH, WEEKLY_MEMORY_LOG_PATH, PUBLISHING_RECOMMENDATIONS_LOG_PATH, MAX_UPLOAD_FILE_SIZE_BYTES, MAX_UPLOAD_FILE_SIZE_MB, BG_SERVICE_PID_PATH
from constants import ACTIVE_CAMERA_MODELS, DEVICE_OWNER_MAPPING, AESTHETIC_SCORE_WEIGHT, GOOGLE_FAVORITES_WEIGHT, APPLE_SELECTION_WEIGHT, APPLE_FEATURED_WEIGHT
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_stage_transitions, get_batch_statuses, get_latest_import_and_month
import requests
import math
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

def write_lock_file(status, pid, started_at_utc=None, latest_successful_refresh_utc="—"):
    try:
        if started_at_utc and not started_at_utc.endswith(" UTC"):
            started_at_utc = f"{started_at_utc} UTC"
        lock_data = {
            "status": status,
            "pid": pid,
            "started_at_utc": started_at_utc,
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

def stop_bg_service_on_exit():
    if os.path.exists(BG_SERVICE_PID_PATH):
        try:
            with open(BG_SERVICE_PID_PATH, "r") as f:
                service_pid = int(f.read().strip())
            if service_pid and is_pid_alive(service_pid):
                logger.info(f"Stopping background copy service (PID: {service_pid})...")
                print(f"🛑 Stopping background copy service (PID: {service_pid})...")
                import signal
                os.kill(service_pid, signal.SIGTERM)
                # Wait up to 3 seconds for the service script to exit and remove the PID file
                for _ in range(30):
                    time.sleep(0.1)
                    if not is_pid_alive(service_pid):
                        break
        except Exception as e:
            logger.warning(f"Error stopping background service: {e}")

def restart_planner():
    logger.info("🔄 Restarting planner to refresh status...")
    close_conn()
    release_planner_lock()
    os.execv(sys.executable, [sys.executable] + sys.argv)

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
            started_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
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

def ensure_views_exist(cursor, conn=None):
    """Ensure essential database views like ranked_assets_view exist."""
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='ranked_assets_view'")
        if not cursor.fetchone():
            logger.info("🛠️ ranked_assets_view missing. Creating view...")
            cursor.execute(f"""
                CREATE VIEW IF NOT EXISTS ranked_assets_view AS
                SELECT
                    a.asset_id,
                    a.original_filename,
                    a.month,
                    a.aesthetic_score,
                    a.google_favorite,
                    a.apple_favorite,
                    a.apple_photos_monthly_selection,
                    a.mobile_apple_photos_featured_photos,
                    (
                        (COALESCE(a.aesthetic_score, 0) * {AESTHETIC_SCORE_WEIGHT}) + 
                        (a.google_favorite * {GOOGLE_FAVORITES_WEIGHT}) + 
                        (a.apple_photos_monthly_selection * {APPLE_SELECTION_WEIGHT}) +
                        (a.mobile_apple_photos_featured_photos * {APPLE_FEATURED_WEIGHT})
                    ) AS score_normalized,
                    a.date_created_utc,
                    a.MomentsAlbumName
                FROM
                    assets a
                WHERE
                    a.ignore_continuity_check = 0 OR a.ignore_continuity_check IS NULL;
            """)
            if conn:
                conn.commit()
            logger.info("✅ ranked_assets_view successfully created.")
    except Exception as e:
        logger.error(f"Error ensuring views exist: {e}")

# Helper to run bootstrap steps
def run_bootstrap_steps(auto_apply, logger):
    """
    Run the bootstrap steps: only 1.0 Generate Batches is synchronous now.
    """
    try:
        bs_conn = get_connection()
        bs_cursor = get_cursor()
        ensure_views_exist(bs_cursor, bs_conn)
        close_conn()
    except Exception as e:
        logger.warning(f"Could not verify views during bootstrap: {e}")

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
    If the list is longer than 5 items, only the first 2 and bottom 2 items are displayed with an ellipsis in between.
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
    if len(assets) > 5:
        for fname, dt, uuid in assets[:2]:
            fname_str = fname if fname else "None"
            dt_str = dt if dt else "None"
            uuid_str = uuid if uuid else "None"
            print(f"  {fname_str:<{max_len}} | {dt_str:<19} | {uuid_str:<36}")
        print(f"  {'...':<{max_len}} | {'...':<19} | {'...':<36}")
        for fname, dt, uuid in assets[-2:]:
            fname_str = fname if fname else "None"
            dt_str = dt if dt else "None"
            uuid_str = uuid if uuid else "None"
            print(f"  {fname_str:<{max_len}} | {dt_str:<19} | {uuid_str:<36}")
    else:
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
    header_b = f"{'Month':<10} {'Status':<8} {'Assets':<8} {'Favorites':<10} {'Fav. Synced':<12}"
    print(header_b)
    print("-" * len(header_b))
    for row in batches:
        if len(row) >= 4:
            month, status, total_assets, fav_assets = row[:4]
            fav_synced = row[4] if len(row) > 4 else None
            assets_str = str(total_assets) if total_assets > 0 else "—"
            fav_str = str(fav_assets) if fav_assets > 0 else "—"
            synced_str = fav_synced[:10] if fav_synced else "—"
            print(f"{month:<10} {status:<8} {assets_str:<8} {fav_str:<10} {synced_str:<12}")
        else:
            month, status = row[:2]
            print(f"{month:<10} {status:<8}")

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
    ensure_views_exist(init_cursor, init_conn)

    # Create threshold_history table if it doesn't exist
    try:
        init_cursor.execute("""
            CREATE TABLE IF NOT EXISTS threshold_history (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at_utc    TEXT NOT NULL DEFAULT (datetime('now')),
                threshold_score     REAL NOT NULL,
                threshold_met       INTEGER DEFAULT 0,
                auto_synced         INTEGER DEFAULT 0,
                notes               TEXT
            )
        """)
        init_conn.commit()
        init_cursor.execute("PRAGMA table_info(threshold_history);")
        cols = [c[1] for c in init_cursor.fetchall()]
        if 'threshold_met' not in cols:
            init_cursor.execute("ALTER TABLE threshold_history ADD COLUMN threshold_met INTEGER DEFAULT 0;")
        if 'auto_synced' not in cols:
            init_cursor.execute("ALTER TABLE threshold_history ADD COLUMN auto_synced INTEGER DEFAULT 0;")
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

    # Fetch last known threshold_met status from previous planner run
    last_threshold_met = False
    try:
        init_cursor.execute("SELECT threshold_met FROM threshold_history WHERE threshold_met IS NOT NULL ORDER BY id DESC LIMIT 1")
        last_row = init_cursor.fetchone()
        if last_row is not None:
            last_threshold_met = bool(last_row[0])
            logger.info(f"Loaded previous threshold_met status from DB: {last_threshold_met}")
    except Exception as e:
        logger.warning(f"Could not fetch previous threshold_met status: {e}")

    generate_weekly_memory_report = False
    generate_skipped_videos = False
    auto_synced_needs_folder = False
    
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

        # Fetch all global skipped asset IDs from Apple Photos & MediaOrganizer
        skipped_asset_ids = set()
        if photos_db_attached:
            try:
                cursor.execute("""
                    SELECT DISTINCT z.ZUUID
                    FROM photos_db.ZASSET z
                    JOIN photos_db.Z_30ASSETS aa ON aa.Z_3ASSETS = z.Z_PK
                    JOIN photos_db.ZGENERICALBUM ga ON ga.Z_PK = aa.Z_30ALBUMS
                    WHERE LOWER(ga.ZTITLE) IN ('skippublishing', 'ignore')
                      AND ga.ZTRASHEDSTATE = 0 AND z.ZTRASHEDSTATE = 0
                """)
                skipped_asset_ids = set(r[0] for r in cursor.fetchall() if r[0])
            except Exception as e:
                logger.warning(f"Could not load skipped assets from Photos DB: {e}")
        try:
            cursor.execute("SELECT asset_id FROM assets WHERE LOWER(COALESCE(MomentsAlbumName, '')) IN ('skippublishing', 'ignore')")
            for r in cursor.fetchall():
                if r[0]:
                    skipped_asset_ids.add(r[0])
        except Exception:
            pass

        # Check and cleanup stale publication records:
        # Reset publication status to unpublished if an asset no longer belongs to any moment,
        # belongs to SkipPublishing/Ignore, or its moment assignment changed.
        try:
            cursor.execute("""
                SELECT p.id, p.asset_id, p.moment_name, a.MomentsAlbumName, a.original_filename
                FROM publications p
                LEFT JOIN assets a ON p.asset_id = a.asset_id
                WHERE a.asset_id IS NULL
                   OR a.MomentsAlbumName IS NULL 
                   OR a.MomentsAlbumName = ''
                   OR LOWER(a.MomentsAlbumName) IN ('skippublishing', 'ignore')
                   OR p.moment_name != a.MomentsAlbumName
            """)
            stale_pubs = cursor.fetchall()
            if stale_pubs:
                stale_ids = [r[0] for r in stale_pubs]
                logger.info(f"🧹 Resetting {len(stale_ids)} stale publication records to unpublished because assets no longer belong to moment...")
                cursor.execute(f"DELETE FROM publications WHERE id IN ({','.join(['?']*len(stale_ids))})", stale_ids)
                conn.commit()
        except Exception as e:
            logger.warning(f"Error checking/cleaning stale publications: {e}")

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

        # Update running historical_min if this is the first recorded threshold or it is smaller
        if historical_min == 0.0 or (cutoff_score > 0.0 and cutoff_score < historical_min):
            historical_min = cutoff_score

        # Check if thresholds are different (using 1e-6 to avoid minor float precision issues)
        thresholds_different = False
        if historical_min > 0.0 and cutoff_score > 0.0:
            thresholds_different = (cutoff_score - historical_min > 1e-6)

        had_threshold_mismatch = False
        if thresholds_different:
            had_threshold_mismatch = True
            # Record that threshold is currently not met
            if cutoff_score > 0.0:
                try:
                    cursor.execute("INSERT INTO threshold_history (threshold_score, threshold_met, auto_synced) VALUES (?, 0, 0)", (cutoff_score,))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Could not record threshold in history: {e}")

        # Loop until threshold matches the historical minimum target
        if thresholds_different:
            while thresholds_different:
                threshold_report = []
                threshold_report.append("==================================================")
                threshold_report.append("📊 Curation Threshold Status")
                threshold_report.append("==================================================")
                threshold_report.append(f" - Current dynamic threshold:  {cutoff_score:.4f}")
                if historical_min > 0.0:
                    threshold_report.append(f" - Historical minimum target:  {historical_min:.4f}")
                    threshold_report.append(f"👉 Note: Please assign moments to assets in new batches until the threshold reaches {historical_min:.4f} again.")
                threshold_report.append("==================================================\n")

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

                print('\n' + '\n'.join(threshold_report))
                try:
                    with open(CURATION_THRESHOLD_LOG_PATH, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(threshold_report) + '\n')
                except Exception as e:
                    logger.warning(f"Could not write curation threshold log: {e}")

                # Close database connection and release lock before action prompt
                if photos_db_attached:
                    try:
                        cursor.execute("DETACH DATABASE photos_db")
                    except Exception:
                        pass
                close_conn()
                release_planner_lock()

                print("\n--- Actions (Threshold Mismatch - Curation/Publishing Disabled) ---")
                print(" [1] Sync proposed assets to ToBeCurated albums in Apple Photos")
                print(" [Enter] Re-evaluate & Refresh threshold")
                print(" [E] Exit")
                
                choice = input("\nSelect action [Or Press Enter to Refresh]: ").strip().lower()
                if choice == 'e':
                    logger.info("Exiting memory publishing flow.")
                    return
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
                elif choice in ('2', '3'):
                    print("⚠️ Curation/publishing actions are disabled because threshold is not aligned with historical minimum target.")
                elif choice == '' or choice == 'r':
                    # Refresh: re-acquire lock and connection
                    acquire_planner_lock()
                    conn = get_connection()
                    conn.execute("PRAGMA busy_timeout = 30000")
                    cursor = get_cursor()
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                        
                    photos_db_attached = False
                    try:
                        cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
                        photos_db_attached = True
                    except Exception as e:
                        logger.warning(f"Could not attach Photos.sqlite for Apple moment lookup: {e}")
                    
                    # Query cutoff_score again
                    cutoff_score = 0.0
                    if photos_db_attached:
                        try:
                            cursor.execute("""
                                SELECT v.score_normalized 
                                FROM ranked_assets_view v
                                JOIN month_batches mb ON v.month = mb.month
                                LEFT JOIN photos_db.ZASSET a ON a.ZUUID = v.asset_id
                                WHERE mb.status_code >= '600' AND (v.MomentsAlbumName IS NULL OR v.MomentsAlbumName = '') 
                                ORDER BY v.score_normalized DESC LIMIT 1
                            """)
                            row = cursor.fetchone()
                            cutoff_score = row[0] if row and row[0] is not None else 0.0
                        except Exception:
                            pass
                    logger.info(f"Re-evaluated Cutoff threshold score: {cutoff_score:.4f}")
                    
                    # Re-evaluate thresholds_different
                    if historical_min > 0.0 and cutoff_score > 0.0:
                        thresholds_different = (cutoff_score - historical_min > 1e-6)
                    else:
                        thresholds_different = False

            # If the loop finished (threshold is now aligned), we restore/acquire the database lock/connection
            # so the rest of the function can run cleanly
            acquire_planner_lock()
            conn = get_connection()
            conn.execute("PRAGMA busy_timeout = 30000")
            cursor = get_cursor()
            try:
                conn.rollback()
            except Exception:
                pass
            photos_db_attached = False
            try:
                cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
                photos_db_attached = True
            except Exception as e:
                logger.warning(f"Could not attach Photos.sqlite for Apple moment lookup: {e}")

        # Determine effective cutoff threshold to use for selecting qualified moments in the table
        effective_threshold = cutoff_score
        if historical_min > 0.0:
            effective_threshold = min(cutoff_score, historical_min) if cutoff_score > 0.0 else historical_min

        # Automatic sync trigger when threshold is met for the first time (previously unmet on last run or in this session)
        should_auto_sync = (not last_threshold_met) or had_threshold_mismatch
        if should_auto_sync:
            logger.info("🔄 Threshold has just been met (previously unmet). Automatically syncing proposed assets to Apple Photos (Option [1])...")
            print("\n🔄 Threshold is met! Automatically syncing proposed assets to ToBeCurated in Apple Photos (Option [1])...")
            acquire_planner_lock()
            script_dir = os.path.dirname(os.path.abspath(__file__))
            try:
                subprocess.run([sys.executable, os.path.join(script_dir, "create_apple_moments_albums.py")], check=True)
                logger.info("✅ Automatic sync to ToBeCurated complete.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Automatic sync failed: {e}")
            release_planner_lock()

            last_threshold_met = True
            had_threshold_mismatch = False

            if cutoff_score > 0.0:
                try:
                    cursor.execute("INSERT INTO threshold_history (threshold_score, threshold_met, auto_synced) VALUES (?, 1, 1)", (cutoff_score,))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Could not record threshold history: {e}")
        else:
            # Steady state: threshold was already met on last run
            if cutoff_score > 0.0:
                try:
                    cursor.execute("INSERT INTO threshold_history (threshold_score, threshold_met, auto_synced) VALUES (?, 1, 0)", (cutoff_score,))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Could not record threshold history: {e}")

        # Normal Flow (Threshold Aligned)
        # Build and write Curation Threshold Status report for aligned state
        threshold_report = []
        threshold_report.append("==================================================")
        threshold_report.append("📊 Curation Threshold Status")
        threshold_report.append("==================================================")
        threshold_report.append(f" - Current dynamic threshold:  {cutoff_score:.4f}")
        if historical_min > 0.0:
            threshold_report.append(f" - Historical minimum target:  {historical_min:.4f}")
            threshold_report.append("🎉 Threshold aligned! Current threshold matches or is below historical minimum.")
        else:
            threshold_report.append(" - Historical minimum target:  None (No history recorded yet)")
        threshold_report.append("==================================================\n")
        
        print('\n' + '\n'.join(threshold_report))
        try:
            with open(CURATION_THRESHOLD_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(threshold_report) + '\n')
        except Exception as e:
            logger.warning(f"Could not write curation threshold log: {e}")

        # Query published moments / folders with stats (only when aligned)
        published_moments_report = []
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
            ORDER BY AVG(v.score_normalized) DESC NULLS LAST, MAX(p.published_at_utc) DESC, p.moment_name ASC
        """)
        published_folders = cursor.fetchall()

        published_moments_report.append("================================================--------------------------------------------------------------------------------")
        published_moments_report.append("🌟 Published Moments / Folders & Stats")
        published_moments_report.append("================================================--------------------------------------------------------------------------------")
        if not published_folders:
            published_moments_report.append("ℹ️  No published moments recorded yet in database.\n")
        else:
            published_moments_report.append("The following moments have been curated and published:")
            pub_header = f"{'No.':<4} {'Moment Name':<30} {'Published At (Local)':<22} {'Assets':<8} {'Avg Score':<11} {'Score Range':<17} {'Capture Dates':<24} {'Camera Sources'}"
            published_moments_report.append(pub_header)
            published_moments_report.append("-" * len(pub_header))
            for idx, p_row in enumerate(published_folders, 1):
                p_name_raw = p_row[0] or "—"
                p_name = p_name_raw[:26] + "..." if len(p_name_raw) > 29 else p_name_raw
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
                published_moments_report.append(f"{idx:<4} {p_name:<30} {p_date_str:<22} {p_count:<8} {p_avg:<11} {score_rng:<17} {date_rng:<24} {c_srcs}")
            published_moments_report.append("================================================--------------------------------------------------------------------------------\n")

        try:
            with open(PUBLISHED_MOMENTS_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(published_moments_report) + '\n')
        except Exception as e:
            logger.warning(f"Could not write published moments stats log: {e}")

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
        start_time_scoring = time.time()
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

        duration_scoring = time.time() - start_time_scoring
        try:
            with open(SCORING_BREAKDOWN_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(scoring_report) + '\n')
            print(f"📄 Qualified Assets Scoring Breakdown ({len(processed_rows)} assets) saved to: {SCORING_BREAKDOWN_LOG_PATH} (took {duration_scoring:.2f}s)\n")
        except Exception as e:
            logger.warning(f"Could not write scoring breakdown log: {e}")
        
        start_time_weekly = time.time()
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

        # 3. Query Apple Photos albums and folders inside Curated, ToBeCurated, and Moments (to match existence and get counts)
        to_be_curated_albums = {}
        curated_albums = {}
        moments_albums = {}
        if photos_db_attached:
            try:
                cursor.execute("""
                    SELECT 
                        COALESCE(p2.ZTITLE, p.ZTITLE) as root_parent,
                        COALESCE(p.ZTITLE, '') as direct_parent,
                        ga.ZTITLE as album_name, 
                        COUNT(aa.Z_3ASSETS) as asset_count
                    FROM photos_db.ZGENERICALBUM ga
                    LEFT JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = ga.Z_PK
                    LEFT JOIN photos_db.ZGENERICALBUM p ON ga.ZPARENTFOLDER = p.Z_PK
                    LEFT JOIN photos_db.ZGENERICALBUM p2 ON p.ZPARENTFOLDER = p2.Z_PK
                    WHERE (p.ZTITLE IN ('Curated', 'ToBeCurated', 'Moments') OR p2.ZTITLE IN ('Curated', 'ToBeCurated', 'Moments'))
                      AND ga.ZTRASHEDSTATE = 0 AND ga.ZKIND <> 1507
                    GROUP BY ga.Z_PK
                """)
                for root_p, dir_p, a_name, a_count in cursor.fetchall():
                    if not a_name:
                        continue
                    a_name_clean = a_name.strip()
                    target_folder = 'Curated' if ('Curated' in (root_p, dir_p) and 'ToBeCurated' not in (root_p, dir_p)) else ('ToBeCurated' if 'ToBeCurated' in (root_p, dir_p) else ('Moments' if 'Moments' in (root_p, dir_p) else ''))
                    if target_folder == 'ToBeCurated':
                        to_be_curated_albums[a_name_clean] = a_count
                    elif target_folder == 'Curated':
                        curated_albums[a_name_clean] = a_count
                    elif target_folder == 'Moments':
                        moments_albums[a_name_clean] = a_count
            except Exception as e:
                logger.warning(f"Could not query Apple Photos albums from photos_db: {e}")
        else:
            applescript_code = """
            tell application "Photos"
                set results to {}
                set parentFolderNames to {"Curated", "ToBeCurated", "Moments"}
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
                            set nestedAlbums to albums of aFolder
                            repeat with anAlbum in nestedAlbums
                                set aNestedName to name of anAlbum
                                try
                                    set aCount to count of media items of anAlbum
                                on error
                                    set aCount to 0
                                end try
                                copy (fName & "|" & aNestedName & "|" & (aCount as string)) to end of results
                            end repeat
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
                                elif folder_name_clean == 'Moments':
                                    moments_albums[album_name_clean] = item_count
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
        ranked_moments = []
        for name, data in moments_data.items():
            target_scores = data['unpublished_scores']
            avg_score = sum(target_scores) / len(target_scores) if target_scores else 0.0
            stage = stages.get(name, 'M100')
            
            # Check Apple Photos existence and count
            name_stripped = name.strip()
            to_be_curated_count = to_be_curated_albums.get(name_stripped, to_be_curated_albums.get(name, 0))
            to_be_curated_exists = (name_stripped in to_be_curated_albums or name in to_be_curated_albums) and (to_be_curated_count > 0)
            curated_exists = (name_stripped in curated_albums or name in curated_albums)

            # Check filesystem curated directory existence
            fs_curated_path_orig = os.path.join(CURATED_LACIE_DIR, name)
            fs_curated_path_strip = os.path.join(CURATED_LACIE_DIR, name_stripped)
            fs_curated_exists = os.path.exists(fs_curated_path_orig) or os.path.exists(fs_curated_path_strip)
            fs_curated_path = fs_curated_path_orig if os.path.exists(fs_curated_path_orig) else fs_curated_path_strip
            
            # Count-weighted rank score to prevent small/single-asset moments from dominating
            rank_score = avg_score * math.log(data['total_qualified'] + 1)
            
            p_data = pub_info.get(name, {})
            last_pub_raw = p_data.get('last_pub_utc')
            pub_count = p_data.get('pub_count', 0)
            pub_avg = p_data.get('pub_avg')
            pub_min = p_data.get('pub_min')
            pub_max = p_data.get('pub_max')

            # If moment is marked as M500 (fully published) in database, but new curated assets were added
            # (curated_count > pub_count), demote it back to M450 (or M400 if nothing published yet).
            if stage == 'M500' and data['curated_count'] > pub_count:
                new_stage = 'M450' if pub_count > 0 else 'M400'
                logger.info(f"🔄 Demoting moment '{name}' from M500 to {new_stage} in database because new curated assets were added (Curated: {data['curated_count']}, Published: {pub_count})")
                try:
                    cursor.execute("UPDATE curated_moments SET memory_stage = ? WHERE moment_name = ?", (new_stage, name))
                    conn.commit()
                    stage = new_stage
                except Exception as e:
                    logger.error(f"Failed to update stage for moment '{name}': {e}")

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
            days_remaining = 0
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
                        days_remaining = max(1, 30 - diff.days)
            
            has_unpublished = (fs_curated_exists and data['curated_count'] > 0 and data['curated_count'] > pub_count)
            if not has_unpublished:
                can_publish_str = "❌ No"
            elif too_recent:
                can_publish_str = f"❌ (in {days_remaining} day{'s' if days_remaining != 1 else ''})"
            else:
                can_publish_str = "✅ Yes"
            
            # Determine asset count to display (use filesystem count if curated folder exists,
            # fallback to database curated count if present, otherwise total qualified proposed assets)
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
            elif name_stripped in curated_albums:
                # Use count from Apple Photos Curated album if available (before filesystem export)
                assets_display = str(curated_albums[name_stripped])
            elif data['curated_count'] > 0:
                assets_display = str(data['curated_count'])
            else:
                assets_display = str(data['total_qualified'])

            # Compare Apple Photos Curated album assets with local filesystem folder contents
            curated_str = "❌ No"
            if curated_exists and fs_curated_exists:
                if generate_weekly_memory_report:
                    # Retrieve Apple Photos Curated album asset base names from Photos DB (excluding skipped assets)
                    photos_bases = set()
                    if photos_db_attached:
                        try:
                            cursor.execute("""
                                SELECT DISTINCT aaa.ZORIGINALFILENAME, a.ZUUID
                                FROM photos_db.ZGENERICALBUM ga
                                JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = ga.Z_PK
                                JOIN photos_db.ZASSET a ON aa.Z_3ASSETS = a.Z_PK
                                JOIN photos_db.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
                                LEFT JOIN photos_db.ZGENERICALBUM p ON ga.ZPARENTFOLDER = p.Z_PK
                                WHERE (ga.ZTITLE = ? OR ga.ZTITLE = ?) AND ga.ZTRASHEDSTATE = 0 AND ga.ZKIND <> 1507
                                  AND p.ZTITLE = 'Curated'
                                  AND a.ZTRASHEDSTATE = 0
                            """, (name, name_stripped))
                            photos_bases = set(os.path.splitext(row[0])[0].lower() for row in cursor.fetchall() if row[0] and row[1] not in skipped_asset_ids)
                        except Exception as e:
                            logger.warning(f"Error querying Photos curated album assets for {name}: {e}")

                    if photos_db_attached and photos_bases:
                        if photos_bases == fs_bases:
                            curated_str = "✅ Yes"
                        else:
                            logger.info(f"🔄 Auto-fixing mismatch for moment '{name}' by running Option [2] (Export)...")
                            script_dir = os.path.dirname(os.path.abspath(__file__))
                            
                            # Ensure CURATED_LACIE_DIR destination folder exists
                            dest_folder = os.path.join(CURATED_LACIE_DIR, name)
                            os.makedirs(dest_folder, exist_ok=True)
                            
                            try:
                                # Run export_curated_album.py synchronously to export Curated album to local folder
                                subprocess.run([sys.executable, os.path.join(script_dir, "export_curated_album.py"), name], check=True)
                                logger.info(f"✅ Auto-export complete for '{name}'. Re-evaluating folder contents...")
                                
                                # Re-read the filesystem folder contents
                                all_files = []
                                if os.path.exists(fs_curated_path):
                                    all_files = [f for f in os.listdir(fs_curated_path) 
                                                 if os.path.isfile(os.path.join(fs_curated_path, f)) 
                                                 and not f.startswith('.')]
                                fs_bases = set(os.path.splitext(f)[0].lower() for f in all_files)
                                fs_count = len(fs_bases)
                                assets_display = str(fs_count)
                                data['curated_count'] = fs_count
                                
                                # Compare again
                                if photos_bases == fs_bases:
                                    curated_str = "✅ Yes"
                                else:
                                    curated_str = "⚠️  Mismatch (Auto-fix failed)"
                            except Exception as e:
                                logger.error(f"Auto-export failed for '{name}': {e}")
                                curated_str = "⚠️  Mismatch"
                    else:
                        curated_str = "✅ Yes"
                else:
                    curated_str = "✅ Yes"
            elif curated_exists and not fs_curated_exists:
                curated_str = "📁 Needs Folder"
            elif not curated_exists and fs_curated_exists:
                curated_str = "📁 Local Only"

            # Calculate display stage based on current status
            if stage == 'M500' or (pub_count >= data['total_qualified'] and data['total_qualified'] > 0):
                display_stage = 'M500'
            elif stage == 'M450' or pub_count > 0:
                display_stage = 'M450'
            elif stage == 'M400' or fs_curated_exists:
                display_stage = 'M400'
            elif curated_exists:
                display_stage = 'M300'
            elif to_be_curated_exists:
                display_stage = 'M200'
            else:
                display_stage = 'M100'

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
                'display_stage': display_stage,
                'to_be_curated_exists': to_be_curated_exists,
                'to_be_curated_count': to_be_curated_count,
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

        # Determine table title and filter console_moments based on presence of M200/M300 moments
        has_pending_curation = any(m['display_stage'] in ('M200', 'M300') for m in ranked_moments)
        if has_pending_curation:
            console_moments = [m for m in ranked_moments if m['display_stage'] in ('M200', 'M300')]
            table_title = "🌟 M200: Proposed Moments in ToBeCurated (Require Curation & Move to Curated)"
        else:
            console_moments = list(ranked_moments)
            table_title = "🌟 Weekly Memory Feature & Publishing (Mode [M])"

        # If all moments in M200 table require folder creation ("📁 Needs Folder"), automatically run Option [1]
        all_needs_folder = (
            has_pending_curation and 
            bool(console_moments) and 
            all(m['curated_str'] == "📁 Needs Folder" for m in console_moments)
        )
        if all_needs_folder:
            if not auto_synced_needs_folder:
                logger.info("🔄 All moments in M200 table require folders ('📁 Needs Folder'). Automatically running Option [1] (Sync proposed assets to ToBeCurated in Apple Photos)...")
                print("\n🔄 All moments in M200 table require folders ('📁 Needs Folder'). Automatically syncing proposed assets to ToBeCurated albums in Apple Photos (Option [1])...")
                if photos_db_attached:
                    try:
                        cursor.execute("DETACH DATABASE photos_db")
                    except Exception:
                        pass
                close_conn()
                release_planner_lock()

                acquire_planner_lock()
                script_dir = os.path.dirname(os.path.abspath(__file__))
                try:
                    subprocess.run([sys.executable, os.path.join(script_dir, "create_apple_moments_albums.py")], check=True)
                    logger.info("✅ Automatic sync to ToBeCurated complete.")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Automatic sync failed: {e}")
                release_planner_lock()

                auto_synced_needs_folder = True
                continue
        else:
            auto_synced_needs_folder = False

        header_m = f"{'No.':<4} {'Moment Name':<30} {'Status':<8} {'Rank Score':<12} {'Avg Score':<10} {'Min Score':<10} {'Max Score':<10} {'Assets':<8} {'Pub.':<6} {'Pub. Avg':<10} {'Pub. Range':<17} {'ToBeCurated?':<13} {'Curated?':<15} {'Published?':<13} {'Can Publish?':<18} {'Last Published':<18}"

        table_lines = []
        divider_width = max(len(header_m), 105)
        table_lines.append("\n" + "=" * divider_width)
        table_lines.append(table_title)
        table_lines.append("=" * divider_width)
        if has_pending_curation:
            table_lines.append("The following moments/folders were added to ToBeCurated and require curation (move selection to Curated once complete):")
        
        # Sort console moments by:
        # 1. Status M300 first (ready for export)
        # 2. Needs update (proposed + curated < total_qualified)
        # 3. If needs update: rank score descending; if up-to-date: average score descending
        console_moments.sort(key=lambda x: (
            x['display_stage'] == 'M300',
            (x['proposed_count'] + x['curated_count']) < x['total_qualified'],
            x['rank_score'] if ((x['proposed_count'] + x['curated_count']) < x['total_qualified']) else x['avg_score']
        ), reverse=True)
        
        table_lines.append(header_m)
        table_lines.append("-" * len(header_m))
        divider_printed = False
        for idx, m in enumerate(console_moments, 1):
            displayed_moments_map[idx] = {'name': m['name'], 'type': 'ranked_moment'}
            is_needs_update = (m['proposed_count'] + m['curated_count']) < m['total_qualified']
            if not is_needs_update and not divider_printed:
                if idx > 1:
                    table_lines.append("-" * len(header_m))
                    table_lines.append(f"--- Up-To-Date Moments " + "-" * (len(header_m) - 23))
                    table_lines.append("-" * len(header_m))
                divider_printed = True
                
            has_tbc = m['to_be_curated_exists']
            to_be_curated_str = "✅ Yes" if has_tbc else "❌ No"
            if (m['proposed_count'] + m['curated_count']) < m['total_qualified'] and has_tbc:
                to_be_curated_str = "🔄 Update needed"
                
            curated_str = m['curated_str']
            if m['pub_count'] == 0:
                published_str = "❌ No"
            elif m['pub_count'] >= m['total_qualified'] or (m['stage'] == 'M500' and m['pub_count'] >= m['curated_count']):
                published_str = "✅ Yes"
            else:
                published_str = f"🔄 Part ({m['pub_count']})"
                
            m_name_raw = m['name'] or "—"
            m_name = m_name_raw[:26] + "..." if len(m_name_raw) > 29 else m_name_raw
            
            table_lines.append(f"{idx:<4} {m_name:<30} {m['display_stage']:<8} {m['rank_score']:<12.4f} {m['avg_score']:<10.4f} {m['min_score']:<10.4f} {m['max_score']:<10.4f} {m['assets_display']:<8} {m['pub_display']:<6} {m['pub_avg_str']:<10} {m['pub_range_str']:<17} {to_be_curated_str:<13} {curated_str:<15} {published_str:<13} {m['can_publish_str']:<18} {m['last_pub_str']:<18}")

        if has_pending_curation:
            table_lines.append("\n👉 Next Steps: Inspect the proposed assets in Apple Photos 'ToBeCurated/[MomentName]', curate your selection, and manually move/copy them to 'Curated/[MomentName]' when done.")

        duration_weekly = time.time() - start_time_weekly
        try:
            os.makedirs(os.path.dirname(WEEKLY_MEMORY_LOG_PATH), exist_ok=True)
            with open(WEEKLY_MEMORY_LOG_PATH, 'w', encoding='utf-8') as f:
                f.write("\n".join(table_lines) + "\n")
        except Exception as e:
            logger.warning(f"Could not write weekly memory log: {e}")

        if table_title == "🌟 Weekly Memory Feature & Publishing (Mode [M])":
            if generate_weekly_memory_report:
                print(f"📄 Weekly Memory Feature & Publishing report saved to: {WEEKLY_MEMORY_LOG_PATH} (took {duration_weekly:.2f}s)\n")
        else:
            print("\n".join(table_lines))

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

        start_time_recommendations = time.time()
        # Display Weekly Memory Publishing Recommendations (only if all M200/M300 curation & export moments are complete)
        published_assets_by_moment = {}
        if not has_pending_curation:
            cursor.execute("SELECT asset_id, moment_name FROM publications")
            for aid, mom_name in cursor.fetchall():
                if mom_name not in published_assets_by_moment:
                    published_assets_by_moment[mom_name] = set()
                published_assets_by_moment[mom_name].add(aid)

        recommendations = []
        for m in (ranked_moments if not has_pending_curation else []):
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
                
            # Query database scores for all assets strictly assigned to this moment under Moments (excluding skipped assets)
            cursor.execute("""
                SELECT original_filename, score_normalized, asset_id 
                FROM ranked_assets_view 
                WHERE MomentsAlbumName = ?
            """, (name,))
            db_assets = [r for r in cursor.fetchall() if r[2] not in skipped_asset_ids]
            
            # Map base name to highest score and keep asset ID
            base_scores = {}
            base_asset_ids = {}
            for orig_fname, score, asset_id in db_assets:
                if orig_fname:
                    base_orig = os.path.splitext(orig_fname)[0].lower()
                    if score > base_scores.get(base_orig, -1.0):
                        base_scores[base_orig] = score
                        base_asset_ids[base_orig] = asset_id
                    
            # Filter unique bases in folder strictly to assets that belong to this moment and are not skipped
            valid_bases = [b for b in unique_bases if b in base_scores and base_asset_ids.get(b) not in skipped_asset_ids]
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
            rec_lines = []
            rec_lines.append("\n==================================================================================================================================================================")
            rec_lines.append("📢 Publishing Recommendations (Top 12 Actionable & Top 10 Disjoint Candidates)")
            rec_lines.append("==================================================================================================================================================================")
            rec_lines.append(f"{'No.':<4} {'Moment Name':<30} {'Avg Score':<10} {'Files':<6} {'Pub.':<5} {'Rec.':<5} {'Recommendation/Action':<40} {'Recommended Assets / Suggested Merge'}")
            rec_lines.append("-" * 168)
            divider_printed = False
            start_idx = len(ranked_moments) + 1
            for idx, rec in enumerate(top_recommendations, start_idx):
                displayed_moments_map[idx] = {'name': rec['name'], 'type': 'recommendation', 'rec_bases': rec['rec_bases'], 'action': rec['action']}
                if rec['action'].startswith("Disjoint") and not divider_printed:
                    rec_lines.append("-" * 168)
                    rec_lines.append(f"--- Disjoint Moments (Need Merge) " + "-" * 134)
                    rec_lines.append("-" * 168)
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
                rec_lines.append(f"{idx:<4} {rec['name']:<30} {rec['avg_score']:<10.4f} {rec['total_unique']:<6} {rec['pub_count']:<5} {rec['rec_count']:<5} {rec['action']:<40} {assets_str}")
            rec_lines.append("==================================================================================================================================================================\n")

            duration_recommendations = time.time() - start_time_recommendations
            try:
                os.makedirs(os.path.dirname(PUBLISHING_RECOMMENDATIONS_LOG_PATH), exist_ok=True)
                with open(PUBLISHING_RECOMMENDATIONS_LOG_PATH, 'w', encoding='utf-8') as f:
                    f.write("\n".join(rec_lines) + "\n")
                print(f"📄 Publishing Recommendations report saved to: {PUBLISHING_RECOMMENDATIONS_LOG_PATH} (took {duration_recommendations:.2f}s)\n")
            except Exception as e:
                logger.warning(f"Could not write publishing recommendations log: {e}")

            # Display Curated Moments Pending Publishing Table
            pending_publishing_moments = []
            combined_pending_display = []
            raw_pending_moments = [m for m in ranked_moments if m['display_stage'] in ('M400', 'M450')]
            
            for m in raw_pending_moments:
                # Gather files in source folder to map base names (to copy Live Photos, etc)
                fs_curated_path = os.path.join(CURATED_LACIE_DIR, m['name'])
                base_to_files = {}
                if m['fs_curated_exists']:
                    try:
                        for f in os.listdir(fs_curated_path):
                            if os.path.isfile(os.path.join(fs_curated_path, f)) and not f.startswith('.'):
                                base, ext = os.path.splitext(f)
                                base_lower = base.lower()
                                if base_lower not in base_to_files:
                                    base_to_files[base_lower] = []
                                base_to_files[base_lower].append(f)
                    except Exception:
                        pass
                
                # Fetch scores and original filenames of curated assets pending publishing (strictly excluding skipped assets)
                cursor.execute("""
                    SELECT v.asset_id, v.score_normalized, v.original_filename
                    FROM moment_exports me
                    JOIN ranked_assets_view v ON me.asset_id = v.asset_id 
                    WHERE (me.album_name = ? OR me.album_name = ?)
                      AND me.curation_stage = 'curated'
                      AND me.asset_id NOT IN (SELECT asset_id FROM publications)
                    ORDER BY v.score_normalized DESC
                """, (m['name'], m['name'].strip()))
                all_curated_pending = [r for r in cursor.fetchall() if r[0] not in skipped_asset_ids]
                
                # Filter to assets matching files in the local curated folder
                valid_curated_pending = []
                for aid, score, fname in all_curated_pending:
                    base = os.path.splitext(fname)[0].lower() if fname else ""
                    if base in base_to_files:
                        valid_curated_pending.append((aid, score, fname))
                
                pending = len(valid_curated_pending)
                published = m['pub_count']
                curated = len(base_to_files) if base_to_files else (pending + published)
                if pending < 9:
                    propose = pending
                else:
                    propose = min(9, math.ceil(pending / 4))
                
                top_proposed = valid_curated_pending[:propose]
                pending_scores = [r[1] for r in top_proposed if r[1] is not None]
                proposed_filenames = [r[2] for r in top_proposed if r[2] is not None]
                avg_proposed = sum(pending_scores) / len(pending_scores) if pending_scores else 0.0
                
                proposed_files = []
                for fname in proposed_filenames:
                    if fname:
                        base = os.path.splitext(fname)[0].lower()
                        proposed_files.extend(base_to_files.get(base, []))
                
                # Gather files of already published assets for this moment (to include in Publishing Recommendation)
                published_files = []
                cursor.execute("""
                    SELECT a.original_filename
                    FROM publications p
                    JOIN assets a ON p.asset_id = a.asset_id
                    WHERE (p.moment_name = ? OR p.moment_name = ?)
                """, (m['name'], m['name'].strip()))
                for (p_fname,) in cursor.fetchall():
                    if p_fname:
                        base = os.path.splitext(p_fname)[0].lower()
                        published_files.extend(base_to_files.get(base, []))
                
                # Combine published files and newly proposed files (preserve order & uniqueness)
                sync_files_set = set()
                sync_files = []
                for f in published_files + proposed_files:
                    if f not in sync_files_set:
                        sync_files_set.add(f)
                        sync_files.append(f)
                
                pending_publishing_moments.append({
                    'moment': m,
                    'curated': curated,
                    'published': published,
                    'pending': pending,
                    'propose': propose,
                    'pending_scores': pending_scores,
                    'avg_proposed': avg_proposed,
                    'proposed_files': proposed_files,
                    'published_files': published_files,
                    'sync_files': sync_files
                })
                
            # Map sorted order index for all albums under Curated in Apple Photos
            sorted_curated_names = sorted(curated_albums.keys())
            total_curated_count = len(sorted_curated_names)
            curated_name_to_index = {name: idx for idx, name in enumerate(sorted_curated_names, 1)}

            # Identify stale curated albums in Apple Photos that do not exist in Moments folder
            stale_curated_moments = []
            if moments_albums:
                for c_name, c_count in sorted(curated_albums.items()):
                    if c_name not in moments_albums and c_name.lower() not in ('skippublishing', 'ignore'):
                        cursor.execute("SELECT COUNT(DISTINCT asset_id) FROM publications WHERE (moment_name = ? OR moment_name = ?)", (c_name, c_name.strip()))
                        pub_row = cursor.fetchone()
                        p_count = pub_row[0] if pub_row else 0
                        
                        cursor.execute("""
                            SELECT AVG(v.score_normalized)
                            FROM moment_exports me
                            JOIN ranked_assets_view v ON me.asset_id = v.asset_id
                            WHERE (me.album_name = ? OR me.album_name = ?) AND me.curation_stage = 'curated'
                        """, (c_name, c_name.strip()))
                        score_row = cursor.fetchone()
                        s_avg = score_row[0] if score_row and score_row[0] is not None else 0.0
                        
                        stale_curated_moments.append({
                            'name': c_name,
                            'curated_count': c_count,
                            'pub_count': p_count,
                            'avg_score': s_avg,
                            'sorted_index': curated_name_to_index.get(c_name, 0),
                            'total_curated': total_curated_count
                        })

            # Sort stale curated albums by their sorted index in Apple Photos Curated folder
            stale_curated_moments.sort(key=lambda x: x['sorted_index'])

            stale_names = set(s['name'] for s in stale_curated_moments)
            active_pending_moments = [x for x in pending_publishing_moments if x['moment']['name'] not in stale_names]

            # 1a. Publishable Moments with New Assets in ToBeCurated (Action Needed: Curation & Move to Curated)
            publishable_tbc = [
                x for x in active_pending_moments 
                if x['moment']['total_qualified'] >= 2 and x['moment']['can_publish_str'] == "✅ Yes" and x['pending'] > 0
                and x['moment']['to_be_curated_exists']
            ]
            publishable_tbc.sort(key=lambda x: x['avg_proposed'], reverse=True)
            top_publishable_tbc = publishable_tbc[:20]

            # 1b. Publishable Moments Fully Curated (No Assets in ToBeCurated)
            publishable_clean = [
                x for x in active_pending_moments 
                if x['moment']['total_qualified'] >= 2 and x['moment']['can_publish_str'] == "✅ Yes" and x['pending'] > 0
                and not x['moment']['to_be_curated_exists']
            ]
            publishable_clean.sort(key=lambda x: x['avg_proposed'], reverse=True)
            top_publishable_clean = publishable_clean[:20]

            # 2. Pending Curation Moments (Can Publish: No, total_qualified >= 2, pending > 0, has new assets in ToBeCurated in Photos)
            pending_curation_actionable = [
                x for x in active_pending_moments 
                if x['moment']['total_qualified'] >= 2 and x['moment']['can_publish_str'] != "✅ Yes" and x['pending'] > 0
                and x['moment']['to_be_curated_exists']
            ]
            pending_curation_actionable.sort(key=lambda x: x['avg_proposed'], reverse=True)
            top_pending_curation = pending_curation_actionable[:20]

            # 3. Pending Time Restriction Moments (Can Publish: No, total_qualified >= 2, pending > 0, NO assets in ToBeCurated)
            pending_cooldown_actionable = [
                x for x in active_pending_moments 
                if x['moment']['total_qualified'] >= 2 and x['moment']['can_publish_str'] != "✅ Yes" and x['pending'] > 0
                and not x['moment']['to_be_curated_exists']
            ]
            pending_cooldown_actionable.sort(key=lambda x: x['avg_proposed'], reverse=True)
            top_pending_cooldown = pending_cooldown_actionable[:20]

            # 4. Disjoint Moments (total_qualified < 2, pending > 0)
            disjoint_pending = [
                x for x in active_pending_moments 
                if x['moment']['total_qualified'] < 2 and x['pending'] > 0
            ]
            disjoint_pending.sort(key=lambda x: x['avg_proposed'], reverse=True)
            top_disjoint = disjoint_pending[:20]

            combined_pending_display = top_publishable_tbc + top_publishable_clean + top_pending_curation + top_pending_cooldown + top_disjoint

            has_items_to_display = bool(top_publishable_tbc or top_publishable_clean or top_pending_curation or top_pending_cooldown or top_disjoint or stale_curated_moments)

            if has_items_to_display:
                start_idx_pp = len(console_moments) + 1
                if top_recommendations:
                    start_idx_pp += len(top_recommendations)

                print("==================================================================================================================================================================")
                print("🌟 Curated Moments Pending Publishing (Top 20 Publishable, Top 20 Pending Curation, Top 20 Time-Restricted & Disjoint Candidates)")
                print("==================================================================================================================================================================")
                print(f"{'No.':<4} {'Moment Name':<30} {'Status':<8} {'Avg Score':<10} {'Curated':<8} {'Published':<10} {'Pending':<8} {'Can Publish?':<16} {'Propose Next Publishing':<24} {'Proposed Asset Scores / Apple Photos Position'}")
                print("-" * 168)
                
                curr_p_idx = start_idx_pp

                def print_pending_row(p_idx, entry, show_tbc_count=False):
                    m = entry['moment']
                    curated = entry['curated']
                    published = entry['published']
                    pending = entry['pending']
                    propose = entry['propose']
                    pending_scores = entry['pending_scores']
                    avg_proposed = entry['avg_proposed']
                    tbc_count = m.get('to_be_curated_count', 0)
                    
                    if pending < 9:
                        propose_str = f"All ({pending})"
                    else:
                        propose_str = f"{propose} (1/4 of {pending})"
                    
                    scores_str = ", ".join(f"{s:.4f}" for s in pending_scores) if pending_scores else "—"
                    
                    if show_tbc_count and tbc_count > 0:
                        unit = "asset" if tbc_count == 1 else "assets"
                        right_col_str = f"ToBeCurated: {tbc_count} {unit} | {scores_str}"
                    else:
                        right_col_str = scores_str
                    
                    m_name_raw = m['name'] or "—"
                    m_name = m_name_raw[:26] + "..." if len(m_name_raw) > 29 else m_name_raw
                    print(f"{p_idx:<4} {m_name:<30} {m['display_stage']:<8} {avg_proposed:<10.4f} {curated:<8} {published:<10} {pending:<8} {m['can_publish_str']:<16} {propose_str:<24} {right_col_str}")

                # Subsection 1: Ready for Publishing (New Assets in ToBeCurated)
                if top_publishable_tbc:
                    sub_title = f"--- 🚀 Ready for Publishing (New Assets in ToBeCurated - Top {len(top_publishable_tbc)}) "
                    print(sub_title + "-" * max(0, 168 - len(sub_title)))
                    for entry in top_publishable_tbc:
                        m = entry['moment']
                        rec_bases = [os.path.splitext(f)[0].lower() for f in entry['proposed_files'] if f]
                        displayed_moments_map[curr_p_idx] = {
                            'name': m['name'],
                            'type': 'pending_publishing',
                            'rec_bases': rec_bases,
                            'action': 'Publishing'
                        }
                        print_pending_row(curr_p_idx, entry, show_tbc_count=True)
                        curr_p_idx += 1

                # Subsection 2: Ready for Publishing (No Assets in ToBeCurated)
                if top_publishable_clean:
                    if top_publishable_tbc:
                        print("-" * 168)
                    sub_title = f"--- 🚀 Ready for Publishing (No Assets in ToBeCurated - Top {len(top_publishable_clean)}) "
                    print(sub_title + "-" * max(0, 168 - len(sub_title)))
                    for entry in top_publishable_clean:
                        m = entry['moment']
                        rec_bases = [os.path.splitext(f)[0].lower() for f in entry['proposed_files'] if f]
                        displayed_moments_map[curr_p_idx] = {
                            'name': m['name'],
                            'type': 'pending_publishing',
                            'rec_bases': rec_bases,
                            'action': 'Publishing'
                        }
                        print_pending_row(curr_p_idx, entry)
                        curr_p_idx += 1

                # Subsection 3: Pending Curation (New Assets in ToBeCurated)
                if top_pending_curation:
                    print("-" * 168)
                    sub_title = f"--- ⏳ Pending Curation (New Assets in ToBeCurated - Top {len(top_pending_curation)}) "
                    print(sub_title + "-" * max(0, 168 - len(sub_title)))
                    for entry in top_pending_curation:
                        m = entry['moment']
                        rec_bases = [os.path.splitext(f)[0].lower() for f in entry['proposed_files'] if f]
                        displayed_moments_map[curr_p_idx] = {
                            'name': m['name'],
                            'type': 'pending_publishing',
                            'rec_bases': rec_bases,
                            'action': 'Publishing'
                        }
                        print_pending_row(curr_p_idx, entry, show_tbc_count=True)
                        curr_p_idx += 1

                # Subsection 3: Pending Time Restriction (Cooldown / Timing Delay)
                if top_pending_cooldown:
                    print("-" * 168)
                    sub_title = f"--- ⏱️ Pending Time Restriction (Cooldown Delay - Top {len(top_pending_cooldown)}) "
                    print(sub_title + "-" * max(0, 168 - len(sub_title)))
                    for entry in top_pending_cooldown:
                        m = entry['moment']
                        rec_bases = [os.path.splitext(f)[0].lower() for f in entry['proposed_files'] if f]
                        displayed_moments_map[curr_p_idx] = {
                            'name': m['name'],
                            'type': 'pending_publishing',
                            'rec_bases': rec_bases,
                            'action': 'Publishing'
                        }
                        print_pending_row(curr_p_idx, entry)
                        curr_p_idx += 1

                # Subsection 3: Disjoint Moments (Need Merge)
                if top_disjoint:
                    print("-" * 168)
                    sub_title = f"--- 🔗 Disjoint Moments (Need Merge - Top {len(top_disjoint)}) "
                    print(sub_title + "-" * max(0, 168 - len(sub_title)))
                    for entry in top_disjoint:
                        m = entry['moment']
                        rec_bases = [os.path.splitext(f)[0].lower() for f in entry['proposed_files'] if f]
                        displayed_moments_map[curr_p_idx] = {
                            'name': m['name'],
                            'type': 'pending_publishing',
                            'rec_bases': rec_bases,
                            'action': 'Publishing'
                        }
                        print_pending_row(curr_p_idx, entry)
                        curr_p_idx += 1

                # Subsection 4: Stale Curated Albums
                if stale_curated_moments:
                    print("-" * 168)
                    print(f"--- ⚠️ Stale Curated Albums (Not in Moments - Require Manual Deletion from Curated in Apple Photos) " + "-" * 73)
                    print("-" * 168)
                    for s_item in stale_curated_moments:
                        s_name_raw = s_item['name'] or "—"
                        s_name = s_name_raw[:26] + "..." if len(s_name_raw) > 29 else s_name_raw
                        s_avg_str = f"{s_item['avg_score']:.4f}" if s_item['avg_score'] > 0 else "—"
                        s_curated = s_item['curated_count']
                        s_published = s_item['pub_count']
                        displayed_moments_map[curr_p_idx] = {
                            'name': s_item['name'],
                            'type': 'stale_curated',
                            'rec_bases': [],
                            'action': 'Stale: Delete from Curated in Apple Photos'
                        }
                        pos_str = f"Curated #{s_item['sorted_index']} of {s_item['total_curated']} (Not in Moments)"
                        print(f"{curr_p_idx:<4} {s_name:<30} {'⚠️ Stale':<8} {s_avg_str:<10} {s_curated:<8} {s_published:<10} {'—':<8} {'❌ Stale':<16} {'🗑️ Delete in Photos':<24} {pos_str}")
                        curr_p_idx += 1

                print("==================================================================================================================================================================\n")

            # Display Skipped Videos Table
            if generate_skipped_videos:
                if photos_db_attached:
                    try:
                        cursor.execute("""
                            SELECT 
                                a.original_filename,
                                a.month,
                                COALESCE(v.score_normalized, 0.0) as score,
                                a.date_created_utc,
                                za.Z_PK,
                                a.uploaded_to_google
                            FROM assets a
                            JOIN photos_db.ZASSET za ON za.ZUUID = a.asset_id
                            JOIN photos_db.Z_30ASSETS aa ON aa.Z_3ASSETS = za.Z_PK
                            JOIN photos_db.ZGENERICALBUM ga ON ga.Z_PK = aa.Z_30ALBUMS
                            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
                            WHERE ga.ZTITLE = 'Google Upload Skipped Videos'
                              AND ga.ZTRASHEDSTATE = 0
                              AND za.ZTRASHEDSTATE = 0
                            ORDER BY v.score_normalized DESC NULLS LAST
                            LIMIT 15
                        """)
                        skipped_rows = cursor.fetchall()
                        if skipped_rows:
                            print("==================================================================================================================================================================")
                            print("🎥 Skipped Videos - Curation & Score Ranking (Google Upload Skipped Videos - Top 15)")
                            print("==================================================================================================================================================================")
                            print(f"{'No.':<4} {'Video Filename':<30} {'Month':<10} {'Avg Score':<11} {'Capture Date & Time':<24} {'Uploaded?':<11} {'Suggested Moment'}")
                            print("-" * 168)
                            for s_idx, (fname, smonth, sscore, sdate, z_pk, sup) in enumerate(skipped_rows, 1):
                                # Query all other albums this asset is in
                                cursor.execute("""
                                    SELECT ga.ZTITLE 
                                    FROM photos_db.Z_30ASSETS aa 
                                    JOIN photos_db.ZGENERICALBUM ga ON ga.Z_PK = aa.Z_30ALBUMS 
                                    WHERE aa.Z_3ASSETS = ? 
                                      AND ga.ZTRASHEDSTATE = 0
                                """, (z_pk,))
                                albums = [r[0] for r in cursor.fetchall() if r[0] != 'Google Upload Skipped Videos']
                                
                                suggested_moment = "—"
                                valid_albums = []
                                for name in albums:
                                    if name and re.match(r'^\d{4}(?:-\d{2})?(?:-\d{2})?(?:\b|\s|-)', name):
                                        valid_albums.append(name.strip())
                                if valid_albums:
                                    # Sort by date specificity: YYYY-MM-DD > YYYY-MM > YYYY, then length
                                    def prefix_specificity(name):
                                        if re.match(r'^\d{4}-\d{2}-\d{2}', name):
                                            return 3
                                        if re.match(r'^\d{4}-\d{2}', name):
                                            return 2
                                        return 1
                                    valid_albums.sort(key=lambda x: (-prefix_specificity(x), -len(x)))
                                    suggested_moment = valid_albums[0]
                                    
                                uploaded_str = "✅ Yes" if sup == 1 else "❌ No"
                                print(f"{s_idx:<4} {fname:<30} {smonth:<10} {sscore:<11.4f} {sdate:<24} {uploaded_str:<11} {suggested_moment}")
                            print("==================================================================================================================================================================\n")
                    except Exception as e:
                        logger.warning(f"Error querying skipped videos: {e}")
    
            # Sync folders and files to 'Publishing Recommendation' directory
            PUBLISHING_RECOMMENDATION_DIR = "/Volumes/LaCie/Media Organizer/Publishing Recommendation"
            os.makedirs(PUBLISHING_RECOMMENDATION_DIR, exist_ok=True)
            active_rec_names = set()
            
            print("📂 Syncing files to 'Publishing Recommendation' folder...")
            import shutil
            
            for entry in combined_pending_display:
                files_to_sync = entry.get('sync_files') or entry['proposed_files']
                if not files_to_sync:
                    continue
                    
                moment_name = entry['moment']['name']
                active_rec_names.add(moment_name)
                
                src_folder = os.path.join(CURATED_LACIE_DIR, moment_name)
                dest_folder = os.path.join(PUBLISHING_RECOMMENDATION_DIR, moment_name)
                os.makedirs(dest_folder, exist_ok=True)
                
                expected_files_set = set(files_to_sync)
                
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
                for f in files_to_sync:
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
            generate_weekly_memory_report = False
            generate_skipped_videos = False

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
        print(" [2] Export Curated Moment for Publishing")
        print(" [3] Record publication in the database (Mark as Published to Shutterfly/YouTube)")
        print(" [4] Generate Weekly Memory report (on demand)")
        print(" [5] Display Skipped Videos Table (on demand)")
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
        elif choice == '4':
            generate_weekly_memory_report = True
            continue
        elif choice == '5':
            generate_skipped_videos = True
            continue
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
                    item_info = displayed_moments_map[idx]
                    moment_name = item_info['name']
                    if item_info.get('type') == 'stale_curated':
                        print(f"⚠️ '{moment_name}' is a stale album in Curated that does not exist in Moments. Please manually delete it from Apple Photos Curated folder.")
                        continue
            
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
                    if item_info.get('type') == 'stale_curated':
                        print(f"⚠️ Cannot publish '{moment_name}': Album does not exist in Moments folder. Please manually delete it from Apple Photos Curated folder.")
                        continue
                    if item_info['type'] in ('recommendation', 'pending_publishing'):
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
            curated_assets_info = [r for r in cursor.fetchall() if r[0] not in skipped_asset_ids]
            
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
                    new_stage = 'M500' if total_pub_after >= len(curated_assets_info) else ('M450' if total_pub_after > 0 else 'M400')

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

def resolve_device_owner(cursor, camera_model, asset_date=None):
    """
    Looks up the owner of a camera model. Checks database overrides first
    (matching asset_date if provided against start_date and end_date),
    then defaults to DEVICE_OWNER_MAPPING, then 'Shared/Other'.
    """
    try:
        if asset_date:
            date_str = str(asset_date).split(' ')[0].split('T')[0]
            cursor.execute("""
                SELECT owner_name FROM device_owners
                WHERE camera_model = ?
                  AND (start_date IS NULL OR start_date <= ?)
                  AND (end_date IS NULL OR end_date >= ?)
                ORDER BY 
                    CASE 
                        WHEN start_date IS NOT NULL AND end_date IS NOT NULL THEN 1
                        WHEN start_date IS NOT NULL OR end_date IS NOT NULL THEN 2
                        ELSE 3 
                    END ASC,
                    updated_at_utc DESC
                LIMIT 1
            """, (camera_model, date_str, date_str))
            row = cursor.fetchone()
            if row:
                return row[0], "Database Override"

        # If no asset_date or no date-specific interval matched, fall back to ongoing or latest
        cursor.execute("""
            SELECT owner_name FROM device_owners
            WHERE camera_model = ?
            ORDER BY 
                CASE WHEN end_date IS NULL THEN 1 ELSE 2 END ASC,
                updated_at_utc DESC
            LIMIT 1
        """, (camera_model,))
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
    Interactive flow to view and edit owners and ownership date intervals of camera devices.
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

        # Fetch all device ownership records from DB
        db_ownerships = {}
        try:
            cursor.execute("""
                SELECT id, camera_model, owner_name, start_date, end_date, notes 
                FROM device_owners 
                ORDER BY COALESCE(start_date, '0000-00-00') ASC, id ASC
            """)
            for row in cursor.fetchall():
                c_mod = row[1]
                if c_mod not in db_ownerships:
                    db_ownerships[c_mod] = []
                db_ownerships[c_mod].append({
                    'id': row[0],
                    'owner_name': row[2],
                    'start_date': row[3],
                    'end_date': row[4],
                    'notes': row[5]
                })
        except Exception as e:
            logger.debug(f"Could not load device_owners records: {e}")

        # Merge with constants DEVICE_OWNER_MAPPING
        db_models = list(set(list(counts_dict.keys()) + list(db_ownerships.keys())))
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
            
            periods = db_ownerships.get(model, [])
            if periods:
                src_type = "Database Override"
                period_strs = []
                for p in periods:
                    s_str = p['start_date'] or ''
                    e_str = p['end_date'] or 'Present' if p['start_date'] else ''
                    if s_str or e_str:
                        date_span = f" ({s_str or '...'} -> {e_str or 'Present'})"
                    else:
                        date_span = ""
                    period_strs.append(f"{p['owner_name']}{date_span}")
                owner_display = "; ".join(period_strs)
            elif model in DEVICE_OWNER_MAPPING:
                src_type = "Default Mapping"
                owner_display = DEVICE_OWNER_MAPPING[model]
            else:
                src_type = "Default Fallback"
                owner_display = "Shared/Other"

            models_list.append({
                'model': model,
                'count': count,
                'min_filename': min_filename,
                'min_created': min_created,
                'max_filename': max_filename,
                'max_created': max_created,
                'owner_display': owner_display,
                'periods': periods,
                'src_type': src_type
            })

        # Sort by asset count ascending, then model name ascending to keep most-used at the bottom
        models_list.sort(key=lambda x: (x['count'], x['model']))

        print("\n" + "=" * 175)
        print("👤  MANAGE DEVICE PRIMARY OWNERS & OWNERSHIP PERIODS")
        print("=" * 175)
        print(f"{'No.':<4} {'Device Camera Model':<34} {'Asset Count':<13} {'Earliest Asset (Date)':<26} {'Latest Asset (Date)':<26} {'Owners & Ownership Dates':<52} {'Source Type':<16}")
        print("-" * 175)

        for idx, item in enumerate(models_list, 1):
            earliest_str = f"{item['min_filename']} ({item['min_created']})" if item['min_filename'] != '—' else '—'
            latest_str = f"{item['max_filename']} ({item['max_created']})" if item['max_filename'] != '—' else '—'
            print(f"{idx:<4} {item['model']:<34} {item['count']:<13,} {earliest_str:<26} {latest_str:<26} {item['owner_display']:<52} {item['src_type']:<16}")

        print("-" * 175)

        # Timeline grouped by Owner across all ownership periods
        timeline_by_owner = {}
        for item in models_list:
            if item['periods']:
                for p in item['periods']:
                    own = p['owner_name']
                    if own not in timeline_by_owner:
                        timeline_by_owner[own] = []
                    timeline_by_owner[own].append({
                        'model': item['model'],
                        'count': item['count'],
                        'start_date': p['start_date'] or '—',
                        'end_date': p['end_date'] or 'Present',
                        'earliest_asset': item['min_created'],
                        'latest_asset': item['max_created']
                    })

        if timeline_by_owner:
            print("\n" + "=" * 130)
            print("👤  DEVICE TIMELINE BY OWNER (DATABASE OVERRIDES)")
            print("=" * 130)
            print(f"{'Primary Owner':<16} {'Device Camera Model':<34} {'Ownership Period':<26} {'Asset Count':<14} {'Earliest Asset':<15} {'Latest Asset':<15}")
            print("-" * 130)

            for owner in sorted(timeline_by_owner.keys()):
                entries = timeline_by_owner[owner]
                entries.sort(key=lambda x: x['start_date'] if x['start_date'] != '—' else (x['earliest_asset'] if x['earliest_asset'] != '—' else '9999-12-31'))
                
                first_row = True
                for ent in entries:
                    owner_col = owner if first_row else ""
                    period_str = f"{ent['start_date']} -> {ent['end_date']}"
                    print(f"{owner_col:<16} {ent['model']:<34} {period_str:<26} {ent['count']:<14,} {ent['earliest_asset']:<15} {ent['latest_asset']:<15}")
                    first_row = False
            print("-" * 130)

        # Detach and release before waiting for user action prompts
        if photos_db_attached:
            try:
                cursor.execute("DETACH DATABASE photos_db")
            except Exception:
                pass
        close_conn()
        release_planner_lock()

        choice = input("\nOptions: [E]dit device owners & periods | [O]wners registry | [B]ack to main menu: ").strip().lower()
        if choice == 'b' or not choice:
            break
        elif choice == 'o':
            # Manage owners registry
            while True:
                acquire_planner_lock()
                conn = get_connection()
                cursor = get_cursor()
                cursor.execute("SELECT id, name FROM owners ORDER BY name ASC")
                registered_owners = cursor.fetchall()
                close_conn()
                release_planner_lock()

                print("\n" + "=" * 60)
                print("👥  REGISTERED OWNERS REGISTRY")
                print("=" * 60)
                for o_idx, (o_id, o_name) in enumerate(registered_owners, 1):
                    print(f"  {o_idx}. {o_name}")
                print("-" * 60)
                o_choice = input("Options: [A]dd owner | [R]ename owner | [B]ack: ").strip().lower()
                if o_choice in ('b', ''):
                    break
                elif o_choice == 'a':
                    new_name = input("Enter new owner name: ").strip()
                    if new_name:
                        acquire_planner_lock()
                        conn = get_connection()
                        cursor = get_cursor()
                        try:
                            cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES (?)", (new_name,))
                            conn.commit()
                            print(f"✅ Added '{new_name}' to owners registry.")
                        except Exception as e:
                            print(f"⚠️ Could not add owner: {e}")
                        close_conn()
                        release_planner_lock()
                elif o_choice == 'r':
                    r_num = input(f"Enter owner number to rename (1-{len(registered_owners)}): ").strip()
                    try:
                        r_idx = int(r_num) - 1
                        if 0 <= r_idx < len(registered_owners):
                            old_name = registered_owners[r_idx][1]
                            renamed = input(f"Enter new name for '{old_name}': ").strip()
                            if renamed:
                                acquire_planner_lock()
                                conn = get_connection()
                                cursor = get_cursor()
                                cursor.execute("UPDATE owners SET name = ? WHERE name = ?", (renamed, old_name))
                                cursor.execute("UPDATE device_owners SET owner_name = ? WHERE owner_name = ?", (renamed, old_name))
                                conn.commit()
                                print(f"✅ Renamed '{old_name}' to '{renamed}' across registry and device records.")
                                close_conn()
                                release_planner_lock()
                    except ValueError:
                        print("⚠️ Invalid selection.")
        elif choice == 'e':
            num_input = input(f"Enter device number to manage (1-{len(models_list)}) or Q to cancel: ").strip()
            if num_input.lower() == 'q':
                continue
            try:
                num = int(num_input)
                if num < 1 or num > len(models_list):
                    print("⚠️ Invalid number selection.")
                    continue
                selected_item = models_list[num - 1]
                selected_model = selected_item['model']

                # Submenu for managing periods of this specific device
                while True:
                    acquire_planner_lock()
                    conn = get_connection()
                    cursor = get_cursor()
                    cursor.execute("""
                        SELECT id, owner_name, start_date, end_date, notes 
                        FROM device_owners 
                        WHERE camera_model = ?
                        ORDER BY COALESCE(start_date, '0000-00-00') ASC, id ASC
                    """, (selected_model,))
                    current_periods = cursor.fetchall()
                    
                    cursor.execute("SELECT name FROM owners ORDER BY name ASC")
                    known_owners = [r[0] for r in cursor.fetchall()]
                    close_conn()
                    release_planner_lock()

                    print("\n" + "=" * 80)
                    print(f"📱 MANAGE DEVICE: {selected_model}")
                    print(f"   Total Assets: {selected_item['count']:,} | Span: {selected_item['min_created']} to {selected_item['max_created']}")
                    print("=" * 80)
                    if not current_periods:
                        print("   (No database overrides. Using default fallback)")
                    else:
                        print("Current Ownership Periods:")
                        for p_idx, (p_id, p_owner, p_start, p_end, p_notes) in enumerate(current_periods, 1):
                            s_text = p_start or "Beginning"
                            e_text = p_end or "Present"
                            notes_text = f" [{p_notes}]" if p_notes else ""
                            print(f"  [{p_idx}] Owner: {p_owner:<16} | Range: {s_text} to {e_text}{notes_text}")

                    print("-" * 80)
                    print("Options: [A]dd period | [E]dit period | [D]elete period | [C]lear all | [B]ack")
                    dev_choice = input("Select action: ").strip().lower()

                    if dev_choice in ('b', ''):
                        break
                    elif dev_choice == 'a':
                        # Add new period
                        print("\nAvailable Owners:")
                        for k_idx, k_name in enumerate(known_owners, 1):
                            print(f"  {k_idx}. {k_name}")
                        owner_input = input("Enter owner number or new owner name: ").strip()
                        if not owner_input:
                            continue
                        if owner_input.isdigit() and 1 <= int(owner_input) <= len(known_owners):
                            owner_name_to_add = known_owners[int(owner_input) - 1]
                        else:
                            owner_name_to_add = owner_input

                        start_date_in = input("Enter start date (YYYY-MM-DD, or leave empty for beginning of time): ").strip()
                        end_date_in = input("Enter end date (YYYY-MM-DD, or leave empty for ongoing/present): ").strip()
                        notes_in = input("Enter optional notes (or leave empty): ").strip()

                        acquire_planner_lock()
                        conn = get_connection()
                        cursor = get_cursor()
                        cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES (?)", (owner_name_to_add,))
                        cursor.execute("""
                            INSERT INTO device_owners (camera_model, owner_name, start_date, end_date, notes)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            selected_model,
                            owner_name_to_add,
                            start_date_in if start_date_in else None,
                            end_date_in if end_date_in else None,
                            notes_in if notes_in else None
                        ))
                        conn.commit()
                        print(f"✅ Added ownership period: {owner_name_to_add} ({start_date_in or 'Beginning'} -> {end_date_in or 'Present'}) for '{selected_model}'.")
                        close_conn()
                        release_planner_lock()

                    elif dev_choice == 'e':
                        if not current_periods:
                            print("⚠️ No periods to edit.")
                            continue
                        p_sel = input(f"Enter period number to edit (1-{len(current_periods)}): ").strip()
                        try:
                            p_idx = int(p_sel) - 1
                            if 0 <= p_idx < len(current_periods):
                                tgt_id, tgt_owner, tgt_start, tgt_end, tgt_notes = current_periods[p_idx]
                                new_owner = input(f"Owner name [{tgt_owner}]: ").strip() or tgt_owner
                                new_start = input(f"Start date (YYYY-MM-DD, or '-' to clear) [{tgt_start or 'None'}]: ").strip()
                                new_end = input(f"End date (YYYY-MM-DD, or '-' to clear) [{tgt_end or 'None'}]: ").strip()

                                final_start = None if new_start == '-' else (new_start if new_start else tgt_start)
                                final_end = None if new_end == '-' else (new_end if new_end else tgt_end)

                                acquire_planner_lock()
                                conn = get_connection()
                                cursor = get_cursor()
                                cursor.execute("INSERT OR IGNORE INTO owners (name) VALUES (?)", (new_owner,))
                                cursor.execute("""
                                    UPDATE device_owners 
                                    SET owner_name = ?, start_date = ?, end_date = ?, updated_at_utc = datetime('now')
                                    WHERE id = ?
                                """, (new_owner, final_start, final_end, tgt_id))
                                conn.commit()
                                print(f"✅ Updated ownership period #{p_idx + 1} for '{selected_model}'.")
                                close_conn()
                                release_planner_lock()
                        except ValueError:
                            print("⚠️ Invalid number.")

                    elif dev_choice == 'd':
                        if not current_periods:
                            print("⚠️ No periods to delete.")
                            continue
                        p_sel = input(f"Enter period number to delete (1-{len(current_periods)}): ").strip()
                        try:
                            p_idx = int(p_sel) - 1
                            if 0 <= p_idx < len(current_periods):
                                tgt_id = current_periods[p_idx][0]
                                acquire_planner_lock()
                                conn = get_connection()
                                cursor = get_cursor()
                                cursor.execute("DELETE FROM device_owners WHERE id = ?", (tgt_id,))
                                conn.commit()
                                print(f"✅ Deleted ownership period #{p_idx + 1} for '{selected_model}'.")
                                close_conn()
                                release_planner_lock()
                        except ValueError:
                            print("⚠️ Invalid number.")

                    elif dev_choice == 'c':
                        confirm = input(f"Are you sure you want to clear ALL ownership periods for '{selected_model}'? [y/N]: ").strip().lower()
                        if confirm == 'y':
                            acquire_planner_lock()
                            conn = get_connection()
                            cursor = get_cursor()
                            cursor.execute("DELETE FROM device_owners WHERE camera_model = ?", (selected_model,))
                            conn.commit()
                            print(f"✅ Cleared all database overrides for '{selected_model}'.")
                            close_conn()
                            release_planner_lock()
            except ValueError:
                print("⚠️ Please enter a valid number.")
            except Exception as e:
                print(f"⚠️ Error: {e}")
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

def parse_asset_selection(choice_str, total_items):
    """
    Parses user input string into a list of 1-based candidate indices.
    Supports formats:
      - '1'
      - '1, 3, 5'
      - '1-5'
      - '1-5, 8, 10-12'
      - 'all'
    Returns a sorted list of valid unique 1-based integer indices, or empty list if invalid.
    """
    cleaned = choice_str.strip().lower()
    if cleaned == 'all':
        return list(range(1, total_items + 1))
    
    selected_indices = set()
    parts = [p.strip() for p in cleaned.split(',') if p.strip()]
    for part in parts:
        if '-' in part:
            range_parts = part.split('-')
            if len(range_parts) == 2 and range_parts[0].strip().isdigit() and range_parts[1].strip().isdigit():
                start_i = int(range_parts[0].strip())
                end_i = int(range_parts[1].strip())
                if start_i > end_i:
                    start_i, end_i = end_i, start_i
                for i in range(start_i, end_i + 1):
                    if 1 <= i <= total_items:
                        selected_indices.add(i)
        elif part.isdigit():
            idx = int(part)
            if 1 <= idx <= total_items:
                selected_indices.add(idx)
                
    return sorted(selected_indices)

def display_quartile_cleanup_flow(cursor=None, conn=None):
    """
    Interactive flow to analyze and clean up low-quality media assets using a 4-quartile model.
    Groups devices by primary owner, shows low-quality (Q1 & Q2, bottom 50%) file counts and reclaimable space,
    and lets the user drill down into any device to view candidates ordered by file size descending,
    with assigned/suggested moments and moment sister assets.
    Allows interactive selection of assets to mark them as removed from source.
    """
    while True:
        # 1. Query device summary grouped by primary owner (excluding already removed assets)
        acquire_planner_lock()
        conn = get_connection()
        conn.execute("PRAGMA busy_timeout = 30000")
        cursor = get_cursor()

        try:
            cursor.execute("""
                WITH scored_assets AS (
                    SELECT 
                        COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
                        COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
                        COALESCE(v.score_normalized, 0.0) AS score_normalized,
                        COALESCE(aaa.ZORIGINALFILESIZE, 0) AS file_size_bytes,
                        NTILE(4) OVER (
                            PARTITION BY COALESCE(zea.ZCAMERAMODEL, 'Unknown')
                            ORDER BY COALESCE(v.score_normalized, 0.0) ASC
                        ) AS score_quartile
                    FROM assets a
                    LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
                    LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                    LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                    LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
                        AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                        AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                    LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
                    WHERE zea.ZCAMERAMODEL IS NOT NULL 
                      AND zea.ZCAMERAMODEL != ''
                      AND COALESCE(a.removed_from_source, 0) = 0
                )
                SELECT 
                    primary_owner,
                    camera_model,
                    COUNT(CASE WHEN score_quartile IN (1, 2) THEN 1 END) AS low_quality_count,
                    ROUND(SUM(CASE WHEN score_quartile IN (1, 2) THEN file_size_bytes ELSE 0 END) / 1073741824.0, 2) AS low_quality_gb,
                    COUNT(*) AS total_assets,
                    ROUND(SUM(file_size_bytes) / 1073741824.0, 2) AS total_gb,
                    ROUND(MIN(score_normalized), 4) AS min_score,
                    ROUND(MAX(CASE WHEN score_quartile = 2 THEN score_normalized END), 4) AS q2_max_score
                FROM scored_assets
                GROUP BY primary_owner, camera_model
                ORDER BY primary_owner ASC, low_quality_gb DESC, low_quality_count DESC;
            """)
            summary_rows = cursor.fetchall()
        finally:
            close_conn()
            release_planner_lock()

        if not summary_rows:
            print("\nℹ️  No imported assets found with camera model attributes.")
            break

        # Build clean owner groups
        owner_data = {}
        device_lookup = {}
        device_idx = 1
        grand_low_count = 0
        grand_low_gb = 0.0
        grand_total_count = 0
        grand_total_gb = 0.0

        for r in summary_rows:
            p_owner = r[0]
            c_model = r[1]
            l_count = r[2] or 0
            l_gb = r[3] or 0.0
            t_count = r[4] or 0
            t_gb = r[5] or 0.0
            min_s = r[6] if r[6] is not None else 0.0
            max_s = r[7] if r[7] is not None else 0.0

            if p_owner not in owner_data:
                owner_data[p_owner] = []

            item = {
                'idx': device_idx,
                'owner': p_owner,
                'model': c_model,
                'low_count': l_count,
                'low_gb': l_gb,
                'total_count': t_count,
                'total_gb': t_gb,
                'min_score': min_s,
                'max_score': max_s
            }
            owner_data[p_owner].append(item)
            device_lookup[device_idx] = item
            device_lookup[c_model.lower()] = item
            device_idx += 1

            grand_low_count += l_count
            grand_low_gb += l_gb
            grand_total_count += t_count
            grand_total_gb += t_gb

        # Print Owner-Grouped Overview Table
        print("\n" + "=" * 140)
        print("🧹 Quartile Media Cleanup: Low-Quality Asset Overview by Device (Bottom 2 Quartiles / Lowest 50%)")
        print("=" * 140)
        print("Splits imported assets into 4 equal quartiles partitioned by device based on quality/curation scores.")
        print("Q1 & Q2 represent the bottom 50% lowest quality assets. Ordered largest-to-smallest so high storage gains appear first.\n")

        for owner_name in sorted(owner_data.keys()):
            items = owner_data[owner_name]
            sub_low_count = sum(i['low_count'] for i in items)
            sub_low_gb = sum(i['low_gb'] for i in items)
            sub_total_count = sum(i['total_count'] for i in items)
            sub_total_gb = sum(i['total_gb'] for i in items)

            print(f"👤 Primary Owner: {owner_name}")
            print("-" * 140)
            print(f"  {'No.':<5} {'Camera Model':<34} {'Low-Quality (Q1+Q2)':<22} {'Reclaimable Space':<20} {'Total on Device':<20} {'Total Device Size':<20} {'Score Ceiling (Q2 Max)':<20}")
            print("  " + "-" * 138)

            for it in items:
                low_cnt_str = f"{it['low_count']:,} files"
                low_gb_str = f"{it['low_gb']:.2f} GB"
                tot_cnt_str = f"{it['total_count']:,} files"
                tot_gb_str = f"{it['total_gb']:.2f} GB"
                score_range = f"{it['min_score']:.4f} - {it['max_score']:.4f}"

                print(f"  {it['idx']:<5} {it['model']:<34} {low_cnt_str:<22} {low_gb_str:<20} {tot_cnt_str:<20} {tot_gb_str:<20} {score_range:<20}")

            print("  " + "-" * 138)
            print(f"  💰 Subtotal for {owner_name}: {sub_low_count:,} low-quality files ({sub_low_gb:.2f} GB reclaimable out of {sub_total_gb:.2f} GB total across {sub_total_count:,} assets)\n")

        print("=" * 140)
        print(f"💰 GRAND TOTAL ACROSS ALL DEVICES: {grand_low_count:,} low-quality files ({grand_low_gb:.2f} GB reclaimable out of {grand_total_gb:.2f} GB total across {grand_total_count:,} assets)")
        print("=" * 140)

        # Device Selection Prompt
        print("\nOptions: Select device number [1-{}] or device name to view candidate files | [B]ack to Main Menu [default: B]: ".format(device_idx - 1))
        choice = input("Enter choice: ").strip()
        if not choice or choice.lower() in ('b', 'back', 'q', 'quit'):
            break

        selected_item = None
        if choice.isdigit() and int(choice) in device_lookup:
            selected_item = device_lookup[int(choice)]
        elif choice.lower() in device_lookup:
            selected_item = device_lookup[choice.lower()]
        else:
            matches = [it for it in device_lookup.values() if isinstance(it, dict) and choice.lower() in it['model'].lower()]
            if len(matches) == 1:
                selected_item = matches[0]
            elif len(matches) > 1:
                print(f"Multiple devices matched '{choice}'. Please pick by number: {[m['idx'] for m in matches]}")
                continue
            else:
                print(f"❌ Unknown device selection '{choice}'.")
                continue

        # Drill-down into candidate files for selected device
        selected_model = selected_item['model']
        selected_owner = selected_item['owner']

        while True:
            print(f"\n🔍 Querying bottom 2 quartiles for {selected_owner} - {selected_model} (ordered largest-to-smallest)...")

            acquire_planner_lock()
            conn = get_connection()
            conn.execute("PRAGMA busy_timeout = 30000")
            cursor = get_cursor()

            photos_db_attached = False
            try:
                cursor.execute(f"ATTACH DATABASE 'file:{APPLE_PHOTOS_DB_PATH}?mode=ro' AS photos_db;")
                photos_db_attached = True
                logger.debug("Attached Photos.sqlite database read-only for quartile cleanup.")
            except Exception as e:
                logger.warning(f"Could not attach Photos.sqlite: {e}")

            try:
                # Query candidate files with moment siblings & publication status
                if photos_db_attached:
                    cand_query = """
                        WITH all_assets_with_moments AS (
                            SELECT 
                                a.asset_id,
                                a.original_filename,
                                a.month,
                                a.date_created_utc,
                                COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
                                COALESCE(zea.ZCAMERAMAKE, 'Unknown') AS camera_make,
                                COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
                                COALESCE(v.score_normalized, 0.0) AS score_normalized,
                                a.aesthetic_score,
                                a.google_favorite,
                                a.mobile_apple_photos_featured_photos AS apple_featured,
                                a.apple_photos_monthly_selection AS apple_monthly_sel,
                                CASE 
                                    WHEN COALESCE(za_ext.ZKIND, za.ZKIND, 0) = 1 
                                         OR lower(a.original_filename) LIKE '%.mov' 
                                         OR lower(a.original_filename) LIKE '%.mp4' 
                                         OR lower(a.original_filename) LIKE '%.m4v' 
                                         OR lower(a.original_filename) LIKE '%.avi' 
                                    THEN 'video' 
                                    ELSE 'photo' 
                                END AS media_type,
                                COALESCE(
                                    a.MomentsAlbumName, 
                                    a.curated_album, 
                                    a.to_be_curated_album, 
                                    ps.published_moments,
                                    CASE 
                                        WHEN m.ZTITLE IS NOT NULL AND m.ZTITLE != '' 
                                        THEN 'SUGG: ' || COALESCE(substr(a.date_created_utc, 1, 10), date(za.ZDATECREATED + 978307200, 'unixepoch'), a.month, '') || ' - ' || m.ZTITLE
                                        ELSE NULL 
                                    END,
                                    '—'
                                ) AS assigned_moment,
                                ps.last_asset_pub,
                                aaa.ZORIGINALFILESIZE AS file_size_bytes,
                                ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1048576.0, 2) AS file_size_mb,
                                ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1073741824.0, 3) AS file_size_gb,
                                NTILE(4) OVER (
                                    PARTITION BY COALESCE(zea.ZCAMERAMODEL, 'Unknown')
                                    ORDER BY COALESCE(v.score_normalized, 0.0) ASC
                                ) AS score_quartile
                            FROM assets a
                            LEFT JOIN photos_db.ZASSET za_ext ON za_ext.ZUUID = a.asset_id
                            LEFT JOIN photos_db.ZMOMENT m ON za_ext.ZMOMENT = m.Z_PK
                            LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
                            LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                            LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
                                AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                                AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                            LEFT JOIN (
                                SELECT asset_id, GROUP_CONCAT(DISTINCT moment_name) AS published_moments, MAX(published_at_utc) AS last_asset_pub 
                                FROM publications 
                                GROUP BY asset_id
                            ) ps ON ps.asset_id = a.asset_id
                            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
                            WHERE zea.ZCAMERAMODEL = ?
                              AND COALESCE(a.removed_from_source, 0) = 0
                        ),
                        moment_pub_stats AS (
                            SELECT 
                                moment_name,
                                MAX(published_at_utc) AS last_published_at
                            FROM publications
                            GROUP BY moment_name
                        ),
                        ranked_siblings AS (
                            SELECT 
                                assigned_moment,
                                media_type,
                                original_filename,
                                score_normalized,
                                file_size_mb,
                                ROW_NUMBER() OVER (PARTITION BY assigned_moment, media_type ORDER BY score_normalized DESC) as rn
                            FROM all_assets_with_moments
                            WHERE assigned_moment != '—'
                        ),
                        moment_photos AS (
                            SELECT 
                                assigned_moment,
                                COUNT(*) AS photo_count,
                                ROUND(MAX(score_normalized), 4) AS max_photo_score,
                                GROUP_CONCAT(
                                    original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
                                    ' | '
                                ) AS top_photos
                            FROM ranked_siblings
                            WHERE media_type = 'photo' AND rn <= 3
                            GROUP BY assigned_moment
                        ),
                        moment_videos AS (
                            SELECT 
                                assigned_moment,
                                COUNT(*) AS video_count,
                                ROUND(MAX(score_normalized), 4) AS max_video_score,
                                GROUP_CONCAT(
                                    original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
                                    ' | '
                                ) AS top_videos
                            FROM ranked_siblings
                            WHERE media_type = 'video' AND rn <= 3
                            GROUP BY assigned_moment
                        )
                        SELECT 
                            c.score_quartile,
                            c.original_filename AS candidate_file,
                            c.month,
                            c.file_size_mb,
                            ROUND(c.score_normalized, 4) AS candidate_score,
                            c.assigned_moment,
                            CASE 
                                WHEN mp.last_published_at IS NOT NULL THEN '✅ ' || substr(mp.last_published_at, 1, 10)
                                WHEN c.last_asset_pub IS NOT NULL THEN '✅ ' || substr(c.last_asset_pub, 1, 10)
                                ELSE '—'
                            END AS moment_published_date,
                            COALESCE(p.photo_count, 0) + COALESCE(v.video_count, 0) AS total_assets_in_moment,
                            MAX(COALESCE(p.max_photo_score, 0), COALESCE(v.max_video_score, 0)) AS best_score_in_moment,
                            CASE 
                                WHEN c.media_type = 'video' THEN
                                    CASE 
                                        WHEN v.top_videos IS NOT NULL AND p.top_photos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos || '  ||  📷 Photos: ' || p.top_photos
                                        WHEN v.top_videos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos
                                        WHEN p.top_photos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos
                                        ELSE '— (Standalone / No other assets)'
                                    END
                                ELSE
                                    CASE 
                                        WHEN p.top_photos IS NOT NULL AND v.top_videos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos || '  ||  🎥 Videos: ' || v.top_videos
                                        WHEN p.top_photos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos
                                        WHEN v.top_videos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos
                                        ELSE '— (Standalone / No other assets)'
                                    END
                            END AS moment_top_assets_with_scores,
                            c.date_created_utc,
                            c.primary_owner,
                            c.camera_model,
                            c.asset_id,
                            c.file_size_bytes
                        FROM all_assets_with_moments c
                        LEFT JOIN moment_pub_stats mp ON mp.moment_name = c.assigned_moment AND c.assigned_moment != '—'
                        LEFT JOIN moment_photos p ON p.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
                        LEFT JOIN moment_videos v ON v.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
                        WHERE c.score_quartile IN (1, 2)
                        ORDER BY c.file_size_bytes DESC;
                    """
                    cursor.execute(cand_query, (selected_model,))
                else:
                    cand_query = """
                        WITH all_assets_with_moments AS (
                            SELECT 
                                a.asset_id,
                                a.original_filename,
                                a.month,
                                a.date_created_utc,
                                COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
                                COALESCE(zea.ZCAMERAMAKE, 'Unknown') AS camera_make,
                                COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
                                COALESCE(v.score_normalized, 0.0) AS score_normalized,
                                a.aesthetic_score,
                                a.google_favorite,
                                a.mobile_apple_photos_featured_photos AS apple_featured,
                                a.apple_photos_monthly_selection AS apple_monthly_sel,
                                CASE 
                                    WHEN COALESCE(za.ZKIND, 0) = 1 
                                         OR lower(a.original_filename) LIKE '%.mov' 
                                         OR lower(a.original_filename) LIKE '%.mp4' 
                                         OR lower(a.original_filename) LIKE '%.m4v' 
                                         OR lower(a.original_filename) LIKE '%.avi' 
                                    THEN 'video' 
                                    ELSE 'photo' 
                                END AS media_type,
                                COALESCE(
                                    a.MomentsAlbumName, 
                                    a.curated_album, 
                                    a.to_be_curated_album, 
                                    ps.published_moments,
                                    '—'
                                ) AS assigned_moment,
                                ps.last_asset_pub,
                                aaa.ZORIGINALFILESIZE AS file_size_bytes,
                                ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1048576.0, 2) AS file_size_mb,
                                ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1073741824.0, 3) AS file_size_gb,
                                NTILE(4) OVER (
                                    PARTITION BY COALESCE(zea.ZCAMERAMODEL, 'Unknown')
                                    ORDER BY COALESCE(v.score_normalized, 0.0) ASC
                                ) AS score_quartile
                            FROM assets a
                            LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
                            LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                            LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
                            LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
                                AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                                AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
                            LEFT JOIN (
                                SELECT asset_id, GROUP_CONCAT(DISTINCT moment_name) AS published_moments, MAX(published_at_utc) AS last_asset_pub 
                                FROM publications 
                                GROUP BY asset_id
                            ) ps ON ps.asset_id = a.asset_id
                            LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
                            WHERE zea.ZCAMERAMODEL = ?
                              AND COALESCE(a.removed_from_source, 0) = 0
                        ),
                        moment_pub_stats AS (
                            SELECT 
                                moment_name,
                                MAX(published_at_utc) AS last_published_at
                            FROM publications
                            GROUP BY moment_name
                        ),
                        ranked_siblings AS (
                            SELECT 
                                assigned_moment,
                                media_type,
                                original_filename,
                                score_normalized,
                                file_size_mb,
                                ROW_NUMBER() OVER (PARTITION BY assigned_moment, media_type ORDER BY score_normalized DESC) as rn
                            FROM all_assets_with_moments
                            WHERE assigned_moment != '—'
                        ),
                        moment_photos AS (
                            SELECT 
                                assigned_moment,
                                COUNT(*) AS photo_count,
                                ROUND(MAX(score_normalized), 4) AS max_photo_score,
                                GROUP_CONCAT(
                                    original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
                                    ' | '
                                ) AS top_photos
                            FROM ranked_siblings
                            WHERE media_type = 'photo' AND rn <= 3
                            GROUP BY assigned_moment
                        ),
                        moment_videos AS (
                            SELECT 
                                assigned_moment,
                                COUNT(*) AS video_count,
                                ROUND(MAX(score_normalized), 4) AS max_video_score,
                                GROUP_CONCAT(
                                    original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
                                    ' | '
                                ) AS top_videos
                            FROM ranked_siblings
                            WHERE media_type = 'video' AND rn <= 3
                            GROUP BY assigned_moment
                        )
                        SELECT 
                            c.score_quartile,
                            c.original_filename AS candidate_file,
                            c.month,
                            c.file_size_mb,
                            ROUND(c.score_normalized, 4) AS candidate_score,
                            c.assigned_moment,
                            CASE 
                                WHEN mp.last_published_at IS NOT NULL THEN '✅ ' || substr(mp.last_published_at, 1, 10)
                                WHEN c.last_asset_pub IS NOT NULL THEN '✅ ' || substr(c.last_asset_pub, 1, 10)
                                ELSE '—'
                            END AS moment_published_date,
                            COALESCE(p.photo_count, 0) + COALESCE(v.video_count, 0) AS total_assets_in_moment,
                            MAX(COALESCE(p.max_photo_score, 0), COALESCE(v.max_video_score, 0)) AS best_score_in_moment,
                            CASE 
                                WHEN c.media_type = 'video' THEN
                                    CASE 
                                        WHEN v.top_videos IS NOT NULL AND p.top_photos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos || '  ||  📷 Photos: ' || p.top_photos
                                        WHEN v.top_videos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos
                                        WHEN p.top_photos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos
                                        ELSE '— (Standalone / No other assets)'
                                    END
                                ELSE
                                    CASE 
                                        WHEN p.top_photos IS NOT NULL AND v.top_videos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos || '  ||  🎥 Videos: ' || v.top_videos
                                        WHEN p.top_photos IS NOT NULL 
                                        THEN '📷 Photos: ' || p.top_photos
                                        WHEN v.top_videos IS NOT NULL 
                                        THEN '🎥 Videos: ' || v.top_videos
                                        ELSE '— (Standalone / No other assets)'
                                    END
                            END AS moment_top_assets_with_scores,
                            c.date_created_utc,
                            c.primary_owner,
                            c.camera_model,
                            c.asset_id,
                            c.file_size_bytes
                        FROM all_assets_with_moments c
                        LEFT JOIN moment_pub_stats mp ON mp.moment_name = c.assigned_moment AND c.assigned_moment != '—'
                        LEFT JOIN moment_photos p ON p.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
                        LEFT JOIN moment_videos v ON v.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
                        WHERE c.score_quartile IN (1, 2)
                        ORDER BY c.file_size_bytes DESC;
                    """
                    cursor.execute(cand_query, (selected_model,))

                candidates = cursor.fetchall()
            finally:
                if photos_db_attached:
                    try:
                        cursor.execute("DETACH DATABASE photos_db;")
                        logger.debug("Detached Photos.sqlite database after candidate query.")
                    except Exception:
                        pass
                close_conn()
                release_planner_lock()

            if not candidates:
                print(f"ℹ️  No candidate files in Q1/Q2 found for {selected_model}.")
                break

            total_cand = len(candidates)
            total_cand_bytes = sum(c[14] or 0 for c in candidates)
            total_cand_gb = total_cand_bytes / 1073741824.0

            # Write candidate report to log file
            try:
                log_lines = [
                    "=" * 230,
                    f"🧹 Quartile Cleanup Candidate Report: {selected_owner} - {selected_model}",
                    f"Total Q1+Q2 Low-Quality Assets: {len(candidates):,} files | Reclaimable Space: {total_cand_gb:.2f} GB",
                    "=" * 230,
                    f"{'No.':<5} {'Q':<4} {'Candidate Filename':<24} {'Month':<9} {'Size(MB)':<11} {'Score':<8} {'Assigned / Suggested Moment':<38} {'Published':<14} {'Moment Top Sibling Assets'}",
                    "-" * 230
                ]
                for c_idx, c_row in enumerate(candidates, 1):
                    c_q = f"Q{c_row[0]}"
                    c_fn = c_row[1]
                    c_mo = c_row[2] or "—"
                    c_mb = f"{c_row[3]:.2f} MB"
                    c_sc = f"{c_row[4]:.4f}"
                    c_mom = c_row[5] or "—"
                    if len(c_mom) > 38:
                        c_mom_trunc = c_mom[:35] + "..."
                    else:
                        c_mom_trunc = c_mom
                    c_pub = c_row[6] or "—"
                    c_sibs = c_row[9] or "—"
                    log_lines.append(f"{c_idx:<5} {c_q:<4} {c_fn:<24} {c_mo:<9} {c_mb:<11} {c_sc:<8} {c_mom_trunc:<38} {c_pub:<14} {c_sibs}")

                with open(QUARTILE_CLEANUP_LOG_PATH, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(log_lines) + '\n')
                logger.info(f"📄 Full candidate report ({len(candidates)} files) saved to {QUARTILE_CLEANUP_LOG_PATH}")
            except Exception as e:
                logger.warning(f"Could not write candidate log: {e}")

            # Interactive pagination & selection of candidates
            page_size = 25
            cur_offset = 0
            exit_to_devices = False

            while cur_offset < total_cand:
                page_items = candidates[cur_offset : cur_offset + page_size]
                print("\n" + "=" * 230)
                print(f"📷 Candidate Files for Removal: {selected_owner} - {selected_model} (Showing {cur_offset + 1} - {min(cur_offset + page_size, total_cand)} of {total_cand:,} files | {total_cand_gb:.2f} GB)")
                print("=" * 230)
                print(f"{'No.':<5} {'Q':<4} {'Candidate Filename':<24} {'Month':<9} {'Size(MB)':<11} {'Score':<8} {'Assigned / Suggested Moment':<38} {'Published':<14} {'Moment Top Sibling Assets'}")
                print("-" * 230)

                for p_idx, c_row in enumerate(page_items, cur_offset + 1):
                    c_q = f"Q{c_row[0]}"
                    c_fn = c_row[1]
                    c_mo = c_row[2] or "—"
                    c_mb = f"{c_row[3]:.2f} MB"
                    c_sc = f"{c_row[4]:.4f}"
                    c_mom = c_row[5] or "—"
                    if len(c_mom) > 38:
                        c_mom_trunc = c_mom[:35] + "..."
                    else:
                        c_mom_trunc = c_mom
                    c_pub = c_row[6] or "—"
                    c_sibs = c_row[9] or "—"
                    if len(c_sibs) > 110:
                        c_sibs = c_sibs[:107] + "..."
                    print(f"{p_idx:<5} {c_q:<4} {c_fn:<24} {c_mo:<9} {c_mb:<11} {c_sc:<8} {c_mom_trunc:<38} {c_pub:<14} {c_sibs}")

                print("-" * 230)
                print(f"ℹ️  Full list with details written to {QUARTILE_CLEANUP_LOG_PATH}")

                prompt_msg = (
                    f"\nOptions: [1-{total_cand}] Select asset(s) to remove (e.g. '1', '1,3', '1-5', 'all')"
                    f"\n         [Enter] Next {page_size} | [A]ll remaining | [B]ack to device list: "
                )
                p_choice = input(prompt_msg).strip()

                if not p_choice:
                    cur_offset += page_size
                    continue
                elif p_choice.lower() in ('b', 'back', 'q', 'quit'):
                    exit_to_devices = True
                    break
                elif p_choice.lower() == 'a' and total_cand > 25:
                    page_size = total_cand
                    cur_offset = 0
                    continue

                # Check if user entered numbers/ranges to mark as removed from source
                selected_indices = parse_asset_selection(p_choice, total_cand)
                if selected_indices:
                    chosen_items = [candidates[i - 1] for i in selected_indices]
                    sel_bytes = sum(c[14] or 0 for c in chosen_items)
                    sel_mb = sum(c[3] or 0.0 for c in chosen_items)
                    sel_gb = sel_bytes / 1073741824.0

                    print(f"\n⚠️  Selected {len(chosen_items)} asset(s) ({sel_mb:.2f} MB / {sel_gb:.2f} GB) from {selected_owner} - {selected_model}:")
                    for item_idx, ch in enumerate(chosen_items[:10], 1):
                        print(f"   • [{selected_indices[item_idx - 1]}] {ch[1]} ({ch[3]:.2f} MB, Q{ch[0]}, Score: {ch[4]:.4f}, Moment: {ch[5]})")
                    if len(chosen_items) > 10:
                        print(f"   • ... and {len(chosen_items) - 10} more asset(s)")

                    confirm = input(f"\nAre you sure you want to mark these {len(chosen_items)} asset(s) as removed from source? [Y/n]: ").strip().lower()
                    if confirm in ('', 'y', 'yes'):
                        asset_ids_to_remove = [ch[13] for ch in chosen_items]
                        acquire_planner_lock()
                        conn = get_connection()
                        conn.execute("PRAGMA busy_timeout = 30000")
                        cursor = get_cursor()
                        try:
                            cursor.executemany("""
                                UPDATE assets 
                                SET removed_from_source = 1,
                                    removed_from_source_at_utc = datetime('now'),
                                    updated_at_utc = datetime('now')
                                WHERE asset_id = ?
                            """, [(aid,) for aid in asset_ids_to_remove])
                            conn.commit()
                        finally:
                            close_conn()
                            release_planner_lock()

                        print(f"\n✅ Marked {len(chosen_items)} asset(s) as removed from source ({human_readable_size(sel_bytes)} freed).")
                        # Break pagination loop to refresh candidates query
                        break
                    else:
                        print("❌ Removal cancelled.")
                        continue
                else:
                    print(f"❌ Invalid selection '{p_choice}'.")
                    continue

            if exit_to_devices:
                break

def display_media_cleanup_recommendations(cursor, verbose=True):
    """
    Generates and displays media cleanup recommendations for source cameras based on published albums.
    Groups recommendations by device owner. For each recommendation row, queries Apple Photos DB copy
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

    # Fetch month selection assets for all devices & months
    month_selection_map = {}
    try:
        cursor.execute("""
            WITH RECURSIVE selection_albums(album_pk, album_title) AS (
                SELECT Z_PK, ZTITLE
                FROM photos_db.ZGENERICALBUM
                WHERE ZTITLE = 'Apple Photos Month Selection' AND ZTRASHEDSTATE = 0
                UNION ALL
                SELECT ga.Z_PK, ga.ZTITLE
                FROM photos_db.ZGENERICALBUM ga
                JOIN selection_albums sa ON ga.ZPARENTFOLDER = sa.album_pk
                WHERE ga.ZTRASHEDSTATE = 0
            )
            SELECT 
                COALESCE(zea.ZCAMERAMODEL, zea.ZCAMERAMAKE, 'Unknown') AS dev_model,
                strftime('%Y-%m', datetime(za.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) AS month,
                COUNT(DISTINCT za.ZUUID) AS selected_count,
                GROUP_CONCAT(DISTINCT COALESCE(a.original_filename, za.ZFILENAME)) AS selected_filenames
            FROM selection_albums sa
            JOIN photos_db.Z_30ASSETS aa ON aa.Z_30ALBUMS = sa.album_pk
            JOIN photos_db.ZASSET za ON za.Z_PK = aa.Z_3ASSETS
            LEFT JOIN assets a ON a.asset_id = za.ZUUID
            LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
            WHERE za.ZTRASHEDSTATE = 0 
              AND sa.album_title != 'Apple Photos Month Selection'
            GROUP BY dev_model, month
        """)
        for s_model, s_month, s_count, s_filenames in cursor.fetchall():
            if s_model and s_month:
                month_selection_map[(s_model, s_month)] = (s_count, s_filenames or "")
    except Exception as e:
        logger.debug(f"Could not scan month selection assets: {e}")

    cleanup_report = []
    cleanup_report.append("=" * 180)
    cleanup_report.append("🧹 Media Cleanup Recommendations (Safe to Delete from Source Cameras)")
    cleanup_report.append("=" * 180)

    if not rows:
        cleanup_report.append("ℹ️  No published moments found in database.")
        cleanup_report.append("👉 Once moments are published in Mode [M], safe deletion recommendations for your camera SD cards will appear here.")
        cleanup_report.append("=" * 180 + "\n")
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
            
            # Look up owner dynamically via database (with asset date) or defaults
            month_date = row[7] if row[7] else (f"{row[0]}-01" if row[0] else None)
            owner_resolved, _ = resolve_device_owner(cursor, c_source, asset_date=month_date)
            if owner_resolved == "Shared/Other" and c_model != c_source:
                owner_resolved, _ = resolve_device_owner(cursor, c_model, asset_date=month_date)
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
            cleanup_report.append("=" * 180)
            
            owner_total_files = 0
            owner_total_bytes = 0
            
            # Process each device for this owner
            for device_name, group_rows in sorted(owner_groups[owner_name].items()):
                cleanup_report.append(f"  📷 Device: {device_name}")
                cleanup_report.append("  " + "-" * 178)
                header = f"  {'No.':<4} {'Month':<10} {'Published':<12} {'Filename Range':<32} {'Date Range':<24} {'Reclaimable from SD Card (Whole Month)':<38} Apple Photos Month Selection"
                cleanup_report.append(header)
                cleanup_report.append("  " + "-" * 178)

                device_total_files = 0
                device_total_bytes = 0

                # Gather data and query photos_db for all rows first
                processed_rows = []
                for row in group_rows:
                    month_val = row[0] or "—"
                    c_make = row[2] or ""
                    c_model = row[3] or "Unknown"
                    c_source = f"{c_model}" if (c_model != "Unknown" and c_model) else (c_make or "Unknown")
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

                    # Look up Apple Photos Month Selection assets
                    sel_info = (
                        month_selection_map.get((c_model, month_val)) or 
                        month_selection_map.get((c_source, month_val)) or 
                        month_selection_map.get((device_name, month_val)) or 
                        (0, "")
                    )
                    sel_count, sel_files = sel_info
                    if sel_count > 0:
                        sel_files_formatted = sel_files.replace(',', ', ')
                        sel_str = f"{sel_count} files: {sel_files_formatted}" if sel_count > 1 else f"1 file: {sel_files_formatted}"
                    else:
                        sel_str = "—"

                    processed_rows.append({
                        "month_val": month_val,
                        "file_count": file_count,
                        "f_range": f_range,
                        "d_range": d_range,
                        "total_scan_count": total_scan_count,
                        "total_scan_bytes": total_scan_bytes,
                        "sel_str": sel_str
                    })

                # Sort processed_rows by total_scan_bytes descending
                processed_rows.sort(key=lambda x: x["total_scan_bytes"], reverse=True)

                for item in processed_rows:
                    device_total_files += item["total_scan_count"]
                    device_total_bytes += item["total_scan_bytes"]

                    scan_range_str = f"{item['total_scan_count']} files ({human_readable_size(item['total_scan_bytes'])})" if item['total_scan_count'] > 0 else "—"

                    line = f"  {global_idx:<4} {item['month_val']:<10} {item['file_count'] + ' files':<12} {item['f_range']:<32} {item['d_range']:<24} {scan_range_str:<38} {item['sel_str']}"
                    cleanup_report.append(line)
                    global_idx += 1

                owner_total_files += device_total_files
                owner_total_bytes += device_total_bytes

                cleanup_report.append("  " + "-" * 178)
                cleanup_report.append(f"  💰 Subtotal reclaimable space on {device_name}: {device_total_files} files ({human_readable_size(device_total_bytes)})")
                cleanup_report.append("")

            cleanup_report.append("-" * 180)
            cleanup_report.append(f"💰 Total reclaimable space for owner {owner_name}: {owner_total_files} files ({human_readable_size(owner_total_bytes)})")
            cleanup_report.append("=" * 180 + "\n")

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
    atexit.register(stop_bg_service_on_exit)
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
            cleanup_type = input("\nSelect Cleanup Mode: [Q] Quartile Low-Quality Device Cleanup (default) | [L] Legacy Published SD-Card Cleanup: ").strip().lower()
            if cleanup_type == 'l':
                acquire_planner_lock()
                conn = get_connection()
                conn.execute("PRAGMA busy_timeout = 30000")
                cursor = get_cursor()
                display_media_cleanup_recommendations(cursor, verbose=True)
                close_conn()
                release_planner_lock()
                sys.exit(0)
            else:
                display_quartile_cleanup_flow(None, None)
                sys.exit(0)
        elif mode == 'o':
            manage_device_owners_flow(None, None)
            # Restart the script to return to the main menu clean
            os.execv(sys.executable, [sys.executable] + sys.argv)

    acquire_planner_lock()
    conn = get_connection()
    conn.execute("PRAGMA busy_timeout = 30000")
    cursor = get_cursor()
    ensure_views_exist(cursor, conn)

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
        latest_month_str, latest_status = batches[0][:2]  # Ordered DESC
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
        for m, s in [b[:2] for b in batches]:
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

        fav_count, source, _ = check_favorites_count(cursor, month, check_remote=True, all_favs=remote_favs_cache, creds=creds)

        if fav_count == 0 and elapsed_days is not None and elapsed_days < 3:
            logger.info(f"    ⏸️ Too soon: Only {elapsed_days:.1f} days since upload and no favorites found. Need 3 days for Google AI curation.")
            continue

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
                restart_planner()
            elif proceed_input == 'bypass' and fav_count == 0:
                now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("UPDATE month_batches SET status_code = ?, is_bypassed = 1, bypass_timestamp = ? WHERE month = ?", (selected_code, now_str, month))
                conn.commit()
                logger.info(f"✅ Month {month} status updated to {selected_code} (Direct-Rank Bypassed).")
                restart_planner()
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
                    conn.commit()
                    logger.info(f"Month {month} updated to 400.")
                    restart_planner()
            elif staging_folder and free_space >= remaining_to_upload:
                logger.info(f"🚀 Found {human_readable_size(remaining_to_upload)} left to upload for {month}. "
                            f"Available space: {human_readable_size(free_space)}. Priority given to finishing this batch.")
                selected_month = month
                selected_transition = transition
                break
            else:
                logger.warning(f"⚠️ Insufficient space for {month}. Free: {human_readable_size(free_space)}, Need: {human_readable_size(remaining_to_upload)}.")
                
                # Branch and suggest cleanup for months at stage 600
                cleanup_candidates = [m for m, s in [b[:2] for b in batches] if str(s) == '600']
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
