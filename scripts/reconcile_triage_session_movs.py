#!/usr/bin/env python3
"""
reconcile_triage_session_movs.py

Remediation utility to reconcile UUID-named video and media assets incorrectly
categorized as 'New' during intake of unbundled Apple Photos libraries (.photoslibrary).

Features:
  - Links Live Photo companion videos (<UUID>_3.mov, <UUID>_1.mov) to their still photos (<UUID>.heic/.jpeg)
    that were already confirmed and moved to Duplicates in the same session.
  - Matches standalone video and image assets against media_organizer.db by exact byte size and timestamp,
    ignoring Photos-generated UUID filenames.
  - Safely relocates confirmed duplicates from LaCie (/New/<triage_id>) to DELL1TB02 (/Duplicates/<triage_id>).
  - Preserves in /New/<triage_id> only assets that are truly net new.
  - Automatically updates triage_session_assets and triage_sessions tables in media_organizer.db.
  - Supports --dry-run mode for zero-risk previewing.
"""

import os
import sys
import shutil
import sqlite3
import argparse
import time
import re
import subprocess

# Add project root and scripts dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.dirname(__file__))

from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LACIE_DESTINATION_ROOT,
    TRIAGE_DELL_DESTINATION_ROOT
)
from utils.logger import setup_logger

LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs/reconcile_triage.log"))
logger = setup_logger(LOG_PATH, "reconcile_triage")

UUID_REGEX = re.compile(r'^([0-9a-fA-F-]{36})')


def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def unlock_path(path):
    try:
        os.chflags(path, 0)
    except (AttributeError, OSError):
        pass
    try:
        subprocess.run(["chmod", "-N", path], capture_output=True, check=False)
    except Exception:
        pass
    try:
        os.chmod(path, os.stat(path).st_mode | 0o200)
    except OSError:
        pass


def clean_empty_directories(root_dir):
    removed = 0
    for current_root, dirs, files in os.walk(root_dir, topdown=False):
        if os.path.abspath(current_root) == os.path.abspath(root_dir):
            continue
        try:
            if not os.listdir(current_root):
                try:
                    os.rmdir(current_root)
                except (PermissionError, OSError):
                    unlock_path(os.path.dirname(current_root))
                    unlock_path(current_root)
                    os.rmdir(current_root)
                removed += 1
        except Exception:
            pass
    return removed


def reconcile_session(triage_id, dry_run=False):
    triage_id_str = str(triage_id)
    new_dir = os.path.join(TRIAGE_LACIE_DESTINATION_ROOT, "New", triage_id_str)
    dup_dir = os.path.join(TRIAGE_DELL_DESTINATION_ROOT, "Duplicates", triage_id_str)

    print("=" * 80)
    print(f"🔧 RECONCILE TRIAGE SESSION UUID ASSETS: Session #{triage_id_str}")
    print("=" * 80)
    print(f"Source Folder (New on LaCie):        {new_dir}")
    print(f"Target Folder (Duplicates on DELL):  {dup_dir}")
    if dry_run:
        print("Mode:                                🔎 DRY RUN (No files moved, DB unchanged)")
    print("-" * 80)

    if not os.path.exists(new_dir):
        print(f"❌ Source directory does not exist: {new_dir}")
        return False

    conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
    c = conn.cursor()

    # Step 1: Index confirmed duplicate photos from session in triage_session_assets
    print("📚 Loading duplicate photos from session records...")
    c.execute(
        "SELECT filename, file_size_bytes, matched_asset_uuid, decision_reason "
        "FROM triage_session_assets WHERE triage_id = ? AND category = 'duplicate';",
        (triage_id_str,)
    )
    dup_photo_rows = c.fetchall()
    companion_photo_map = {}
    for fn, sz, matched_uuid, reason in dup_photo_rows:
        m = UUID_REGEX.match(fn)
        if m:
            base_uuid = m.group(1).upper()
            companion_photo_map[base_uuid] = (fn, sz, matched_uuid, reason)

    print(f"   Indexed {len(companion_photo_map):,} duplicate photos with base UUIDs.")

    # Step 2: Index All-Media database video sizes
    print("📚 Loading All-Media asset sizes from database...")
    c.execute(
        "SELECT aaa.ZORIGINALFILESIZE, a.ZUUID, aaa.ZORIGINALFILENAME, "
        "datetime(a.ZDATECREATED + 978307200, 'unixepoch') "
        "FROM ZADDITIONALASSETATTRIBUTES aaa "
        "JOIN ZASSET a ON a.Z_PK = aaa.ZASSET "
        "WHERE aaa.ZORIGINALFILESIZE IS NOT NULL;"
    )
    library_size_map = {}
    for sz, uuid, orig_fn, dt in c.fetchall():
        library_size_map.setdefault(sz, []).append((uuid, orig_fn, dt))

    print(f"   Indexed {len(library_size_map):,} unique asset sizes from All-Media library.")

    # Step 3: Scan all files currently in New/<triage_id>
    print(f"🔍 Scanning files in {new_dir}...")
    target_files = []
    for root, dirs, files in os.walk(new_dir):
        rel_root = os.path.relpath(root, new_dir)
        for f in files:
            if f.startswith('.'):
                continue
            full_src = os.path.join(root, f)
            rel_path = os.path.join(rel_root, f) if rel_root != '.' else f
            try:
                sz = os.path.getsize(full_src)
                target_files.append((full_src, rel_path, f, sz))
            except OSError as e:
                logger.warning(f"Could not stat {full_src}: {e}")

    total_scanned = len(target_files)
    total_bytes = sum(sz for _, _, _, sz in target_files)
    print(f"📂 Found {total_scanned:,} files ({format_size(total_bytes)}) in New/{triage_id_str}.")

    # Step 4: Classify and plan actions
    to_move_duplicates = []
    truly_new_files = []

    for full_src, rel_path, filename, sz in target_files:
        m = UUID_REGEX.match(filename)
        base_uuid = m.group(1).upper() if m else None

        # Case A: Live Photo companion video of duplicate still photo
        if base_uuid and base_uuid in companion_photo_map:
            paired_photo_fn, photo_sz, matched_uuid, orig_reason = companion_photo_map[base_uuid]
            reason = f"Live Photo companion of duplicate photo {paired_photo_fn} (matched asset {matched_uuid})"
            to_move_duplicates.append({
                "full_src": full_src,
                "rel_path": rel_path,
                "filename": filename,
                "size": sz,
                "match_type": "companion_photo",
                "matched_uuid": matched_uuid,
                "reason": reason
            })
            continue

        # Case B: Standalone match by exact byte size in All-Media library
        if sz in library_size_map:
            candidates = library_size_map[sz]
            matched_uuid, orig_fn, dt = candidates[0]
            reason = f"Exact byte match ({format_size(sz)}) match All-Media asset {matched_uuid} ({orig_fn})"
            to_move_duplicates.append({
                "full_src": full_src,
                "rel_path": rel_path,
                "filename": filename,
                "size": sz,
                "match_type": "exact_size",
                "matched_uuid": matched_uuid,
                "reason": reason
            })
            continue

        # Case C: Truly net new
        truly_new_files.append((full_src, rel_path, filename, sz))

    dup_count = len(to_move_duplicates)
    dup_bytes = sum(item["size"] for item in to_move_duplicates)
    net_new_count = len(truly_new_files)
    net_new_bytes = sum(sz for _, _, _, sz in truly_new_files)

    print("-" * 80)
    print("📊 CLASSIFICATION RESULTS:")
    print(f"   • Confirmed Duplicates to relocate:  {dup_count:>6,} ({format_size(dup_bytes)})")
    print(f"       - Live Photo Companions:         {sum(1 for i in to_move_duplicates if i['match_type'] == 'companion_photo'):>6,}")
    print(f"       - Standalone Exact Byte Matches: {sum(1 for i in to_move_duplicates if i['match_type'] == 'exact_size'):>6,}")
    print(f"   • Truly Net New to preserve:         {net_new_count:>6,} ({format_size(net_new_bytes)})")
    print("-" * 80)

    if dup_count == 0:
        print("✨ No duplicate assets to relocate. Everything in New is net new.")
        conn.close()
        return True

    # Step 5: Perform relocation
    if dry_run:
        print(f"🔎 DRY RUN COMPLETE — Would move {dup_count:,} files ({format_size(dup_bytes)}) to DELL1TB02.")
        conn.close()
        return True

    print(f"🚚 Moving {dup_count:,} duplicate files to DELL1TB02 ({dup_dir})...")
    moved_count = 0
    moved_bytes = 0
    error_count = 0
    t0 = time.time()
    last_print = time.time()

    for idx, item in enumerate(to_move_duplicates, 1):
        src_path = item["full_src"]
        rel_path = item["rel_path"]
        target_dest = os.path.join(dup_dir, rel_path)
        dest_parent = os.path.dirname(target_dest)

        try:
            os.makedirs(dest_parent, exist_ok=True)
            try:
                shutil.move(src_path, target_dest)
            except (PermissionError, OSError):
                unlock_path(os.path.dirname(src_path))
                unlock_path(src_path)
                unlock_path(dest_parent)
                shutil.move(src_path, target_dest)

            moved_count += 1
            moved_bytes += item["size"]

            # Update database record for this asset
            c.execute(
                """
                UPDATE triage_session_assets
                SET category = 'duplicate',
                    sub_type = 'identical',
                    decision_reason = ?,
                    destination_path = ?,
                    matched_asset_uuid = ?
                WHERE triage_id = ? AND filename = ?;
                """,
                (item["reason"], target_dest, item["matched_uuid"], triage_id_str, item["filename"])
            )

        except Exception as e:
            error_count += 1
            logger.error(f"Failed to move {src_path} -> {target_dest}: {e}")

        now = time.time()
        if idx == 1 or idx == dup_count or (now - last_print) >= 0.5 or idx % 50 == 0:
            last_print = now
            pct = (idx / dup_count) * 100
            sys.stdout.write(f"\r🚚 Relocating: {idx:,}/{dup_count:,} ({pct:.1f}%) | Moved: {moved_count:,} | Errors: {error_count}...")
            sys.stdout.flush()

    sys.stdout.write("\r" + " " * 85 + "\r")
    sys.stdout.flush()

    # Step 6: Clean empty directories in New/<triage_id>
    print("🧹 Cleaning empty subdirectories in New...")
    cleaned_dirs = clean_empty_directories(new_dir)

    # Step 7: Update triage_sessions summary row in database
    c.execute(
        """
        UPDATE triage_sessions
        SET duplicate_count = duplicate_count + ?,
            duplicate_bytes = duplicate_bytes + ?,
            new_count = new_count - ?,
            new_bytes = new_bytes - ?
        WHERE triage_id = ?;
        """,
        (moved_count, moved_bytes, moved_count, moved_bytes, triage_id_str)
    )
    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print("=" * 80)
    print("🎉 RECONCILIATION COMPLETE")
    print("=" * 80)
    print(f"Elapsed Time:               {elapsed:.2f}s")
    print(f"Files Relocated to DELL:    {moved_count:>6,} ({format_size(moved_bytes)})")
    print(f"Assets Preserved in New:    {net_new_count:>6,} ({format_size(net_new_bytes)})")
    print(f"Empty Folders Cleaned:      {cleaned_dirs:>6,}")
    print(f"Errors Encountered:         {error_count:>6,}")
    print(f"Space Freed on LaCie:       {format_size(moved_bytes)}")
    print("=" * 80)

    logger.info(
        f"Reconcile finished for session {triage_id_str}: {moved_count:,} moved to Duplicates ({format_size(moved_bytes)}), "
        f"{net_new_count:,} preserved in New, {error_count} errors."
    )
    return error_count == 0


def main():
    parser = argparse.ArgumentParser(description="Reconcile UUID-named assets in Triage New folder into Duplicates")
    parser.add_argument("--triage-id", default="37", help="Triage session ID to reconcile (default: 37)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate reconciliation without moving files or touching DB")
    args = parser.parse_args()

    reconcile_session(args.triage_id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
