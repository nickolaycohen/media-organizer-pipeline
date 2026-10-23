#!/usr/bin/env python3
"""
merge_directories.py

A robust, production-grade directory merger utility for macOS and Linux.
Recursively merges all files from a source directory into a destination directory,
preserving the nested relative directory structure.

Features:
  - Move (default) or Copy mode (--copy / --move)
  - Identical duplicate detection (byte-by-byte chunk comparison):
      * In move mode: deletes the redundant source copy (safe deduplication)
      * In copy mode: skips redundant re-copying
  - Collision disambiguation (--on-collision rename|overwrite|skip|newer)
  - macOS Time Machine ACL & file lock auto-bypass (clears deny ACLs and uchg flags on PermissionError)
  - Clean empty source directories after moving (--no-clean to preserve)
  - Live progress bar in terminal and periodic logging to logs/merge_directories.log
  - Dry run mode (--dry-run) for previewing actions safely
  - Clean Ctrl-C (KeyboardInterrupt) handling
"""

import os
import sys
import shutil
import subprocess
import argparse
import time
import logging

# Add project root and script dir to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.dirname(__file__))

from constants import MERGE_LOG_PATH
from utils.logger import setup_logger

MODULE_TAG = "merge_directories"
logger = setup_logger(MERGE_LOG_PATH, MODULE_TAG)


def format_size(bytes_val):
    """Formats bytes into a readable string (e.g. 1.25 MB)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def unlock_path(path):
    """
    Clears macOS immutable flags (uchg, nouappnd) and Time Machine ACLs
    (such as 'group:everyone deny delete,delete_child,writeattr') to allow moving/deleting.
    """
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


def are_files_identical(p1, p2):
    """
    Fast, reliable byte-comparison of two files in 64KB chunks.
    Returns True if both files exist and have identical contents.
    """
    try:
        if os.path.getsize(p1) != os.path.getsize(p2):
            return False
        with open(p1, 'rb') as f1, open(p2, 'rb') as f2:
            while True:
                b1 = f1.read(65536)
                b2 = f2.read(65536)
                if b1 != b2:
                    return False
                if not b1:
                    return True
    except OSError:
        return False


def disambiguate_filename(dest_path):
    """
    Generates a unique destination path by appending _dup1, _dup2, etc.
    before the extension.
    """
    base, ext = os.path.splitext(dest_path)
    idx = 1
    while os.path.exists(f"{base}_dup{idx}{ext}"):
        idx += 1
    return f"{base}_dup{idx}{ext}"


def clean_empty_directories(root_dir, protected_paths=None):
    """
    Removes empty directories bottom-up starting from root_dir.
    Automatically unlocks directories if macOS deny ACLs prevent deletion.
    """
    protected = {os.path.abspath(p) for p in (protected_paths or [])}
    removed_count = 0
    for current_root, dirs, files in os.walk(root_dir, topdown=False):
        current_abs = os.path.abspath(current_root)
        if any(current_abs == p or current_abs.startswith(p + os.sep) for p in protected):
            continue
        if current_abs == os.path.abspath(root_dir):
            continue

        try:
            if not os.listdir(current_root):
                try:
                    os.rmdir(current_root)
                except (PermissionError, OSError):
                    unlock_path(os.path.dirname(current_root))
                    unlock_path(current_root)
                    os.rmdir(current_root)
                removed_count += 1
                logger.debug(f"🧹 Removed empty directory: {current_root}")
        except Exception:
            pass
    return removed_count


def merge_directories(
    source_dir,
    dest_dir,
    mode="move",
    collision_strategy="rename",
    dry_run=False,
    clean_empty=True
):
    """
    Recursively merges all files from source_dir into dest_dir.

    Args:
      source_dir: Directory to merge from
      dest_dir: Directory to merge into
      mode: 'move' or 'copy'
      collision_strategy: 'rename', 'overwrite', 'skip', 'newer'
      dry_run: If True, simulates without moving or copying
      clean_empty: If True (and mode=='move'), deletes empty source folders
    """
    source_dir = os.path.abspath(source_dir)
    dest_dir = os.path.abspath(dest_dir)

    print("=" * 78)
    print("📁 RECURSIVE FILE MERGER UTILITY")
    print("=" * 78)
    print(f"Source Directory:      {source_dir}")
    print(f"Destination Directory: {dest_dir}")
    print(f"Transfer Mode:         {mode.upper()}")
    print(f"Collision Strategy:    {collision_strategy.upper()}")
    print(f"Clean Empty Folders:   {'YES' if clean_empty and mode == 'move' else 'NO'}")
    if dry_run:
        print("Mode:                  🔎 DRY RUN (Simulating only - no files will be moved/copied)")
    print("-" * 78)

    logger.info(f"🚀 Starting Directory Merge: {source_dir} -> {dest_dir} [Mode: {mode}, Strategy: {collision_strategy}, DryRun: {dry_run}]")

    if not os.path.exists(source_dir):
        logger.error(f"❌ Source directory does not exist: {source_dir}")
        print(f"❌ Source directory does not exist: {source_dir}")
        return False

    if not os.path.isdir(source_dir):
        logger.error(f"❌ Source path is not a directory: {source_dir}")
        print(f"❌ Source path is not a directory: {source_dir}")
        return False

    # Prevent recursive looping if destination is inside source or vice versa
    if dest_dir == source_dir:
        logger.error("❌ Destination directory cannot be identical to source directory.")
        print("❌ Destination directory cannot be identical to source directory.")
        return False

    if dest_dir.startswith(source_dir + os.sep):
        logger.error("❌ Destination directory is nested inside the source directory. Merge would cause an infinite loop.")
        print("❌ Destination directory is nested inside the source directory.")
        return False

    # Step 1: Discover all files in source_dir
    print("🔍 Scanning source directory...")
    source_files = []
    last_scan_print = time.time()

    for root, dirs, files in os.walk(source_dir):
        rel_root = os.path.relpath(root, source_dir)
        for f in files:
            if f == ".DS_Store" or f.lower() in ("thumbs.db", "desktop.ini"):
                continue
            src_full = os.path.join(root, f)
            rel_path = os.path.join(rel_root, f) if rel_root != '.' else f
            try:
                sz = os.path.getsize(src_full)
                source_files.append((src_full, rel_path, sz))
            except OSError as e:
                logger.warning(f"Could not stat {src_full}: {e}")

        now = time.time()
        if len(source_files) > 0 and (now - last_scan_print) >= 1.0:
            last_scan_print = now
            sys.stdout.write(f"\r🔍 Found {len(source_files):,} files so far...")
            sys.stdout.flush()

    if source_files:
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()

    total_files = len(source_files)
    total_bytes = sum(sz for _, _, sz in source_files)
    print(f"📂 Found {total_files:,} files ({format_size(total_bytes)}) in source directory.")
    logger.info(f"📂 Found {total_files:,} files ({format_size(total_bytes)}) in source directory.")

    if total_files == 0:
        print("✨ Source directory is empty. Nothing to merge.")
        return True

    # Step 2: Execute or Simulate Merge
    transferred_count = 0
    transferred_bytes = 0
    dedup_count = 0
    dedup_bytes = 0
    renamed_count = 0
    skipped_count = 0
    overwritten_count = 0
    error_count = 0

    last_progress_time = time.time()
    t0 = time.time()

    for idx, (src_full, rel_path, sz) in enumerate(source_files, 1):
        target_dest = os.path.join(dest_dir, rel_path)
        dest_parent = os.path.dirname(target_dest)

        # Progress reporting
        now = time.time()
        pct = (idx / total_files) * 100
        if idx == 1 or idx == total_files or (now - last_progress_time) >= 0.5 or idx % 25 == 0:
            last_progress_time = now
            sys.stdout.write(
                f"\r🚚 Processing: {idx:,}/{total_files:,} ({pct:.1f}%) | "
                f"Transferred: {transferred_count:,} | Dedup: {dedup_count:,} | Renamed: {renamed_count:,}..."
            )
            sys.stdout.flush()

        if idx == 1 or idx % max(1, total_files // 10) == 0 or idx == total_files:
            logger.info(
                f"Progress: {idx:,}/{total_files:,} ({pct:.0f}%) | "
                f"Transferred: {transferred_count:,}, Dedup: {dedup_count:,}, Renamed: {renamed_count:,}"
            )

        # Evaluate target existence
        if os.path.exists(target_dest):
            # Check if destination file is identical
            if are_files_identical(src_full, target_dest):
                dedup_count += 1
                dedup_bytes += sz
                if not dry_run:
                    if mode == "move":
                        try:
                            os.remove(src_full)
                        except (PermissionError, OSError):
                            unlock_path(os.path.dirname(src_full))
                            unlock_path(src_full)
                            try:
                                os.remove(src_full)
                            except Exception as e:
                                logger.warning(f"Could not remove deduplicated source {src_full}: {e}")
                    logger.debug(f"Identical duplicate resolved: {rel_path}")
                continue

            # Non-identical collision
            if collision_strategy == "rename":
                final_dest = disambiguate_filename(target_dest)
                renamed_count += 1
            elif collision_strategy == "overwrite":
                final_dest = target_dest
                overwritten_count += 1
            elif collision_strategy == "skip":
                skipped_count += 1
                logger.debug(f"Skipped collision: {rel_path}")
                continue
            elif collision_strategy == "newer":
                try:
                    src_mtime = os.path.getmtime(src_full)
                    dst_mtime = os.path.getmtime(target_dest)
                    if src_mtime > dst_mtime:
                        final_dest = target_dest
                        overwritten_count += 1
                    else:
                        skipped_count += 1
                        continue
                except OSError:
                    final_dest = disambiguate_filename(target_dest)
                    renamed_count += 1
            else:
                final_dest = disambiguate_filename(target_dest)
                renamed_count += 1
        else:
            final_dest = target_dest

        # Perform transfer
        transferred_count += 1
        transferred_bytes += sz

        if not dry_run:
            try:
                os.makedirs(os.path.dirname(final_dest), exist_ok=True)
                if mode == "move":
                    try:
                        shutil.move(src_full, final_dest)
                    except (PermissionError, OSError):
                        unlock_path(os.path.dirname(src_full))
                        unlock_path(src_full)
                        unlock_path(os.path.dirname(final_dest))
                        shutil.move(src_full, final_dest)
                else:
                    try:
                        shutil.copy2(src_full, final_dest)
                    except (PermissionError, OSError):
                        unlock_path(os.path.dirname(final_dest))
                        shutil.copy2(src_full, final_dest)
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Failed to transfer {src_full} -> {final_dest}: {e}")

    sys.stdout.write("\r" + " " * 95 + "\r")
    sys.stdout.flush()

    # Step 3: Clean empty directories if moving
    removed_dirs = 0
    if clean_empty and mode == "move" and not dry_run:
        print("🧹 Cleaning empty source directories...")
        logger.info("🧹 Cleaning empty source directories...")
        removed_dirs = clean_empty_directories(source_dir)
        logger.info(f"✅ Removed {removed_dirs:,} empty directories from source.")

    elapsed = time.time() - t0

    # Summary Report
    print("=" * 78)
    print("📊 MERGE SUMMARY REPORT")
    print("=" * 78)
    print(f"Source Directory:      {source_dir}")
    print(f"Destination Directory: {dest_dir}")
    print(f"Execution Mode:        {mode.upper()}{' (DRY RUN)' if dry_run else ''}")
    print(f"Elapsed Time:          {elapsed:.2f}s")
    print("-" * 78)
    print(f"Total Source Files:    {total_files:>6,} ({format_size(total_bytes):>10})")
    print(f"Transferred:           {transferred_count:>6,} ({format_size(transferred_bytes):>10})")
    if dedup_count > 0:
        action_word = "Deduplicated (Source Deleted)" if mode == "move" else "Identical (Skipped)"
        print(f"{action_word:<23}: {dedup_count:>6,} ({format_size(dedup_bytes):>10})")
    if renamed_count > 0:
        print(f"Collisions Renamed:    {renamed_count:>6,}")
    if overwritten_count > 0:
        print(f"Overwritten:           {overwritten_count:>6,}")
    if skipped_count > 0:
        print(f"Skipped:               {skipped_count:>6,}")
    if removed_dirs > 0:
        print(f"Empty Folders Cleaned: {removed_dirs:>6,}")
    if error_count > 0:
        print(f"Errors Encountered:    {error_count:>6,}")
    print("=" * 78)
    print(f"📝 Full audit log: {MERGE_LOG_PATH}\n")

    logger.info(
        f"✅ Merge Finished in {elapsed:.2f}s: {transferred_count:,} transferred, "
        f"{dedup_count:,} deduped, {renamed_count:,} renamed, {error_count:,} errors."
    )
    return error_count == 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Recursively merges all files from a source directory into a destination directory.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Move all files from source to destination (default):
  python3 scripts/merge_directories.py /path/to/source /path/to/dest

  # Copy files instead of moving:
  python3 scripts/merge_directories.py /path/to/source /path/to/dest --copy

  # Dry run preview without making changes:
  python3 scripts/merge_directories.py /path/to/source /path/to/dest --dry-run

  # Overwrite existing files on collision:
  python3 scripts/merge_directories.py /path/to/source /path/to/dest --on-collision overwrite
        """
    )
    parser.add_argument("source", help="Source directory containing files to merge")
    parser.add_argument("destination", help="Destination directory to merge into")
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--move", dest="mode", action="store_const", const="move", default="move",
                            help="Move files from source to destination (default)")
    mode_group.add_argument("--copy", dest="mode", action="store_const", const="copy",
                            help="Copy files from source to destination (leaves source intact)")

    parser.add_argument("--on-collision", choices=["rename", "overwrite", "skip", "newer"], default="rename",
                        help="Collision resolution for non-identical files (default: rename with _dupX)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate the merge process without transferring files or deleting directories")
    parser.add_argument("--no-clean", action="store_true",
                        help="Do not delete empty directories from the source folder after moving")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        success = merge_directories(
            args.source,
            args.destination,
            mode=args.mode,
            collision_strategy=args.on_collision,
            dry_run=args.dry_run,
            clean_empty=not args.no_clean
        )
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Directory merge cancelled by user (Ctrl-C).")
        logger.warning("⚠️ Directory merge cancelled by user (KeyboardInterrupt).")
        sys.exit(130)
