#!/usr/bin/env python3
"""
triage_photos_library.py

Standalone Apple Photos Library Intake & Deduplication Utility.
Processes Apple Photos Library packages (.photoslibrary / .photolibrary) in 100% Read-Only Full Replica mode:
  - Replicates Duplicates to DELL1TB02: /Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/Duplicates/<triage_id>
  - Replicates Review-Similar to LaCie: /Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/Review-Similar/<triage_id>
  - Replicates Novel Media to LaCie:   /Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/New/<triage_id>

Protections:
  - 100% Read-Only: Source library files, directories, and bundles are NEVER moved, deleted, renamed, or modified.
  - Automatically locates originals/ or Masters/ while skipping internal thumbnail caches (resources/), derivatives, and private DBs.
  - Normalizes Apple Photos UUID companion video stems (<UUID>_3.mov -> <UUID>) so Live Photos remain paired.
  - Queries exact byte sizes (aaa.ZORIGINALFILESIZE) across All-Media.photoslibrary to identify standalone UUID assets.
  - Records session history and asset classifications atomically in SQLite.
"""

import os
import sys
import shutil
import sqlite3
import argparse
import time
import re

# Add project root and script dir to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.dirname(__file__))

from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LACIE_DESTINATION_ROOT,
    TRIAGE_DELL_DESTINATION_ROOT,
    MEDIA_EXTENSIONS
)
from utils.logger import setup_logger
from scripts.triage_media_intake import (
    LibraryIndex,
    determine_triage_id,
    ensure_triage_tables,
    tag_triage_folders,
    safe_copy,
    format_size
)

LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs/triage_photos_library.log"))
MODULE_TAG = "triage_photos_library"
logger = setup_logger(LOG_PATH, MODULE_TAG)

IGNORED_LIBRARY_SUBDIRS = {"resources", "private", "database", "scopes", "derivatives"}
IGNORED_FILE_NAMES = {".ds_store", "thumbs.db", "desktop.ini"}


def resolve_library_media_root(library_path):
    """
    Given a path to a .photoslibrary or .photolibrary bundle,
    locates the internal media directory (originals/ or Masters/).
    """
    lib_abs = os.path.abspath(library_path)
    if not os.path.exists(lib_abs):
        raise FileNotFoundError(f"Photos Library does not exist: {lib_abs}")

    originals_dir = os.path.join(lib_abs, "originals")
    masters_dir = os.path.join(lib_abs, "Masters")

    if os.path.isdir(originals_dir):
        return originals_dir
    elif os.path.isdir(masters_dir):
        return masters_dir
    return lib_abs


def load_photos_library_metadata(library_path):
    """
    Attempts to read the internal Apple Photos SQLite database (Photos.sqlite or photos.db)
    from the library package. Returns a dictionary mapping lower-cased UUIDs and internal
    filenames to their original camera filenames (e.g. '0000609D...jpeg' -> 'IMG_4614.JPG').

    To avoid slow seek latency on external mechanical HDDs, this function copies the
    SQLite database to /tmp, queries the metadata tables, and cleans up the temp copy.
    """
    lib_abs = os.path.abspath(library_path)
    db_candidates = [
        os.path.join(lib_abs, "database", "Photos.sqlite"),
        os.path.join(lib_abs, "database", "photos.db"),
    ]
    source_db = None
    for cand in db_candidates:
        if os.path.isfile(cand):
            source_db = cand
            break

    if not source_db:
        return {}

    tmp_db = f"/tmp/photos_meta_{os.getpid()}_{int(time.time())}.sqlite"
    name_map = {}

    try:
        logger.info(f"📚 Copying internal Photos database to local temporary storage for high-speed indexing: {source_db}")
        sys.stdout.write("📚 Reading internal Photos SQLite database for original filenames...\n")
        sys.stdout.flush()
        shutil.copyfile(source_db, tmp_db)

        conn = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
        cur = conn.cursor()

        # Check existing tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {r[0] for r in cur.fetchall()}

        # 1. Modern macOS Photos (ZASSET or ZGENERICASSET + ZADDITIONALASSETATTRIBUTES)
        asset_table = "ZASSET" if "ZASSET" in existing_tables else ("ZGENERICASSET" if "ZGENERICASSET" in existing_tables else None)
        if asset_table and "ZADDITIONALASSETATTRIBUTES" in existing_tables:
            cur.execute(f"SELECT Z_PK, ZUUID, ZFILENAME FROM {asset_table}")
            asset_map = {}
            for pk, uuid, fname in cur.fetchall():
                asset_map[pk] = (uuid, fname)

            cur.execute("SELECT ZASSET, ZORIGINALFILENAME FROM ZADDITIONALASSETATTRIBUTES WHERE ZORIGINALFILENAME IS NOT NULL")
            for asset_pk, orig_fname in cur.fetchall():
                if asset_pk in asset_map:
                    uuid, fname = asset_map[asset_pk]
                    if uuid:
                        name_map[uuid.lower()] = orig_fname
                    if fname:
                        name_map[fname.lower()] = orig_fname
                        stem = os.path.splitext(fname)[0]
                        name_map[stem.lower()] = orig_fname

        # 2. Older iPhoto / Aperture (RKMaster)
        elif "RKMaster" in existing_tables:
            cur.execute("SELECT uuid, fileName, originalFileName FROM RKMaster WHERE originalFileName IS NOT NULL")
            for uuid, fname, orig_fname in cur.fetchall():
                if uuid:
                    name_map[uuid.lower()] = orig_fname
                if fname:
                    name_map[fname.lower()] = orig_fname
                    stem = os.path.splitext(fname)[0]
                    name_map[stem.lower()] = orig_fname

        conn.close()
        logger.info(f"✅ Indexed {len(name_map):,} original filename mappings from internal library database.")
        print(f"✅ Indexed {len(name_map):,} original filename mappings from internal library database.")
    except Exception as e:
        logger.warning(f"Could not extract original filenames from Photos database: {e}")
    finally:
        if os.path.exists(tmp_db):
            try:
                os.remove(tmp_db)
            except OSError:
                pass

    return name_map


def resolve_asset_original_name(filename, original_names_map):
    """
    Resolves the original camera filename (e.g. IMG_4614.JPG) for a given
    Apple Photos asset filename (e.g. 0000609D-6414-4E51-A7CF-83B6283E8632.jpeg or <UUID>_3.mov).
    """
    if not original_names_map:
        return filename

    f_lower = filename.lower()
    if f_lower in original_names_map:
        return original_names_map[f_lower]

    stem, ext = os.path.splitext(filename)
    stem_lower = stem.lower()
    if stem_lower in original_names_map:
        return original_names_map[stem_lower]

    # Handle Live Photo companion video UUIDs (e.g. <UUID>_3.mov or <UUID>_1.mov)
    clean_stem = re.sub(r'_\d+$', '', stem)
    clean_lower = clean_stem.lower()
    if clean_lower in original_names_map:
        base_orig = original_names_map[clean_lower]
        base_stem, _ = os.path.splitext(base_orig)
        orig_ext = ext if ext.startswith('.') else f".{ext}"
        if any(c.isupper() for c in base_orig):
            orig_ext = orig_ext.upper()
        return f"{base_stem}{orig_ext}"

    return filename


def triage_photos_library(library_path, lacie_dest_root=None, dell_dest_root=None, triage_id=None, dry_run=False, ignore_duplicates=False):
    library_path = os.path.abspath(library_path)
    media_root = resolve_library_media_root(library_path)
    lacie_dest_root = os.path.abspath(lacie_dest_root or TRIAGE_LACIE_DESTINATION_ROOT)
    dell_dest_root = os.path.abspath(dell_dest_root or TRIAGE_DELL_DESTINATION_ROOT)

    triage_id = determine_triage_id(MEDIA_ORGANIZER_DB_PATH, lacie_dest_root, dell_dest_root, requested_id=triage_id)

    # Attach triage_id to the logger
    setup_logger(LOG_PATH, MODULE_TAG, extra_fields={"triage_id": triage_id})

    duplicates_dir = os.path.join(dell_dest_root, "Duplicates", triage_id)
    review_dir = os.path.join(lacie_dest_root, "Review-Similar", triage_id)
    new_dir = os.path.join(lacie_dest_root, "New", triage_id)
    session_dest_desc = (
        f"DELL1TB02: [Skipped Duplicates] | LaCie: [Review-Similar|New]/{triage_id}"
        if ignore_duplicates
        else f"DELL1TB02: Duplicates/{triage_id} | LaCie: [Review-Similar|New]/{triage_id}"
    )

    print("=" * 80)
    print(f"🛡️  APPLE PHOTOS LIBRARY INTAKE TRIAGE [Session ID: {triage_id}]")
    print("=" * 80)
    print(f"Source Library:         {library_path}")
    print(f"Resolved Media Root:    {media_root}")
    print(f"Destination Hierarchy:  {session_dest_desc}")
    mode_desc = "100% READ-ONLY NOVEL REPLICA (Duplicates Ignored/Skipped)" if ignore_duplicates else "100% READ-ONLY FULL REPLICA (Source package completely untouched)"
    print(f"Mode:                   {mode_desc}")
    if ignore_duplicates:
        print(f"Duplicates Policy:      ⚡ IGNORE DUPLICATES (No duplicate files will be copied to DELL1TB02)")
    if dry_run:
        print(f"Operation:              🔎 DRY RUN SIMULATION (No files will be copied, DB unchanged)")
    print("-" * 80)

    logger.info(f"🚀 Starting Photos Library Triage [Triage ID: {triage_id}]")
    logger.info(f"   - Library Package:         {library_path}")
    logger.info(f"   - Media Root:              {media_root}")
    if ignore_duplicates:
        logger.info(f"   - Duplicates Destination:  SKIPPED (--ignore-duplicates active)")
    else:
        logger.info(f"   - Duplicates Destination:  {duplicates_dir} (DELL1TB02)")
    logger.info(f"   - Review & New (LaCie):    {lacie_dest_root}/[Review-Similar|New]/{triage_id}")

    # Check DELL1TB02 availability only if duplicates replication is active
    if not dry_run and not ignore_duplicates and not os.path.exists("/Volumes/DELL1TB02"):
        logger.error("❌ DELL1TB02 drive is not mounted at /Volumes/DELL1TB02! Please connect DELL1TB02.")
        raise FileNotFoundError("DELL1TB02 volume not mounted: /Volumes/DELL1TB02")

    # Record session in DB as running
    db_conn = None
    db_session_pk = None
    if not dry_run:
        try:
            db_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            ensure_triage_tables(db_conn)
            cur = db_conn.cursor()
            cur.execute("""
                INSERT INTO triage_sessions (
                    triage_id, source_directory, destination_directory, status, dry_run, started_at_utc
                ) VALUES (?, ?, ?, 'running', 0, datetime('now'))
            """, (triage_id, media_root, session_dest_desc))
            db_session_pk = cur.lastrowid
            db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to record triage session in DB: {e}")

    try:
        # Step 1: Load All-Media library catalog and index
        index = LibraryIndex(MEDIA_ORGANIZER_DB_PATH)
        index.load()

        # Step 2: Index original filenames if internal Photos SQLite database is available
        original_names_map = load_photos_library_metadata(library_path)
        if original_names_map:
            print("   ✨ Original camera filenames will be restored for all evaluated and exported assets.")

        # Discover candidate media files inside library originals/Masters
        logger.info(f"🔍 Scanning library media directory: {media_root}")
        print(f"🔍 Scanning library media directory: {media_root}")
        candidate_files = []
        last_scan_time = time.time()

        for root, dirs, files in os.walk(media_root):
            rel_root = os.path.relpath(root, media_root)
            # Skip internal caches, face thumbnails, derivatives
            rel_parts = [p.lower() for p in rel_root.split(os.sep) if p and p != '.']
            if rel_parts and rel_parts[0] in IGNORED_LIBRARY_SUBDIRS:
                continue

            for f in sorted(files):
                if f.startswith('.') or f.lower() in IGNORED_FILE_NAMES:
                    continue
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue

                full_p = os.path.join(root, f)
                rel_p = os.path.relpath(full_p, media_root)

                try:
                    sz = os.path.getsize(full_p)
                    orig_f = resolve_asset_original_name(f, original_names_map)
                    candidate_files.append((full_p, rel_p, f, orig_f, sz))
                except OSError as e:
                    logger.warning(f"Could not stat {full_p}: {e}")

            now = time.time()
            if len(candidate_files) > 0 and (now - last_scan_time) >= 1.0:
                last_scan_time = now
                sys.stdout.write(f"\r🔍 Found {len(candidate_files):,} candidate files so far...")
                sys.stdout.flush()

        if candidate_files:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

        logger.info(f"📂 Found {len(candidate_files):,} candidate media files in library.")
        print(f"📂 Found {len(candidate_files):,} candidate media files in library.")

        if not candidate_files:
            print("✨ No media files found in library to triage.")
            logger.info("✨ No media files found in library to triage.")
            if db_conn and db_session_pk:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'completed', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
                db_conn.close()
            return

        # Step 3: Live Photo companion grouping (normalizing stems)
        grouped_by_stem = {}
        for full_p, rel_p, f, orig_f, sz in candidate_files:
            dirname = os.path.dirname(rel_p)
            orig_stem, orig_ext = os.path.splitext(orig_f)
            clean_stem = re.sub(r'_\d+$', '', orig_stem)
            key = (dirname, clean_stem.lower())
            grouped_by_stem.setdefault(key, []).append((full_p, rel_p, f, orig_f, sz, orig_ext.lower()))

        # Step 4: Evaluate and classify assets
        duplicates_list = []
        review_list = []
        new_list = []

        identical_dup_count = 0
        identical_dup_bytes = 0
        compressed_dup_count = 0
        compressed_dup_bytes = 0
        visual_dup_count = 0
        visual_dup_bytes = 0

        upgrade_count = 0
        upgrade_bytes = 0
        visual_sim_count = 0
        visual_sim_bytes = 0
        discard_sim_count = 0
        discard_sim_bytes = 0
        web_artifact_count = 0
        web_artifact_bytes = 0
        moment_matched_count = 0

        total_dup_bytes = 0
        total_review_bytes = 0
        total_new_bytes = 0

        total_groups = len(grouped_by_stem)
        last_progress_time = time.time()

        for idx, ((dirname, stem_lower), file_group) in enumerate(grouped_by_stem.items(), 1):
            now = time.time()
            pct = (idx / total_groups) * 100
            if idx == 1 or idx == total_groups or (now - last_progress_time) >= 0.5 or idx % 25 == 0:
                last_progress_time = now
                sys.stdout.write(
                    f"\r⏳ Evaluating library assets: {idx:,}/{total_groups:,} ({pct:.1f}%) | "
                    f"Dups: {len(duplicates_list):,} | Review: {len(review_list):,} | New: {len(new_list):,}..."
                )
                sys.stdout.flush()

            primary_candidates = [item for item in file_group if item[5] not in ('.mov', '.mp4')]
            video_candidates = [item for item in file_group if item[5] in ('.mov', '.mp4')]

            if primary_candidates:
                p_full, p_rel, p_f, p_orig_f, p_sz, p_ext = primary_candidates[0]
                cat, sub_type, reason, matched, visual_meta = index.evaluate_asset(
                    p_full, p_rel, p_sz, p_orig_f, is_photos_library=True
                )

                if visual_meta and visual_meta.get("matched_moment_name"):
                    moment_matched_count += len(file_group)

                if cat == "duplicate":
                    if sub_type == "visual_duplicate":
                        visual_dup_count += len(file_group)
                        visual_dup_bytes += sum(it[4] for it in file_group)
                    elif sub_type == "compressed":
                        compressed_dup_count += len(file_group)
                        compressed_dup_bytes += sum(it[4] for it in file_group)
                    else:
                        identical_dup_count += len(file_group)
                        identical_dup_bytes += sum(it[4] for it in file_group)

                    for item in primary_candidates:
                        duplicates_list.append((item[0], item[1], item[3], item[4], reason, sub_type, visual_meta))
                        total_dup_bytes += item[4]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to duplicate {p_orig_f} ({reason})"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        duplicates_list.append((v_item[0], v_item[1], v_item[3], v_item[4], v_reason, sub_type, v_meta))
                        total_dup_bytes += v_item[4]

                elif cat == "review_similar":
                    if sub_type == "upgrade_candidate":
                        upgrade_count += len(file_group)
                        upgrade_bytes += sum(it[4] for it in file_group)
                    elif sub_type == "visual_similar":
                        visual_sim_count += len(file_group)
                        visual_sim_bytes += sum(it[4] for it in file_group)
                    elif sub_type == "web_artifact":
                        web_artifact_count += len(file_group)
                        web_artifact_bytes += sum(it[4] for it in file_group)
                    else:
                        discard_sim_count += len(file_group)
                        discard_sim_bytes += sum(it[4] for it in file_group)

                    for item in primary_candidates:
                        review_list.append((item[0], item[1], item[3], item[4], reason, sub_type, visual_meta))
                        total_review_bytes += item[4]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to review-similar asset {p_orig_f} ({reason})"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        review_list.append((v_item[0], v_item[1], v_item[3], v_item[4], v_reason, sub_type, v_meta))
                        total_review_bytes += v_item[4]

                else:
                    for item in primary_candidates:
                        new_list.append((item[0], item[1], item[3], item[4], reason, sub_type, visual_meta))
                        total_new_bytes += item[4]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to new asset {p_orig_f}"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        new_list.append((v_item[0], v_item[1], v_item[3], v_item[4], v_reason, sub_type, v_meta))
                        total_new_bytes += v_item[4]

            else:
                # Standalone videos
                for v_item in video_candidates:
                    v_full, v_rel, v_f, v_orig_f, v_sz, v_ext = v_item
                    cat, sub_type, reason, matched, visual_meta = index.evaluate_asset(
                        v_full, v_rel, v_sz, v_orig_f, is_photos_library=True
                    )

                    if visual_meta and visual_meta.get("matched_moment_name"):
                        moment_matched_count += 1

                    if cat == "duplicate":
                        if sub_type == "compressed":
                            compressed_dup_count += 1
                            compressed_dup_bytes += v_sz
                        else:
                            identical_dup_count += 1
                            identical_dup_bytes += v_sz
                        duplicates_list.append((v_full, v_rel, v_orig_f, v_sz, reason, sub_type, visual_meta))
                        total_dup_bytes += v_sz
                    elif cat == "review_similar":
                        if sub_type == "upgrade_candidate":
                            upgrade_count += 1
                            upgrade_bytes += v_sz
                        elif sub_type == "web_artifact":
                            web_artifact_count += 1
                            web_artifact_bytes += v_sz
                        else:
                            discard_sim_count += 1
                            discard_sim_bytes += v_sz
                        review_list.append((v_full, v_rel, v_orig_f, v_sz, reason, sub_type, visual_meta))
                        total_review_bytes += v_sz
                    else:
                        new_list.append((v_full, v_rel, v_orig_f, v_sz, reason, sub_type, visual_meta))
                        total_new_bytes += v_sz

        sys.stdout.write("\r" + " " * 95 + "\r")
        sys.stdout.flush()

        # Step 5: Summary Output
        print("\n" + "=" * 80)
        print(f"📊 PHOTOS LIBRARY TRIAGE SUMMARY [Triage ID: {triage_id}]")
        print("=" * 80)
        print(f"Library Source:        {library_path}")
        print(f"Destination Hierarchy: {session_dest_desc}")
        print(f"Total Files Scanned:   {len(candidate_files):,}")
        print("-" * 80)
        print(f"1. Duplicates Total:                                {len(duplicates_list):>5,} assets ({format_size(total_dup_bytes):>10})")
        print(f"   - Identical Matches:                             {identical_dup_count:>5,} assets ({format_size(identical_dup_bytes):>10})")
        print(f"   - Compressed Copies (Library is Superior):      {compressed_dup_count:>5,} assets ({format_size(compressed_dup_bytes):>10})")
        if visual_dup_count > 0:
            print(f"   - 👁️ Visual Duplicates (>=95% Match):             {visual_dup_count:>5,} assets ({format_size(visual_dup_bytes):>10})")
        print("-" * 80)
        print(f"2. Review-Similar Total:                            {len(review_list):>5,} assets ({format_size(total_review_bytes):>10})")
        print(f"   - 🌟 Upgrade Candidates (Intake is Better):      {upgrade_count:>5,} assets ({format_size(upgrade_bytes):>10})")
        if visual_sim_count > 0:
            print(f"   - 👁️ Visually Similar Photos (88-94% Match):      {visual_sim_count:>5,} assets ({format_size(visual_sim_bytes):>10})")
        print(f"   - Suspected Discard / Burst Siblings:           {discard_sim_count:>5,} assets ({format_size(discard_sim_bytes):>10})")
        print(f"   - Web / UI Graphic Artifacts (<50KB):           {web_artifact_count:>5,} assets ({format_size(web_artifact_bytes):>10})")
        if moment_matched_count > 0:
            print(f"   - 📍 Apple Photos Moment Matches:                 {moment_matched_count:>5,} assets")
        print("-" * 80)
        print(f"3. New (High-Confidence Clean Assets):              {len(new_list):>5,} assets ({format_size(total_new_bytes):>10})")
        print("=" * 80 + "\n")

        logger.info(
            f"Triage result for {library_path}: {len(duplicates_list):,} Duplicates ({format_size(total_dup_bytes)}), "
            f"{len(review_list):,} Review-Similar ({format_size(total_review_bytes)}), {len(new_list):,} New ({format_size(total_new_bytes)})"
        )

        if dry_run:
            print(f"🔍 [DRY RUN] Triage ID: {triage_id}")
            print(f"   Destination Hierarchy:")
            if ignore_duplicates:
                print(f"   - DELL1TB02: [SKIPPED via --ignore-duplicates]")
            else:
                print(f"   - DELL1TB02: {dell_dest_root}/Duplicates/{triage_id}/")
            print(f"   - LaCie:     {lacie_dest_root}/[Review-Similar|New]/{triage_id}/")
            print(f"   Would replicate (copy):")
            if ignore_duplicates:
                print(f"   - Duplicates: SKIPPED ({len(duplicates_list):,} duplicate assets will NOT be copied)")
            else:
                print(f"   - {len(duplicates_list):,} duplicates to DELL1TB02: '{duplicates_dir}/'")
            print(f"   - {len(review_list):,} review items to LaCie: '{review_dir}/'")
            print(f"   - {len(new_list):,} clean new assets to LaCie: '{new_dir}/'")
            print(f"\n   Source Protection: Source package {library_path} remains 100% UNTOUCHED.")
            print(f"📝 Logs appended to: {LOG_PATH}")
            return

        # Step 6: Perform non-destructive replication (safe_copy)
        total_to_move = (0 if ignore_duplicates else len(duplicates_list)) + len(review_list) + len(new_list)
        if ignore_duplicates:
            logger.info(f"🚚 Replicating {total_to_move:,} novel/review files from library (skipping {len(duplicates_list):,} duplicates)...")
            print(f"🚚 Replicating {total_to_move:,} files into destination folders (skipping {len(duplicates_list):,} duplicates)...")
        else:
            logger.info(f"🚚 Replicating {total_to_move:,} triaged files from library to destination folders...")
            print(f"🚚 Replicating (Copying) {total_to_move:,} files into destination folders...")
        replicated_duplicates = 0
        replicated_review = 0
        replicated_new = 0
        moved_total = 0
        last_move_time = time.time()

        def report_copy_progress():
            nonlocal last_move_time
            now = time.time()
            pct = (moved_total / total_to_move) * 100 if total_to_move else 100.0
            if moved_total == 1 or moved_total == total_to_move or (now - last_move_time) >= 0.5 or moved_total % 25 == 0:
                last_move_time = now
                sys.stdout.write(f"\r🚚 Replicating files: {moved_total:,}/{total_to_move:,} ({pct:.1f}%)...")
                sys.stdout.flush()
            if moved_total == 1 or moved_total % max(1, total_to_move // 10) == 0 or moved_total == total_to_move:
                logger.info(f"🚚 Replication Progress: {moved_total:,}/{total_to_move:,} ({pct:.0f}%)")

        if not ignore_duplicates:
            for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in duplicates_list:
                dest_full = os.path.join(duplicates_dir, orig_f)
                try:
                    dest_full = safe_copy(src_full, dest_full)
                    replicated_duplicates += 1
                except Exception as e:
                    logger.error(f"❌ Failed to copy duplicate {src_full} -> {dest_full}: {e}")
                moved_total += 1
                report_copy_progress()

        for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in review_list:
            if sub_t == "upgrade_candidate":
                dest_full = os.path.join(review_dir, "Upgrade-Candidates", orig_f)
            else:
                dest_full = os.path.join(review_dir, orig_f)
            try:
                dest_full = safe_copy(src_full, dest_full)
                replicated_review += 1
            except Exception as e:
                logger.error(f"❌ Failed to copy review asset {src_full} -> {dest_full}: {e}")
            moved_total += 1
            report_copy_progress()

        for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in new_list:
            dest_full = os.path.join(new_dir, orig_f)
            try:
                dest_full = safe_copy(src_full, dest_full)
                replicated_new += 1
            except Exception as e:
                logger.error(f"❌ Failed to copy new asset {src_full} -> {dest_full}: {e}")
            moved_total += 1
            report_copy_progress()

        if total_to_move > 0:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

        if ignore_duplicates:
            logger.info(f"⏭️  Skipped replicating {len(duplicates_list):,} duplicates (--ignore-duplicates active)")
            print(f"⏭️  Skipped replicating {len(duplicates_list):,} duplicates (--ignore-duplicates active)")
        else:
            logger.info(f"✅ Replicated {replicated_duplicates:,} duplicates to DELL1TB02: {duplicates_dir}")
            print(f"✅ Replicated {replicated_duplicates:,} duplicates to DELL1TB02: {duplicates_dir}")
        logger.info(f"✅ Replicated {replicated_review:,} review assets to LaCie: {review_dir}")
        logger.info(f"✅ Replicated {replicated_new:,} new assets to LaCie: {new_dir}")
        print(f"✅ Replicated {replicated_review:,} review assets to LaCie: {review_dir}")
        print(f"✅ Replicated {replicated_new:,} new assets to LaCie: {new_dir}")

        # Step 7: Apply macOS Finder color tags
        logger.info("🏷️  Applying macOS Finder color tags on DELL1TB02 and LaCie...")
        tag_results = tag_triage_folders(lacie_dest_root=lacie_dest_root, dell_dest_root=dell_dest_root, triage_id=triage_id)

        # Step 8: Update database records
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("""
                    UPDATE triage_sessions SET
                        status = 'completed',
                        total_files_scanned = ?,
                        duplicates_count = ?,
                        duplicates_bytes = ?,
                        compressed_duplicates_count = ?,
                        compressed_duplicates_bytes = ?,
                        review_similar_count = ?,
                        review_similar_bytes = ?,
                        upgrade_candidates_count = ?,
                        upgrade_candidates_bytes = ?,
                        discard_sim_count = ?,
                        discard_sim_bytes = ?,
                        web_artifact_count = ?,
                        web_artifact_bytes = ?,
                        new_count = ?,
                        new_bytes = ?,
                        visual_duplicate_count = ?,
                        moment_matched_count = ?,
                        completed_at_utc = datetime('now')
                    WHERE id = ?
                """, (
                    len(candidate_files),
                    len(duplicates_list), total_dup_bytes,
                    compressed_dup_count, compressed_dup_bytes,
                    replicated_review, total_review_bytes,
                    upgrade_count, upgrade_bytes,
                    discard_sim_count, discard_sim_bytes,
                    web_artifact_count, web_artifact_bytes,
                    replicated_new, total_new_bytes,
                    visual_dup_count, moment_matched_count,
                    db_session_pk
                ))

                asset_rows = []
                for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in duplicates_list:
                    dest_p = "SKIPPED_DUPLICATE" if ignore_duplicates else os.path.join(duplicates_dir, orig_f)
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, orig_f, sz, "duplicate", sub_t, reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))
                for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in review_list:
                    sub_f = "Upgrade-Candidates" if sub_t == "upgrade_candidate" else ""
                    dest_p = os.path.join(review_dir, sub_f, orig_f) if sub_f else os.path.join(review_dir, orig_f)
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, orig_f, sz, "review_similar", sub_t, reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))
                for src_full, rel_p, orig_f, sz, reason, sub_t, vmeta in new_list:
                    dest_p = os.path.join(new_dir, orig_f)
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, orig_f, sz, "new", sub_t or "clean_media", reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))


                cur.executemany("""
                    INSERT INTO triage_session_assets (
                        triage_session_id, triage_id, original_rel_path, filename,
                        file_size_bytes, category, sub_type, decision_reason, destination_path,
                        matched_asset_uuid, matched_moment_name, visual_similarity_score,
                        hamming_distance, perceptual_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, asset_rows)
                db_conn.commit()
                logger.info(f"💾 Logged triage session '{triage_id}' and {len(asset_rows):,} asset records in DB.")
            except Exception as e:
                logger.error(f"Failed to update triage session in DB: {e}")
            finally:
                db_conn.close()

        print(f"✅ Triage Complete! [Triage ID: {triage_id}]")
        print(f"   Destination Hierarchy: {session_dest_desc}")
        if ignore_duplicates:
            print(f"   - {len(duplicates_list):,} duplicates skipped/not copied (--ignore-duplicates)")
        else:
            print(f"   - {replicated_duplicates:,} duplicates replicated to DELL1TB02: '{duplicates_dir}/'")
        print(f"   - {replicated_review:,} review assets replicated to LaCie: '{review_dir}/'")
        print(f"   - {replicated_new:,} clean new assets replicated to LaCie: '{new_dir}/'")

        if tag_results:
            print(f"🏷️  Finder Color Tags Applied:")
            for folder_rel, (color, ok) in tag_results.items():
                icon = "🔴" if color == "Red" else "🟡" if color == "Yellow" else "🟠" if color == "Orange" else "🟢"
                status = "✅" if ok else "⚠️"
                print(f"   {status} {folder_rel:<45} -> {color} {icon}")
        print(f"📝 Full audit log: {LOG_PATH}")

        print("\n" + "=" * 80)
        print("🛡️  PHOTOS MEDIA LIBRARY PROCESSED (READ-ONLY MODE)")
        print("-" * 80)
        print(f"The following Apple Photos Library package was processed as a read-only source:")
        print(f"   📦 {library_path}")
        print("\nAll assets were evaluated and processed:")
        if ignore_duplicates:
            print(f"   • {len(duplicates_list):,} duplicates identified & skipped (not copied to DELL1TB02)")
        else:
            print(f"   • {replicated_duplicates:,} duplicates copied to DELL1TB02: '{duplicates_dir}/'")
        print(f"   • {replicated_review:,} review assets copied to LaCie: '{review_dir}/'")
        print(f"   • {replicated_new:,} clean new assets copied to LaCie: '{new_dir}/'")
        print("\nSource Library Protection:")
        print("   ✅ 100% READ-ONLY: Source files and folders remain completely unmodified.")
        print("\n👉 ACTION REQUIRED:")
        print(f"   The source Photos Library package ({library_path})")
        print("   must now be MANUALLY PROCESSED or DELETED when you have verified the intake.")
        print("=" * 80 + "\n")

    except KeyboardInterrupt:
        logger.warning(f"\n⚠️ Triage session '{triage_id}' cancelled by user (KeyboardInterrupt).")
        print(f"\n\n⚠️ Triage session '{triage_id}' cancelled by user (Ctrl-C). No files were copied.")
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'cancelled', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
            except Exception:
                pass
            finally:
                db_conn.close()
        return

    except Exception as e:
        logger.error(f"❌ Triage session failed unexpectedly: {e}", exc_info=True)
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'failed', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
            except Exception:
                pass
            finally:
                db_conn.close()
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Triage and replicate assets from an Apple Photos Library (.photoslibrary / .photolibrary)\n"
            "in 100% Read-Only Full Replica mode:\n"
            "  - DELL1TB02: /Volumes/DELL1TB02/.../Duplicates/<triage_id>\n"
            "  - LaCie:     /Volumes/LaCie/.../[Review-Similar|New]/<triage_id>\n\n"
            "Protections:\n"
            "  - The source library is strictly read-only (zero moves, zero deletions, zero modifications).\n"
            "  - Automatically targets inner originals/ or Masters/ while skipping internal cache dirs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("library_path",
                        help="Path to the Apple Photos Library bundle (.photoslibrary / .photolibrary)")
    parser.add_argument("--lacie-dest-dir", "--dest-dir", dest="lacie_dest_dir", default=None,
                        help=f"LaCie base destination directory for Review-Similar and New (default: {TRIAGE_LACIE_DESTINATION_ROOT})")
    parser.add_argument("--dell-dest-dir", dest="dell_dest_dir", default=None,
                        help=f"DELL1TB02 base destination directory for Duplicates (default: {TRIAGE_DELL_DESTINATION_ROOT})")
    parser.add_argument("--triage-id", default=None,
                        help="Custom triage session ID (default: auto-increment integer ID from database)")
    parser.add_argument("--ignore-duplicates", action="store_true",
                        help="Skip copying duplicate files to DELL1TB02 (only replicate Review-Similar and New assets to LaCie). Duplicates are still evaluated and logged in DB.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate the triage process without copying any files or modifying DB.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    triage_photos_library(
        args.library_path,
        lacie_dest_root=args.lacie_dest_dir,
        dell_dest_root=args.dell_dest_dir,
        triage_id=args.triage_id,
        dry_run=args.dry_run,
        ignore_duplicates=args.ignore_duplicates
    )
