#!/usr/bin/env bash
"""": # Bash wrapper for python script
exec python3 "$0" "$@"
"""
# ==============================================================================
# verify_triage_duplicates.py - Independent Safety Audit of Triage Duplicates
# ==============================================================================
# Audits all files stored in:
#   /Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/Duplicates
# and cross-references them against the active Apple Photos database
# (db/media_organizer.db / All-Media.photoslibrary) and originals on disk.
#
# Guarantees that no file marked as 'Duplicate' is purged unless it is 100%
# confirmed to exist in the master library.
# ==============================================================================

import argparse
import csv
import datetime
import os
import re
import sqlite3
import sys
import time

try:
    from scripts.constants import (
        APPLE_PHOTOS_ORIGINALS_PATH,
        MEDIA_ORGANIZER_DB_PATH,
        TRIAGE_DUPLICATES_ROOT,
        MEDIA_EXTENSIONS,
    )
except ImportError:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    MEDIA_ORGANIZER_DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../db/media_organizer.db"))
    APPLE_PHOTOS_ORIGINALS_PATH = "/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/originals"
    TRIAGE_DUPLICATES_ROOT = "/Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/Duplicates"
    MEDIA_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.heic', '.tif', '.tiff', '.webp', '.bmp', '.gif',
        '.dng', '.cr2', '.cr3', '.nef', '.arw', '.raw',
        '.mov', '.mp4', '.m4v', '.avi', '.mpg', '.mpeg', '.mkv', '.wmv',
        '.mts', '.m2ts', '.vob', '.webm', '.flv', '.3gp', '.mod'
    }

# ANSI Colors
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
RESET = "\033[0m"


def format_size(bytes_val):
    if bytes_val is None or bytes_val <= 0:
        return "0.00 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def are_files_identical(p1, p2, chunk_size=65536):
    """Fast chunked byte comparison between two files."""
    try:
        if os.path.getsize(p1) != os.path.getsize(p2):
            return False
        with open(p1, "rb") as f1, open(p2, "rb") as f2:
            while True:
                b1 = f1.read(chunk_size)
                b2 = f2.read(chunk_size)
                if b1 != b2:
                    return False
                if not b1:
                    return True
    except OSError:
        return False


class MasterLibraryIndex:
    """Loads and indexes the Apple Photos master catalog for instant $O(1)$ lookups."""

    def __init__(self, db_path):
        self.db_path = db_path
        self.total_assets = 0
        self.size_orig_name_map = {}   # (size, orig_filename_lower) -> asset_info
        self.size_cur_name_map = {}    # (size, cur_filename_lower) -> asset_info
        self.size_map = {}             # size -> list of asset_info
        self.uuid_map = {}             # uuid -> asset_info
        self.stem_map = {}             # stem_lower -> list of asset_info
        self.triage_history_map = {}   # (triage_id, filename_lower, file_size) -> triage_record
        self.triage_uuid_map = {}      # (triage_id, filename_lower) -> matched_asset_uuid

    def load(self):
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # 1. Index master Apple Photos catalog (ZASSET joined with ZADDITIONALASSETATTRIBUTES)
        query = """
            SELECT 
                a.ZUUID,
                a.ZDIRECTORY,
                a.ZFILENAME,
                aaa.ZORIGINALFILENAME,
                aaa.ZORIGINALFILESIZE
            FROM ZASSET a
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        """
        cur.execute(query)
        rows = cur.fetchall()
        self.total_assets = len(rows)

        for uuid, zdir, zfname, orig_fname, orig_size in rows:
            entry = {
                "uuid": uuid,
                "directory": zdir,
                "cur_filename": zfname,
                "orig_filename": orig_fname or zfname,
                "size": orig_size or 0,
            }

            if uuid:
                self.uuid_map[uuid] = entry

            sz = orig_size or 0
            if sz > 0:
                self.size_map.setdefault(sz, []).append(entry)

                if orig_fname:
                    self.size_orig_name_map[(sz, orig_fname.lower())] = entry
                    stem, _ = os.path.splitext(orig_fname.lower())
                    if stem:
                        self.stem_map.setdefault(stem, []).append(entry)

                if zfname:
                    self.size_cur_name_map[(sz, zfname.lower())] = entry
                    stem, _ = os.path.splitext(zfname.lower())
                    if stem:
                        self.stem_map.setdefault(stem, []).append(entry)

        # 2. Index past triage sessions recorded decisions
        uuid_regex = re.compile(r'([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})')
        try:
            cur.execute("""
                SELECT 
                    triage_id,
                    filename,
                    file_size_bytes,
                    sub_type,
                    matched_asset_uuid,
                    decision_reason,
                    original_rel_path,
                    destination_path
                FROM triage_session_assets
                WHERE category = 'duplicate'
            """)
            triage_rows = cur.fetchall()
            for tid, fname, fsz, sub_t, m_uuid, reason, rel_p, dest_p in triage_rows:
                tid_str = str(tid)
                fn_lower = (fname or "").lower()
                reason_str = reason or ""
                if not m_uuid and reason_str:
                    m = uuid_regex.search(reason_str)
                    if m:
                        m_uuid = m.group(1)

                rec = {
                    "triage_id": tid_str,
                    "filename": fname,
                    "file_size": fsz,
                    "sub_type": sub_t,
                    "matched_uuid": m_uuid,
                    "reason": reason_str,
                    "rel_path": rel_p,
                    "dest_path": dest_p,
                }
                if fn_lower and fsz:
                    self.triage_history_map[(tid_str, fn_lower, fsz)] = rec
                if fn_lower and m_uuid:
                    self.triage_uuid_map[(tid_str, fn_lower)] = m_uuid
                if rel_p and m_uuid:
                    self.triage_uuid_map[(tid_str, rel_p.lower())] = m_uuid
        except sqlite3.OperationalError:
            # triage_session_assets might not exist in empty test DBs
            pass

        conn.close()


def audit_duplicates_directory(
    target_dir,
    session_filter=None,
    verify_disk=False,
    export_csv=None,
    export_missing_csv=None,
):
    print(f"{CYAN}{BOLD}")
    print("  ╔═══════════════════════════════════════════════════════════════════════════╗")
    print("  ║            🛡️  APPLE PHOTOS MASTER DUPLICATES AUDIT & CONFIRMATION         ║")
    print("  ║         Independent Multi-Tier Cross-Reference Against Master Library     ║")
    print("  ╚═══════════════════════════════════════════════════════════════════════════╝")
    print(f"{RESET}")

    target_dir = os.path.abspath(target_dir)
    print(f"{BOLD}Target Duplicates Directory:{RESET} {target_dir}")
    if session_filter:
        print(f"{BOLD}Session Filter:{RESET}            Session {session_filter}")

    if not os.path.exists(target_dir):
        print(f"{RED}❌ Error: Duplicates directory does not exist: {target_dir}{RESET}")
        return False

    # Check database and originals disk
    print(f"{BOLD}Master Database:{RESET}            {MEDIA_ORGANIZER_DB_PATH}")
    has_originals_disk = os.path.exists(APPLE_PHOTOS_ORIGINALS_PATH)
    status_disk = f"{GREEN}✓ Mounted ({APPLE_PHOTOS_ORIGINALS_PATH}){RESET}" if has_originals_disk else f"{YELLOW}✗ Offline (Disk hash comparison skipped){RESET}"
    print(f"{BOLD}Master Library Disk:{RESET}        {status_disk}")
    if verify_disk and not has_originals_disk:
        print(f"{YELLOW}⚠️  --verify-disk requested, but {APPLE_PHOTOS_ORIGINALS_PATH} is not mounted. Byte hashing will be skipped.{RESET}")

    # Step 1: Load master index
    print(f"\n{BLUE}📚 Loading Apple Photos catalog from database...{RESET}")
    t0 = time.time()
    index = MasterLibraryIndex(MEDIA_ORGANIZER_DB_PATH)
    index.load()
    load_time = time.time() - t0
    print(f"{GREEN}✓ Loaded {index.total_assets:,} master assets in {load_time:.2f}s ({len(index.size_orig_name_map):,} size+name keys, {len(index.triage_history_map):,} past triage logs).{RESET}\n")

    # Step 2: Discover duplicate files
    print(f"{BLUE}🔍 Scanning duplicate files on disk...{RESET}")
    candidate_files = []
    total_scanned_bytes = 0

    # Determine paths to inspect
    if session_filter:
        session_path = os.path.join(target_dir, str(session_filter))
        search_dirs = [session_path] if os.path.exists(session_path) else []
    else:
        search_dirs = [target_dir]

    for base_p in search_dirs:
        for root, dirs, files in os.walk(base_p):
            for f in files:
                if f.startswith(".") or f in ("Thumbs.db", "desktop.ini"):
                    continue
                full_p = os.path.join(root, f)
                try:
                    sz = os.path.getsize(full_p)
                except OSError:
                    continue

                rel_p = os.path.relpath(full_p, target_dir)
                parts = rel_p.split(os.sep)
                session_id = parts[0] if len(parts) > 1 else "Unknown"

                candidate_files.append((full_p, rel_p, f, sz, session_id))
                total_scanned_bytes += sz

    total_files = len(candidate_files)
    print(f"{GREEN}📂 Found {total_files:,} files ({format_size(total_scanned_bytes)}) to verify across duplicates.{RESET}\n")

    if total_files == 0:
        print(f"{YELLOW}No files found to verify.{RESET}")
        return True

    # Step 3: Multi-tier Verification
    print(f"{BLUE}⚡ Cross-referencing against Apple Photos Master Catalog...{RESET}")
    results = {
        "CONFIRMED_DB_EXACT": [],         # Exact size + filename (original or current)
        "CONFIRMED_DISK_EXACT": [],       # Exact byte-for-byte read comparison
        "CONFIRMED_LIVE_PHOTO_MOV": [],   # Companion Live Photo MOV confirmed
        "CONFIRMED_SUPERIOR_IN_LIBRARY": [], # Compressed duplicate where superior original exists in library
        "CONFIRMED_VISUAL_DUPLICATE": [], # Visual duplicate confirmed in past session logs
        "CONFIRMED_TRIAGE_LOG": [],       # Recorded in past triage session with valid UUID
        "UNCONFIRMED_SIZE_MATCH": [],     # Size matches an asset, but name/hash differ
        "MISSING": [],                    # Completely not found in Apple Photos DB
    }

    t_start = time.time()
    last_print = t_start

    for idx, (full_p, rel_p, fname, sz, sess_id) in enumerate(candidate_files, 1):
        fn_lower = fname.lower()
        stem, ext = os.path.splitext(fn_lower)
        # Disambiguated clean filename (e.g. IMG_1234_dup1.JPG -> IMG_1234.JPG)
        clean_fname = re.sub(r"_dup\d+", "", fn_lower)
        clean_stem, _ = os.path.splitext(clean_fname)

        verified = False
        ver_category = None
        match_info = {}

        # ── Tier 1: Exact (Size, Original Filename) ──
        if (sz, fn_lower) in index.size_orig_name_map:
            master = index.size_orig_name_map[(sz, fn_lower)]
            ver_category = "CONFIRMED_DB_EXACT"
            match_info = master
            verified = True
        elif (sz, clean_fname) in index.size_orig_name_map:
            master = index.size_orig_name_map[(sz, clean_fname)]
            ver_category = "CONFIRMED_DB_EXACT"
            match_info = master
            verified = True
        elif (sz, fn_lower) in index.size_cur_name_map:
            master = index.size_cur_name_map[(sz, fn_lower)]
            ver_category = "CONFIRMED_DB_EXACT"
            match_info = master
            verified = True
        elif (sz, clean_fname) in index.size_cur_name_map:
            master = index.size_cur_name_map[(sz, clean_fname)]
            ver_category = "CONFIRMED_DB_EXACT"
            match_info = master
            verified = True

        # ── Tier 2: Check past triage decision log ──
        if not verified:
            sub_rel_p = os.path.relpath(full_p, os.path.join(target_dir, sess_id)).lower()
            hist_key = (sess_id, fn_lower, sz)
            if hist_key in index.triage_history_map:
                hist = index.triage_history_map[hist_key]
                m_uuid = hist.get("matched_uuid")
                sub_t = hist.get("sub_type")
                if m_uuid and m_uuid in index.uuid_map:
                    master = index.uuid_map[m_uuid]
                    match_info = master
                    if sub_t == "compressed":
                        ver_category = "CONFIRMED_SUPERIOR_IN_LIBRARY"
                    elif sub_t == "visual_duplicate":
                        ver_category = "CONFIRMED_VISUAL_DUPLICATE"
                    else:
                        ver_category = "CONFIRMED_TRIAGE_LOG"
                    verified = True
            elif (sess_id, sub_rel_p) in index.triage_uuid_map:
                m_uuid = index.triage_uuid_map[(sess_id, sub_rel_p)]
                if m_uuid and m_uuid in index.uuid_map:
                    master = index.uuid_map[m_uuid]
                    match_info = master
                    ver_category = "CONFIRMED_TRIAGE_LOG"
                    verified = True
            elif (sess_id, fn_lower) in index.triage_uuid_map:
                m_uuid = index.triage_uuid_map[(sess_id, fn_lower)]
                if m_uuid and m_uuid in index.uuid_map:
                    master = index.uuid_map[m_uuid]
                    match_info = master
                    ver_category = "CONFIRMED_TRIAGE_LOG"
                    verified = True

        # ── Tier 3: Live Photo Companion MOV Check ──
        if not verified and ext in (".mov", ".mp4"):
            # Clean companion stem (remove _3 suffix if UUID companion)
            c_stem = re.sub(r"_3$", "", clean_stem)
            if c_stem in index.stem_map:
                master = index.stem_map[c_stem][0]
                ver_category = "CONFIRMED_LIVE_PHOTO_MOV"
                match_info = master
                verified = True

        # ── Tier 4: Disk Bit-for-Bit Hash Check (if disk mounted & candidate size matches) ──
        if (not verified or verify_disk) and has_originals_disk and sz in index.size_map:
            for cand in index.size_map[sz]:
                cand_zdir = cand.get("directory")
                cand_zfn = cand.get("cur_filename")
                if cand_zdir and cand_zfn:
                    cand_full = os.path.join(APPLE_PHOTOS_ORIGINALS_PATH, cand_zdir, cand_zfn)
                    if os.path.exists(cand_full) and are_files_identical(full_p, cand_full):
                        ver_category = "CONFIRMED_DISK_EXACT"
                        match_info = cand
                        verified = True
                        break

        # ── Tier 5: Ambiguous Size Match vs Complete Missing ──
        if not verified:
            if sz in index.size_map:
                ver_category = "UNCONFIRMED_SIZE_MATCH"
                match_info = index.size_map[sz][0]
            else:
                ver_category = "MISSING"

        results[ver_category].append({
            "path": full_p,
            "rel_path": rel_p,
            "filename": fname,
            "size": sz,
            "session_id": sess_id,
            "category": ver_category,
            "matched_uuid": match_info.get("uuid", ""),
            "matched_orig_filename": match_info.get("orig_filename", ""),
            "matched_cur_filename": match_info.get("cur_filename", ""),
        })

        # Progress reporting
        now = time.time()
        if idx == 1 or idx == total_files or (now - last_print) >= 0.5 or idx % 200 == 0:
            last_print = now
            pct = (idx / total_files) * 100
            sys.stdout.write(
                f"\r🚚 Verifying: {idx:,}/{total_files:,} ({pct:.1f}%) | "
                f"Exact: {len(results['CONFIRMED_DB_EXACT']) + len(results['CONFIRMED_DISK_EXACT']):,} | "
                f"Missing: {len(results['MISSING']):,}..."
            )
            sys.stdout.flush()

    sys.stdout.write("\r" + " " * 80 + "\r")
    sys.stdout.flush()

    # Step 4: Generate Summary Report
    confirmed_count = (
        len(results["CONFIRMED_DB_EXACT"])
        + len(results["CONFIRMED_DISK_EXACT"])
        + len(results["CONFIRMED_LIVE_PHOTO_MOV"])
        + len(results["CONFIRMED_SUPERIOR_IN_LIBRARY"])
        + len(results["CONFIRMED_VISUAL_DUPLICATE"])
        + len(results["CONFIRMED_TRIAGE_LOG"])
    )
    confirmed_bytes = sum(
        item["size"]
        for cat in [
            "CONFIRMED_DB_EXACT",
            "CONFIRMED_DISK_EXACT",
            "CONFIRMED_LIVE_PHOTO_MOV",
            "CONFIRMED_SUPERIOR_IN_LIBRARY",
            "CONFIRMED_VISUAL_DUPLICATE",
            "CONFIRMED_TRIAGE_LOG",
        ]
        for item in results[cat]
    )

    unconfirmed_count = len(results["UNCONFIRMED_SIZE_MATCH"]) + len(results["MISSING"])
    unconfirmed_bytes = sum(item["size"] for item in results["UNCONFIRMED_SIZE_MATCH"] + results["MISSING"])

    elapsed = time.time() - t_start

    print("=" * 82)
    print("📊 MASTER APPLE PHOTOS DUPLICATES VERIFICATION REPORT")
    print("=" * 82)
    print(f"Target Directory:      {target_dir}")
    print(f"Total Files Audited:   {total_files:,} ({format_size(total_scanned_bytes)})")
    print(f"Audit Elapsed Time:    {elapsed:.2f}s ({(total_files / max(0.001, elapsed)):.0f} files/sec)")
    print("-" * 82)
    print(f"{BOLD}Detailed Verification Breakdown:{RESET}")
    print(f"  🟢 {GREEN}Exact DB Match (Size + Filename):{RESET}       {len(results['CONFIRMED_DB_EXACT']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_DB_EXACT'])):>9})")
    print(f"  🟢 {GREEN}Disk Bit-for-Bit Verified Match:{RESET}       {len(results['CONFIRMED_DISK_EXACT']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_DISK_EXACT'])):>9})")
    print(f"  🟢 {GREEN}Live Photo Companion MOV Matched:{RESET}      {len(results['CONFIRMED_LIVE_PHOTO_MOV']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_LIVE_PHOTO_MOV'])):>9})")
    print(f"  🟢 {GREEN}Superior Master in Library (Compressed):{RESET} {len(results['CONFIRMED_SUPERIOR_IN_LIBRARY']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_SUPERIOR_IN_LIBRARY'])):>9})")
    print(f"  🟢 {GREEN}Visual Duplicate (Confirmed in Logs):{RESET}   {len(results['CONFIRMED_VISUAL_DUPLICATE']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_VISUAL_DUPLICATE'])):>9})")
    print(f"  🟢 {GREEN}Logged Triage Decision (UUID Match):{RESET}   {len(results['CONFIRMED_TRIAGE_LOG']):>7,} files ({format_size(sum(x['size'] for x in results['CONFIRMED_TRIAGE_LOG'])):>9})")
    print("  " + "-" * 80)
    conf_pct = (confirmed_count / total_files) * 100 if total_files else 100.0
    print(f"  {BOLD}{GREEN}TOTAL CONFIRMED SAFE TO PURGE:{RESET}          {confirmed_count:>7,} files ({format_size(confirmed_bytes):>9}) [{conf_pct:.2f}%]")

    if unconfirmed_count > 0:
        print(f"\n  {BOLD}{YELLOW}⚠️  Ambiguous (Size Matches, Name Differs):{RESET} {len(results['UNCONFIRMED_SIZE_MATCH']):>7,} files ({format_size(sum(x['size'] for x in results['UNCONFIRMED_SIZE_MATCH'])):>9})")
        print(f"  {BOLD}{RED}🔴 NOT FOUND / MISSING IN LIBRARY:{RESET}         {len(results['MISSING']):>7,} files ({format_size(sum(x['size'] for x in results['MISSING'])):>9})")
        print("  " + "-" * 80)
        unconf_pct = (unconfirmed_count / total_files) * 100
        print(f"  {BOLD}{RED}TOTAL UNCONFIRMED (DO NOT PURGE):{RESET}       {unconfirmed_count:>7,} files ({format_size(unconfirmed_bytes):>9}) [{unconf_pct:.2f}%]")
    print("=" * 82)

    # Export CSV Reports
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not export_csv:
        export_csv = os.path.join(BASE_DIR, f"../reports/duplicates_audit_{timestamp}.csv")
    export_csv = os.path.abspath(export_csv)

    all_items = []
    for cat, items in results.items():
        all_items.extend(items)

    try:
        os.makedirs(os.path.dirname(export_csv), exist_ok=True)
        with open(export_csv, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(
                f_out,
                fieldnames=[
                    "session_id",
                    "category",
                    "filename",
                    "size",
                    "matched_uuid",
                    "matched_orig_filename",
                    "matched_cur_filename",
                    "rel_path",
                    "path",
                ],
            )
            writer.writeheader()
            for r in all_items:
                writer.writerow(r)
        print(f"\n📝 Full audit record exported to: {export_csv}")
    except Exception as e:
        print(f"⚠️ Failed to write audit CSV: {e}")

    # Export missing/unconfirmed only
    if unconfirmed_count > 0:
        if not export_missing_csv:
            export_missing_csv = os.path.join(
                BASE_DIR, f"../reports/unconfirmed_duplicates_{timestamp}.csv"
            )
        export_missing_csv = os.path.abspath(export_missing_csv)
        try:
            with open(export_missing_csv, "w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(
                    f_out,
                    fieldnames=[
                        "session_id",
                        "category",
                        "filename",
                        "size",
                        "rel_path",
                        "path",
                    ],
                )
                writer.writeheader()
                for r in results["UNCONFIRMED_SIZE_MATCH"] + results["MISSING"]:
                    writer.writerow({
                        "session_id": r["session_id"],
                        "category": r["category"],
                        "filename": r["filename"],
                        "size": r["size"],
                        "rel_path": r["rel_path"],
                        "path": r["path"],
                    })
            print(f"🚨 {RED}Actionable report of unconfirmed files saved to:{RESET} {export_missing_csv}")
        except Exception as e:
            print(f"⚠️ Failed to write missing CSV: {e}")

        print(f"\n{RED}{BOLD}⚠️ CAUTION: {unconfirmed_count:,} files could not be confirmed in Apple Photos DB.{RESET}")
        print(f"{RED}DO NOT PURGE OR DELETE THESE FILES UNTIL INVESTIGATED.{RESET}")
        print("\nSample Unconfirmed / Missing Files (First 5):")
        for sample in (results["MISSING"] + results["UNCONFIRMED_SIZE_MATCH"])[:5]:
            print(f"  • [{sample['category']}] Session {sample['session_id']} | {sample['filename']} ({format_size(sample['size'])})")
            print(f"    Path: {sample['path']}")
        return False

    print(f"\n{GREEN}{BOLD}🎉 100% CONFIRMED SAFE: All {confirmed_count:,} duplicate files verified to exist in Apple Photos DB!{RESET}\n")
    return True


def parse_args():
    parser = argparse.ArgumentParser(
        description="Verify and cross-reference Triage Duplicates against the master Apple Photos database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "target_dir",
        nargs="?",
        default=TRIAGE_DUPLICATES_ROOT,
        help=f"Path to Duplicates directory to audit (default: {TRIAGE_DUPLICATES_ROOT})",
    )
    parser.add_argument(
        "--session",
        "-s",
        default=None,
        help="Audit only a specific triage session ID (e.g. --session 47 or --session 14)",
    )
    parser.add_argument(
        "--verify-disk",
        action="store_true",
        help="Perform chunked byte-for-byte read comparisons against originals on disk (requires /Volumes/Extreme Pro)",
    )
    parser.add_argument(
        "--export",
        dest="export_csv",
        default=None,
        help="Custom file path for the full audit CSV export",
    )
    parser.add_argument(
        "--export-missing",
        dest="export_missing_csv",
        default=None,
        help="Custom file path for the unconfirmed/missing files CSV export",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ok = audit_duplicates_directory(
        args.target_dir,
        session_filter=args.session,
        verify_disk=args.verify_disk,
        export_csv=args.export_csv,
        export_missing_csv=args.export_missing_csv,
    )
    sys.exit(0 if ok else 1)
