#!/usr/bin/env python3
"""
report_triage_isolated_assets.py

Analyzes non-duplicate triage assets in 'New' and 'Review-Similar' folders
under /Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake.

For each asset, it searches the master library catalog (All-Media.photoslibrary)
for the closest preceding and succeeding assets by creation date, calculating
the temporal isolation distance (gap).

Assets are stored in a temporary SQLite table and ranked with the furthest
temporal distance bubbling up to the top, highlighting assets that are most
likely unique, novel, and have zero proximate representation in the master library.

Usage:
  python3 scripts/report_triage_isolated_assets.py
  python3 scripts/report_triage_isolated_assets.py --top 50
  python3 scripts/report_triage_isolated_assets.py --category new
  python3 scripts/report_triage_isolated_assets.py --category review_similar
  python3 scripts/report_triage_isolated_assets.py --min-gap-days 30
  python3 scripts/report_triage_isolated_assets.py --session 41
  python3 scripts/report_triage_isolated_assets.py --markdown
  python3 scripts/report_triage_isolated_assets.py --csv triage_isolated_assets.csv
"""

import os
import sys
import re
import bisect
import sqlite3
import argparse
import csv
from datetime import datetime

# Optional EXIF reader
try:
    import exifread
    HAS_EXIFREAD = True
except ImportError:
    HAS_EXIFREAD = False

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LACIE_DESTINATION_ROOT
)


def format_size(bytes_val):
    if not bytes_val or bytes_val <= 0:
        return "0.0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def format_days_gap(days):
    if days is None or days >= 999990:
        return "∞ (Endpoint)"
    if days >= 365.25:
        years = days / 365.25
        return f"{years:.1f} years ({days:.0f}d)"
    elif days >= 30.0:
        months = days / 30.44
        return f"{months:.1f} mo ({days:.0f}d)"
    elif days >= 1.0:
        return f"{days:.1f} days"
    else:
        hours = days * 24.0
        if hours >= 1.0:
            return f"{hours:.1f} hours"
        mins = hours * 60.0
        return f"{mins:.0f} mins"


def extract_creation_date(filename, file_path=None):
    """
    Extracts the most accurate capture or creation datetime for an asset:
    1. Timestamps in filename or path (YYYYMMDD_HHMMSS or YYYY-MM-DD)
    2. File system st_birthtime or st_mtime
    3. EXIF DateTimeOriginal as fallback
    """
    # 1. Regex search for timestamp in filename or path
    targets = [filename]
    if file_path:
        targets.append(file_path)

    for target in targets:
        m = re.search(r'(20\d{2}|19\d{2})[-_]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12]\d|3[01])[-_T ](\d{2})[-_:]?(\d{2})[-_:]?(\d{2})', target)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                                int(m.group(4)), int(m.group(5)), int(m.group(6)))
            except ValueError:
                pass

        m2 = re.search(r'(20\d{2}|19\d{2})[-_]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12]\d|3[01])', target)
        if m2:
            try:
                return datetime(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)), 12, 0, 0)
            except ValueError:
                pass

    # 2. Filesystem birthtime / mtime (instantaneous on APFS/HFS+)
    if file_path and os.path.isfile(file_path):
        try:
            st = os.stat(file_path)
            if hasattr(st, 'st_birthtime') and st.st_birthtime > 0:
                dt_birth = datetime.fromtimestamp(st.st_birthtime)
                if 1980 <= dt_birth.year <= 2035:
                    return dt_birth
            if st.st_mtime > 0:
                dt_mtime = datetime.fromtimestamp(st.st_mtime)
                if 1980 <= dt_mtime.year <= 2035:
                    return dt_mtime
        except Exception:
            pass

    # 3. EXIF reader as last resort
    if file_path and os.path.isfile(file_path) and HAS_EXIFREAD:
        ext = os.path.splitext(file_path)[1].lower()
        if ext in ('.jpg', '.jpeg', '.tif', '.tiff'):
            try:
                with open(file_path, 'rb') as f:
                    tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)
                    tag = tags.get('EXIF DateTimeOriginal') or tags.get('Image DateTime')
                    if tag:
                        val = str(tag).strip()
                        if len(val) >= 19:
                            return datetime.strptime(val[:19], '%Y:%m:%d %H:%M:%S')
            except Exception:
                pass

    return None


def run_temporal_isolation_analysis(
    db_path=MEDIA_ORGANIZER_DB_PATH,
    triage_root=TRIAGE_LACIE_DESTINATION_ROOT,
    category_filter="all",
    session_filter=None,
    top_n=30,
    min_gap_days=0.0,
    exclude_ignored=True,
    output_format="text",
    csv_path=None,
    save_db_table=None
):
    if not os.path.isfile(db_path):
        print(f"❌ Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    # 1. Connect to database
    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=30.0)
    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout = 30000")

    sys.stdout.write("📚 Loading Master Library timeline from database...\n")
    sys.stdout.flush()

    # Load master timeline (All-Media assets with valid date_created_utc)
    cur.execute("""
        SELECT date_created_utc, asset_id, original_filename
        FROM assets
        WHERE date_created_utc IS NOT NULL
          AND date_created_utc >= '1970-01-01'
          AND date_created_utc <= '2035-01-01'
        ORDER BY date_created_utc ASC
    """)
    timeline = []
    for dt_str, aid, fname in cur.fetchall():
        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            timeline.append((dt.timestamp(), dt_str, aid, fname))
        except ValueError:
            pass

    if not timeline:
        print("❌ No valid dated assets found in master catalog.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    ts_list = [item[0] for item in timeline]
    print(f"✅ Loaded {len(timeline):,} dated assets from All-Media catalog into timeline.")

    # 2. Fetch non-duplicate triage assets
    query_conditions = ["tsa.category IN ('new', 'review_similar')"]
    params = []

    if category_filter == "new":
        query_conditions = ["tsa.category = 'new'"]
    elif category_filter in ("review_similar", "review"):
        query_conditions = ["tsa.category = 'review_similar'"]

    if session_filter:
        query_conditions.append("tsa.triage_id = ?")
        params.append(str(session_filter))

    where_clause = " AND ".join(query_conditions)
    cur.execute(f"""
        SELECT 
            tsa.id, 
            tsa.triage_id, 
            tsa.filename, 
            tsa.category, 
            tsa.sub_type, 
            tsa.file_size_bytes, 
            tsa.destination_path, 
            tsa.decision_reason,
            ts.source_directory,
            tsa.original_rel_path
        FROM triage_session_assets tsa
        LEFT JOIN triage_sessions ts ON ts.triage_id = tsa.triage_id OR ts.id = tsa.triage_session_id
        WHERE {where_clause}
    """, params)
    triage_rows = cur.fetchall()

    # Load ignored folders if table exists
    ignored_folders = set()
    if exclude_ignored:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='triage_folder_decisions'")
        if cur.fetchone():
            cur.execute("SELECT folder_key FROM triage_folder_decisions WHERE decision = 'ignored'")
            ignored_folders = {r[0] for r in cur.fetchall()}
            if ignored_folders:
                print(f"🛡️  Excluding assets from {len(ignored_folders):,} folders marked as 'ignored'.")

    conn.close()

    print(f"📂 Analyzing {len(triage_rows):,} triage assets across 'New' and 'Review-Similar' categories...")

    # 3. Create In-Memory SQLite Database with Temporary Table
    mem_conn = sqlite3.connect(":memory:")
    mem_cur = mem_conn.cursor()

    mem_cur.execute("""
        CREATE TABLE temp_triage_isolated_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            triage_id TEXT,
            category TEXT,
            sub_type TEXT,
            filename TEXT,
            source_folder TEXT,
            file_path TEXT,
            file_size_bytes INTEGER,
            date_created TEXT,
            closest_before_date TEXT,
            closest_before_name TEXT,
            closest_before_uuid TEXT,
            gap_before_days REAL,
            closest_after_date TEXT,
            closest_after_name TEXT,
            closest_after_uuid TEXT,
            gap_after_days REAL,
            min_gap_days REAL,
            min_gap_display TEXT
        )
    """)

    # Process and populate temp table
    processed = 0
    skipped_no_date = 0
    skipped_ignored = 0

    insert_batch = []
    for row in triage_rows:
        row_id, tid, fname, cat, sub_t, fsz, dest_p, reason, src_dir, orig_rel = row
        p = dest_p
        # If destination path doesn't point to actual file, check standard triage root
        if not p or not os.path.isfile(p):
            alt_cat = "New" if cat == "new" else "Review-Similar"
            p_cand = os.path.join(triage_root, alt_cat, str(tid), fname)
            if os.path.isfile(p_cand):
                p = p_cand

        # Construct partial source folder representation
        src_base = os.path.basename(src_dir.rstrip('/')) if src_dir else ''
        rel_dir = os.path.dirname(orig_rel) if orig_rel else ''
        if src_base and rel_dir:
            src_partial = f"{src_base}/{rel_dir}"
        elif src_base:
            src_partial = src_base
        elif rel_dir:
            src_partial = rel_dir
        else:
            src_partial = "Triage Intake"

        if exclude_ignored and src_partial in ignored_folders:
            skipped_ignored += 1
            continue

        dt = extract_creation_date(fname, p)
        if not dt:
            skipped_no_date += 1
            continue

        target_ts = dt.timestamp()
        idx = bisect.bisect_left(ts_list, target_ts)

        before = timeline[idx - 1] if idx > 0 else None
        after = timeline[idx] if idx < len(timeline) else None

        gap_bef = (target_ts - before[0]) / 86400.0 if before else 999999.0
        gap_aft = (after[0] - target_ts) / 86400.0 if after else 999999.0
        min_gap = min(gap_bef, gap_aft)

        if min_gap < min_gap_days:
            continue

        dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        gap_display = format_days_gap(min_gap)

        insert_batch.append((
            tid, cat, sub_t or 'clean_media', fname, src_partial, p or '', fsz or 0, dt_str,
            before[1] if before else None,
            before[3] if before else None,
            before[2] if before else None,
            round(gap_bef, 2),
            after[1] if after else None,
            after[3] if after else None,
            after[2] if after else None,
            round(gap_aft, 2),
            round(min_gap, 2),
            gap_display
        ))
        processed += 1

    mem_cur.executemany("""
        INSERT INTO temp_triage_isolated_assets (
            triage_id, category, sub_type, filename, source_folder, file_path, file_size_bytes, date_created,
            closest_before_date, closest_before_name, closest_before_uuid, gap_before_days,
            closest_after_date, closest_after_name, closest_after_uuid, gap_after_days,
            min_gap_days, min_gap_display
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, insert_batch)
    mem_conn.commit()

    print(f"✨ Populated temporary table with {processed:,} evaluated assets (ordered by temporal distance).")

    # 4. Query Summary Statistics from temp table
    mem_cur.execute("""
        SELECT 
            COUNT(*),
            SUM(CASE WHEN min_gap_days >= 30.0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN min_gap_days >= 7.0 AND min_gap_days < 30.0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN min_gap_days >= 1.0 AND min_gap_days < 7.0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN min_gap_days < 1.0 THEN 1 ELSE 0 END),
            MAX(min_gap_days),
            AVG(min_gap_days)
        FROM temp_triage_isolated_assets
    """)
    sum_row = mem_cur.fetchone()
    total_matches, g_30, g_7_30, g_1_7, g_under_1, max_gap, avg_gap = sum_row

    # 5. Query Bubbled Up Top Assets
    mem_cur.execute(f"""
        SELECT 
            triage_id, category, sub_type, filename, source_folder, file_size_bytes, date_created,
            closest_before_date, closest_before_name, gap_before_days,
            closest_after_date, closest_after_name, gap_after_days,
            min_gap_days, min_gap_display
        FROM temp_triage_isolated_assets
        ORDER BY min_gap_days DESC, file_size_bytes DESC
        LIMIT {top_n}
    """)
    top_results = mem_cur.fetchall()

    # Optional CSV Export
    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow([
                "Triage ID", "Category", "Subtype", "Filename", "Source Folder", "File Size Bytes", "Date Created",
                "Closest Before Date", "Closest Before Name", "Gap Before Days",
                "Closest After Date", "Closest After Name", "Gap After Days",
                "Min Gap Days", "Min Gap Formatted"
            ])
            mem_cur.execute("""
                SELECT 
                    triage_id, category, sub_type, filename, source_folder, file_size_bytes, date_created,
                    closest_before_date, closest_before_name, gap_before_days,
                    closest_after_date, closest_after_name, gap_after_days,
                    min_gap_days, min_gap_display
                FROM temp_triage_isolated_assets
                ORDER BY min_gap_days DESC
            """)
            for r in mem_cur.fetchall():
                writer.writerow(r)
        print(f"📁 Full temporal isolation ranking exported to CSV: {csv_path}")

    # Optional Persistent Table Export
    if save_db_table:
        write_conn = sqlite3.connect(db_path, timeout=30.0)
        write_cur = write_conn.cursor()
        write_cur.execute(f"DROP TABLE IF EXISTS {save_db_table}")
        write_cur.execute(f"""
            CREATE TABLE {save_db_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                triage_id TEXT,
                category TEXT,
                sub_type TEXT,
                filename TEXT,
                source_folder TEXT,
                file_path TEXT,
                file_size_bytes INTEGER,
                date_created TEXT,
                closest_before_date TEXT,
                closest_before_name TEXT,
                closest_before_uuid TEXT,
                gap_before_days REAL,
                closest_after_date TEXT,
                closest_after_name TEXT,
                closest_after_uuid TEXT,
                gap_after_days REAL,
                min_gap_days REAL,
                min_gap_display TEXT
            )
        """)
        mem_cur.execute("SELECT * FROM temp_triage_isolated_assets")
        all_temp = mem_cur.fetchall()
        write_cur.executemany(f"""
            INSERT INTO {save_db_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, all_temp)
        write_conn.commit()
        write_conn.close()
        print(f"💾 Saved {len(all_temp):,} records into permanent database table: '{save_db_table}' in {db_path}")

    def truncate_middle(text, max_len=45):
        if not text or len(text) <= max_len:
            return text
        part = (max_len - 3) // 2
        return f"{text[:part]}...{text[-part:]}"

    # 6. Render Output
    if output_format == "markdown":
        print(f"\n### ⏳ Triage Intake Temporal Isolation & Uniqueness Report")
        print(f"**Location Analyzed**: `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/[New | Review-Similar]`")
        print(f"**Total Intake Assets Evaluated**: {processed:,}  ")
        print(f"**Filter**: Category: `{category_filter}` | Min Gap: `{min_gap_days}d`\n")

        print("#### 📊 Temporal Isolation Distribution")
        print(f"- 🚀 **Ultra-Isolated (> 30 days gap)**: **{g_30 or 0:,}** assets *(Completely distinct events/shoots with zero library coverage)*")
        print(f"- 🏖️ **Moderately Isolated (7 to 30 days)**: **{g_7_30 or 0:,}** assets")
        print(f"- 📍 **Clustered (1 to 7 days)**: **{g_1_7 or 0:,}** assets")
        print(f"- ⏱️ **Tight Neighbors (< 1 day)**: **{g_under_1 or 0:,}** assets *(Likely shot alongside existing library media)*\n")

        print(f"#### 🌟 Top {len(top_results)} Most Temporally Isolated Assets (Bubbled Up)")
        print("| # | Min Gap | Triage | Category | Asset Filename | Source Folder (Partial) | Date Created | Size | Closest Preceding Library Asset | Closest Succeeding Library Asset |")
        print("| :-: | :---: | :-: | :---: | :--- | :--- | :---: | :---: | :--- | :--- |")
        for idx, r in enumerate(top_results, 1):
            tid, cat, sub, fn, src_f, fsz, dt_c, b_dt, b_fn, b_gap, a_dt, a_fn, a_gap, m_gap, m_disp = r
            b_desc = f"{b_dt[:10]} (`{b_fn}` -{b_gap:.1f}d)" if b_dt else "None (Start)"
            a_desc = f"{a_dt[:10]} (`{a_fn}` +{a_gap:.1f}d)" if a_dt else "None (End)"
            disp_src = truncate_middle(src_f, 40)
            print(f"| {idx} | **{m_disp}** | {tid} | `{cat}` | `{fn}` | `{disp_src}` | {dt_c[:16]} | {format_size(fsz)} | {b_desc} | {a_desc} |")
    else:
        print("\n" + "=" * 100)
        print("⏳  TRIAGE INTAKE TEMPORAL ISOLATION & UNIQUENESS REPORT")
        print("=" * 100)
        print(f"Target Directory:      {triage_root}/[New | Review-Similar]")
        print(f"Total Assets Ranked:   {processed:,} (evaluated against 212,059 master library dates)")
        print("-" * 100)
        print("📊 Temporal Isolation Distribution:")
        print(f"   • 🚀 Ultra-Isolated (> 30 days gap):      {g_30 or 0:>6,} assets (Zero nearby media in All-Media)")
        print(f"   • 🏖️  Moderately Isolated (7 - 30 days):   {g_7_30 or 0:>6,} assets")
        print(f"   • 📍 Clustered (1 - 7 days gap):          {g_1_7 or 0:>6,} assets")
        print(f"   • ⏱️  Tight Neighbors (< 1 day):           {g_under_1 or 0:>6,} assets")
        print(f"   • 🌟 Maximum Isolation Gap:               {format_days_gap(max_gap)}")
        print(f"   • 📈 Average Nearest Neighbor Distance:   {avg_gap or 0:.1f} days")
        print("=" * 100)

        print(f"\n🌟 Top {len(top_results)} Most Temporally Isolated Assets (Bubbled Up to Top):")
        print("-" * 100)
        for idx, r in enumerate(top_results, 1):
            tid, cat, sub, fn, src_f, fsz, dt_c, b_dt, b_fn, b_gap, a_dt, a_fn, a_gap, m_gap, m_disp = r
            b_desc = f"{b_dt[:10]} ({b_fn}) [-{b_gap:.1f}d]" if b_dt else "None (Beginning of library)"
            a_desc = f"{a_dt[:10]} ({a_fn}) [+{a_gap:.1f}d]" if a_dt else "None (End of library)"
            badge = "🟢 NEW" if cat == "new" else "🟡 REVIEW"
            print(f"{idx:>2}. [{m_disp:<17}] {badge} [Triage {tid}] {fn:<26} ({format_size(fsz)}) | Captured: {dt_c}")
            print(f"    📂 Source Folder:  {src_f}")
            print(f"    ⬅️  Closest Before: {b_desc}")
            print(f"    ➡️  Closest After:  {a_desc}")
        print("=" * 100 + "\n")

    mem_conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Temporal Isolation and Uniqueness Reporting Tool for Triage Intake Assets"
    )
    parser.add_argument(
        "--db",
        default=MEDIA_ORGANIZER_DB_PATH,
        help=f"Path to SQLite database (default: {MEDIA_ORGANIZER_DB_PATH})"
    )
    parser.add_argument(
        "--triage-root",
        default=TRIAGE_LACIE_DESTINATION_ROOT,
        help=f"Path to TriageMediaIntake directory (default: {TRIAGE_LACIE_DESTINATION_ROOT})"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=30,
        metavar="N",
        help="Number of top most isolated assets to display (default: 30)"
    )
    parser.add_argument(
        "--category",
        choices=["all", "new", "review_similar", "review"],
        default="all",
        help="Filter assets by category: 'all', 'new', or 'review_similar' (default: all)"
    )
    parser.add_argument(
        "--min-gap-days",
        type=float,
        default=0.0,
        metavar="DAYS",
        help="Filter to assets with at least DAYS of temporal isolation from any library asset (default: 0)"
    )
    parser.add_argument(
        "--session",
        help="Limit analysis to a specific triage session ID (e.g. 41 or 14)"
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Output report formatted as GitHub Flavored Markdown"
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Export full ranked table to specified CSV path"
    )
    parser.add_argument(
        "--save-db-table",
        metavar="TABLE_NAME",
        help="Persist the analyzed isolation table into SQLite database (e.g. triage_isolated_assets)"
    )
    parser.add_argument(
        "--include-ignored",
        action="store_true",
        help="Include assets from folders marked as 'ignored' in triage_folder_decisions (default: excluded)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fmt = "markdown" if args.markdown else "text"
    run_temporal_isolation_analysis(
        db_path=args.db,
        triage_root=args.triage_root,
        category_filter=args.category,
        session_filter=args.session,
        top_n=args.top,
        min_gap_days=args.min_gap_days,
        exclude_ignored=not args.include_ignored,
        output_format=fmt,
        csv_path=args.csv_path,
        save_db_table=args.save_db_table
    )
