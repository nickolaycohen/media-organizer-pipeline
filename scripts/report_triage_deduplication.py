#!/usr/bin/env python3
"""
report_triage_deduplication.py

Generates deduplication and intake performance reports from media_organizer.db
across all triage sessions (triage_media_intake.py and triage_photos_library.py).

Outputs:
  - Cumulative intake metrics (files scanned, duplicates filtered, review items, novel assets)
  - Cumulative duplicate storage eliminated/diverted (GB saved)
  - Per-session breakdown table with source directory, status, and classification counts
  - Optional deep-dive inspection into specific triage sessions (--session <id>)

Usage:
  python3 scripts/report_triage_deduplication.py
  python3 scripts/report_triage_deduplication.py --markdown
  python3 scripts/report_triage_deduplication.py --session 41
  python3 scripts/report_triage_deduplication.py --csv report_triage_sessions.csv
"""

import os
import sys
import sqlite3
import argparse
import csv

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import MEDIA_ORGANIZER_DB_PATH


def format_size(bytes_val):
    if not bytes_val or bytes_val <= 0:
        return "0.0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def generate_triage_report(db_path=MEDIA_ORGANIZER_DB_PATH, session_id=None, output_format="text", csv_path=None, limit=None):
    if not os.path.isfile(db_path):
        print(f"❌ Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=30.0)
    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout = 30000")

    # If drill-down on a specific session is requested
    if session_id:
        generate_session_detail(cur, session_id, output_format)
        conn.close()
        return

    # Cumulative summary query
    cur.execute("""
        SELECT 
            status,
            COUNT(*) AS total_sessions,
            SUM(total_files_scanned) AS total_files,
            SUM(duplicates_count) AS total_dups,
            SUM(duplicates_bytes) AS total_dup_bytes,
            SUM(review_similar_count) AS total_review,
            SUM(new_count) AS total_new
        FROM triage_sessions
        GROUP BY status
    """)
    summary_by_status = {r[0]: r[1:] for r in cur.fetchall()}

    completed_stats = summary_by_status.get("completed", (0, 0, 0, 0, 0, 0))
    comp_sessions, total_files, total_dups, total_dup_bytes, total_review, total_new = completed_stats
    total_files = total_files or 0
    total_dups = total_dups or 0
    total_dup_bytes = total_dup_bytes or 0
    total_review = total_review or 0
    total_new = total_new or 0

    dup_pct = (total_dups / total_files * 100) if total_files else 0.0
    review_pct = (total_review / total_files * 100) if total_files else 0.0
    new_pct = (total_new / total_files * 100) if total_files else 0.0

    # Query individual completed sessions
    limit_clause = f"LIMIT {limit}" if limit else ""
    cur.execute(f"""
        SELECT 
            id, triage_id, source_directory, status, 
            total_files_scanned, duplicates_count, duplicates_bytes,
            review_similar_count, new_count, started_at_utc, completed_at_utc
        FROM triage_sessions
        WHERE status = 'completed' AND total_files_scanned > 0
        ORDER BY id ASC
        {limit_clause}
    """)
    sessions = cur.fetchall()

    # CSV Export if requested
    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow([
                "Session ID", "Source Directory", "Status", "Scanned Files",
                "Duplicates Count", "Duplicates Bytes", "Duplicates Size Formatted",
                "Review Count", "New Count", "Started UTC", "Completed UTC"
            ])
            for s in sessions:
                writer.writerow([
                    s[1], s[2], s[3], s[4], s[5], s[6], format_size(s[6]), s[7], s[8], s[9], s[10]
                ])
        print(f"📁 Triage sessions report saved to CSV: {csv_path}")

    # Output text or markdown
    if output_format == "markdown":
        print("### 🛡️ Intake Triage & Deduplication Cumulative Report")
        print(f"**Database**: `{db_path}`  ")
        print(f"**Completed Triage Sessions**: {comp_sessions:,}\n")
        print("| Classification Metric | Asset Count | Storage Volume | % of Scanned Intake | Destination / Action |")
        print("| :--- | :---: | :---: | :---: | :--- |")
        print(f"| **Confirmed Duplicates** | **{total_dups:,}** | **{format_size(total_dup_bytes)}** | **{dup_pct:.1f}%** | Diverted/Skipped (DELL1TB02) |")
        print(f"| **Review-Similar** *(Upgrades/Bursts)* | **{total_review:,}** | — | **{review_pct:.1f}%** | Routed to LaCie `Review-Similar/` |")
        print(f"| **High-Confidence New Media** | **{total_new:,}** | — | **{new_pct:.1f}%** | Routed to LaCie `New/` (Ready for master import) |")
        print(f"| **TOTAL INTAKE EVALUATED** | **{total_files:,}** | **{format_size(total_dup_bytes)}** | **100.0%** | **{comp_sessions:,} Sessions Processed** |\n")

        print("#### 📋 Completed Triage Sessions Breakdown")
        print("| ID | Source Description | Scanned | Duplicates | Dup % | Review | New | Date (UTC) |")
        print("| :-: | :--- | :---: | :---: | :---: | :---: | :---: | :--- |")
        for s in sessions:
            t_id = s[1]
            src = s[2]
            # Truncate source path if too long
            src_short = os.path.basename(src.rstrip("/")) if len(src) > 40 else src
            scanned = s[4]
            dups = s[5]
            pct = (dups / scanned * 100) if scanned else 0.0
            rev = s[7]
            n_val = s[8]
            dt = s[9].split()[0] if s[9] else ""
            print(f"| **{t_id}** | `{src_short}` | {scanned:,} | {dups:,} | {pct:.1f}% | {rev:,} | {n_val:,} | {dt} |")
    else:
        print("=" * 86)
        print("🛡️  INTAKE TRIAGE & DEDUPLICATION REPORT")
        print("=" * 86)
        print(f"Database:               {db_path}")
        print(f"Completed Sessions:     {comp_sessions:,}")
        print(f"Total Scanned Files:    {total_files:,}")
        print("-" * 86)
        print(f"1. Confirmed Duplicates Diverted:    {total_dups:>8,} assets ({format_size(total_dup_bytes):>10}) [{dup_pct:>5.1f}%]")
        print(f"2. Review-Similar Routed to LaCie:   {total_review:>8,} assets              [{review_pct:>5.1f}%]")
        print(f"3. High-Confidence New Media:        {total_new:>8,} assets              [{new_pct:>5.1f}%]")
        print("-" * 86)
        print(f"⚡ OVERALL DEDUPLICATION RATE:       {dup_pct:>5.1f}% of intake files eliminated as duplicates")
        print(f"💾 TOTAL REDUNDANT STORAGE SAVED:    {format_size(total_dup_bytes)}")
        print("=" * 86)

        print("\n📋 Individual Triage Sessions Breakdown:")
        print("-" * 86)
        print(f"{'ID':<4} | {'Scanned':>7} | {'Duplicates':>10} | {'Dup %':>6} | {'Review':>6} | {'New':>6} | {'Source / Date'}")
        print("-" * 86)
        for s in sessions:
            t_id = s[1]
            src = s[2]
            src_short = os.path.basename(src.rstrip("/")) or src
            scanned = s[4]
            dups = s[5]
            pct = (dups / scanned * 100) if scanned else 0.0
            rev = s[7]
            n_val = s[8]
            dt = s[9].split()[0] if s[9] else ""
            print(f"{t_id:<4} | {scanned:>7,} | {dups:>10,} | {pct:>5.1f}% | {rev:>6,} | {n_val:>6,} | {src_short} ({dt})")
        print("-" * 86 + "\n")

    conn.close()


def generate_session_detail(cur, session_id, output_format="text"):
    cur.execute("""
        SELECT 
            id, triage_id, source_directory, destination_directory, status,
            total_files_scanned, duplicates_count, duplicates_bytes,
            review_similar_count, new_count, started_at_utc, completed_at_utc
        FROM triage_sessions
        WHERE triage_id = ? OR id = ?
    """, (session_id, session_id))
    session = cur.fetchone()

    if not session:
        print(f"❌ Session '{session_id}' not found in database.", file=sys.stderr)
        return

    s_id, t_id, src, dest, status, scanned, dups, dup_bytes, review, new_cnt, started, completed = session

    # Query subtype breakdown
    cur.execute("""
        SELECT category, sub_type, COUNT(*), SUM(file_size_bytes)
        FROM triage_session_assets
        WHERE triage_id = ?
        GROUP BY category, sub_type
        ORDER BY category, COUNT(*) DESC
    """, (t_id,))
    subtypes = cur.fetchall()

    # Query sample novel and review assets
    cur.execute("""
        SELECT filename, category, sub_type, file_size_bytes, decision_reason, matched_asset_uuid
        FROM triage_session_assets
        WHERE triage_id = ? AND category IN ('review_similar', 'new')
        ORDER BY category DESC, file_size_bytes DESC
        LIMIT 10
    """, (t_id,))
    interesting_samples = cur.fetchall()

    if output_format == "markdown":
        print(f"### 🔍 Triage Session Detail: ID {t_id}")
        print(f"- **Source**: `{src}`")
        print(f"- **Destination**: `{dest}`")
        print(f"- **Status**: `{status}`")
        print(f"- **Total Scanned**: {scanned:,}")
        print(f"- **Duplicates**: {dups:,} ({format_size(dup_bytes)})")
        print(f"- **Review-Similar**: {review:,}")
        print(f"- **New Media**: {new_cnt:,}")
        print(f"- **Time**: {started} to {completed}\n")

        print("#### Classification Subtype Breakdown")
        print("| Category | Subtype | Asset Count | Storage Size |")
        print("| :--- | :--- | :---: | :---: |")
        for cat, sub, cnt, sz in subtypes:
            print(f"| **{cat}** | `{sub}` | {cnt:,} | {format_size(sz)} |")

        if interesting_samples:
            print("\n#### Top Novel / Review Assets Sample")
            print("| File | Category | Subtype | Size | Match Reason / Counterpart |")
            print("| :--- | :--- | :--- | :---: | :--- |")
            for fn, cat, sub, sz, reason, counterpart in interesting_samples:
                print(f"| `{fn}` | {cat} | {sub} | {format_size(sz)} | {reason} |")
    else:
        print("=" * 86)
        print(f"🔍  TRIAGE SESSION DETAIL: ID {t_id}")
        print("=" * 86)
        print(f"Source Directory:      {src}")
        print(f"Destination:           {dest}")
        print(f"Status:                {status}")
        print(f"Total Scanned Files:   {scanned:,}")
        print(f"Duplicates Diverted:   {dups:,} ({format_size(dup_bytes)})")
        print(f"Review-Similar Routed: {review:,}")
        print(f"New Media Routed:      {new_cnt:,}")
        print(f"Execution Period:      {started} -> {completed}")
        print("-" * 86)
        print("Classification Breakdown:")
        for cat, sub, cnt, sz in subtypes:
            print(f"   • [{cat.upper():<14}] {sub:<25} : {cnt:>6,} files ({format_size(sz):>10})")
        print("-" * 86)
        if interesting_samples:
            print("Top Novel / Review Assets Sample:")
            for fn, cat, sub, sz, reason, counterpart in interesting_samples:
                print(f"   [{cat.upper():<14}] {fn:<22} ({format_size(sz):>8}) -> {reason[:60]}")
        print("=" * 86 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Intake Triage Sessions & Deduplication Reporting Tool"
    )
    parser.add_argument(
        "--db",
        default=MEDIA_ORGANIZER_DB_PATH,
        help=f"Path to SQLite database (default: {MEDIA_ORGANIZER_DB_PATH})"
    )
    parser.add_argument(
        "--session",
        help="Inspect details and asset classification breakdown for a specific session ID"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of sessions shown in table"
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Output report formatted as GitHub Flavored Markdown"
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Export triage sessions data to specified CSV path"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fmt = "markdown" if args.markdown else "text"
    generate_triage_report(
        db_path=args.db,
        session_id=args.session,
        output_format=fmt,
        csv_path=args.csv_path,
        limit=args.limit
    )
