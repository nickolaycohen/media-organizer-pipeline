#!/usr/bin/env python3
"""
report_library_duplicates.py

Generates comprehensive asset distribution and duplicate analysis reports
for the master library catalog (All-Media.photoslibrary) from media_organizer.db.

Outputs:
  - Yearly asset distribution (file count, total storage, unique assets)
  - Identified duplicate assets (same camera filename + exact byte size, or same date + size)
  - Duplicate ratio per year and library-wide
  - Duplicate cluster metrics: total clusters, redundant copies, and reclaimable storage

Usage:
  python3 scripts/report_library_duplicates.py
  python3 scripts/report_library_duplicates.py --markdown
  python3 scripts/report_library_duplicates.py --match-criteria size_date
  python3 scripts/report_library_duplicates.py --csv report_duplicates.csv
  python3 scripts/report_library_duplicates.py --show-clusters 10
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
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def generate_library_duplicate_report(db_path=MEDIA_ORGANIZER_DB_PATH, criteria="name_size", show_clusters=0, output_format="text", csv_path=None):
    if not os.path.isfile(db_path):
        print(f"❌ Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=30.0)
    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout = 30000")

    # Define duplicate grouping based on criteria
    if criteria == "size_date":
        criteria_label = "Exact Byte Size + Creation Date"
        dup_cte = """
            SELECT 
                aaa.ZORIGINALFILESIZE AS fsize, 
                a.date_created_utc AS dmatch,
                COUNT(*) AS cnt,
                (COUNT(*) - 1) * aaa.ZORIGINALFILESIZE AS redundant_bytes
            FROM assets a
            JOIN ZASSET za ON za.ZUUID = a.asset_id
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
            WHERE aaa.ZORIGINALFILESIZE IS NOT NULL AND a.date_created_utc IS NOT NULL
            GROUP BY aaa.ZORIGINALFILESIZE, a.date_created_utc
            HAVING COUNT(*) > 1
        """
        join_clause = "ON ds.fsize = aaa.ZORIGINALFILESIZE AND ds.dmatch = a.date_created_utc"
    else:
        criteria_label = "Exact Byte Size + Camera Filename"
        dup_cte = """
            SELECT 
                aaa.ZORIGINALFILESIZE AS fsize, 
                aaa.ZORIGINALFILENAME AS nmatch,
                COUNT(*) AS cnt,
                (COUNT(*) - 1) * aaa.ZORIGINALFILESIZE AS redundant_bytes
            FROM assets a
            JOIN ZASSET za ON za.ZUUID = a.asset_id
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
            WHERE aaa.ZORIGINALFILESIZE IS NOT NULL AND aaa.ZORIGINALFILENAME IS NOT NULL
            GROUP BY aaa.ZORIGINALFILESIZE, aaa.ZORIGINALFILENAME
            HAVING COUNT(*) > 1
        """
        join_clause = "ON ds.fsize = aaa.ZORIGINALFILESIZE AND ds.nmatch = aaa.ZORIGINALFILENAME"

    query = f"""
        WITH dup_stats AS (
            {dup_cte}
        )
        SELECT 
            COALESCE(SUBSTR(a.month, 1, 4), SUBSTR(a.date_created_utc, 1, 4), 'Unknown') AS yr,
            COUNT(a.asset_id) AS total_assets,
            SUM(COALESCE(aaa.ZORIGINALFILESIZE, 0)) AS total_bytes,
            SUM(CASE WHEN ds.cnt > 1 THEN 1 ELSE 0 END) AS potential_dup_assets,
            SUM(CASE WHEN ds.cnt > 1 THEN (ds.cnt - 1) ELSE 0 END) AS redundant_copies,
            COUNT(DISTINCT a.asset_id) AS distinct_assets
        FROM assets a
        JOIN ZASSET za ON za.ZUUID = a.asset_id
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
        LEFT JOIN dup_stats ds {join_clause}
        GROUP BY yr
        ORDER BY yr
    """

    cur.execute(query)
    rows = cur.fetchall()

    # Cluster summary query
    cluster_query = f"""
        WITH dup_groups AS (
            {dup_cte}
        )
        SELECT 
            COUNT(*) AS total_duplicate_clusters,
            SUM(cnt) AS total_files_in_clusters,
            SUM(cnt - 1) AS redundant_extra_copies,
            SUM(redundant_bytes) AS total_wasted_bytes
        FROM dup_groups
    """
    cur.execute(cluster_query)
    cluster_res = cur.fetchone() or (0, 0, 0, 0)
    total_clusters, files_in_clusters, extra_copies, wasted_bytes = cluster_res

    # CSV Export if requested
    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(["Year", "Total Assets", "Total Bytes", "Total Size Formatted", "Duplicate Assets", "Duplicate Ratio Pct", "Unique Assets"])
            for yr, total, sz, dups, red_copies, uniq in rows:
                if yr == "0001": continue
                dups_val = dups or 0
                pct = (dups_val / total * 100) if total else 0.0
                writer.writerow([yr, total, sz, format_size(sz), dups_val, f"{pct:.2f}%", total - dups_val])
        print(f"📁 Report saved to CSV: {csv_path}")

    # Text / Markdown Output
    tot_assets = 0
    tot_bytes = 0
    tot_dups = 0

    filtered_rows = []
    for yr, total, sz, dups, red_copies, uniq in rows:
        if yr == "0001":
            continue
        dups_val = dups or 0
        tot_assets += total
        tot_bytes += sz
        tot_dups += dups_val
        pct = (dups_val / total * 100) if total else 0.0
        uniq_val = total - dups_val
        filtered_rows.append((yr, total, sz, dups_val, pct, uniq_val))

    overall_dup_pct = (tot_dups / tot_assets * 100) if tot_assets else 0.0

    if output_format == "markdown":
        print(f"### 📅 Master Catalog Asset Distribution & Internal Duplicates Report")
        print(f"**Database**: `{db_path}`  ")
        print(f"**Duplicate Match Criteria**: {criteria_label}  \n")
        print("| Year | Total Assets | Total Size | Identified Duplicates | Duplicate % | Unique Assets |")
        print("| :--- | :---: | :---: | :---: | :---: | :---: |")
        for yr, total, sz, dups_val, pct, uniq_val in filtered_rows:
            print(f"| **{yr}** | {total:,} | {format_size(sz)} | {dups_val:,} | {pct:.1f}% | {uniq_val:,} |")
        print("| :--- | :---: | :---: | :---: | :---: | :---: |")
        print(f"| **TOTAL** | **{tot_assets:,}** | **{format_size(tot_bytes)}** | **{tot_dups:,}** | **{overall_dup_pct:.1f}%** | **{tot_assets - tot_dups:,}** |")
        print("\n#### 🔍 Redundancy Summary")
        print(f"- **Duplicate Clusters**: {total_clusters:,}")
        print(f"- **Total Files in Clusters**: {files_in_clusters:,}")
        print(f"- **Redundant Extra Copies**: {extra_copies:,}")
        print(f"- **Potential Reclaimable Storage**: {format_size(wasted_bytes)}")
    else:
        print("=" * 82)
        print("📅  MASTER CATALOG ASSET DISTRIBUTION & INTERNAL DUPLICATES REPORT")
        print("=" * 82)
        print(f"Database:              {db_path}")
        print(f"Duplicate Criteria:    {criteria_label}")
        print("-" * 82)
        print(f"{'Year':<6} | {'Total Assets':>12} | {'Total Size':>10} | {'Dup Assets':>10} | {'Dup Ratio':>9} | {'Unique Assets':>13}")
        print("-" * 82)
        for yr, total, sz, dups_val, pct, uniq_val in filtered_rows:
            print(f"{yr:<6} | {total:>12,} | {format_size(sz):>10} | {dups_val:>10,} | {pct:>8.1f}% | {uniq_val:>13,}")
        print("-" * 82)
        print(f"{'TOTAL':<6} | {tot_assets:>12,} | {format_size(tot_bytes):>10} | {tot_dups:>10,} | {overall_dup_pct:>8.1f}% | {tot_assets - tot_dups:>13,}")
        print("=" * 82)
        print(f"🔍 Redundancy & Deduplication Metrics:")
        print(f"   • Total Duplicate Clusters:        {total_clusters:>10,}")
        print(f"   • Total Files in Clusters:         {files_in_clusters:>10,}")
        print(f"   • Redundant Extra Copies:          {extra_copies:>10,}")
        print(f"   • Wasted Redundant Storage:        {format_size(wasted_bytes):>10}")
        print("=" * 82 + "\n")

    # Optional cluster breakdown
    if show_clusters > 0:
        print(f"🔍 Top {show_clusters} Largest Duplicate Clusters:")
        print("-" * 82)
        if criteria == "size_date":
            top_q = f"""
                SELECT aaa.ZORIGINALFILENAME, a.date_created_utc, aaa.ZORIGINALFILESIZE, COUNT(*) as cnt
                FROM assets a
                JOIN ZASSET za ON za.ZUUID = a.asset_id
                JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                WHERE aaa.ZORIGINALFILESIZE IS NOT NULL AND a.date_created_utc IS NOT NULL
                GROUP BY aaa.ZORIGINALFILESIZE, a.date_created_utc
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC, (COUNT(*) - 1) * aaa.ZORIGINALFILESIZE DESC
                LIMIT ?
            """
        else:
            top_q = f"""
                SELECT aaa.ZORIGINALFILENAME, MIN(a.date_created_utc), aaa.ZORIGINALFILESIZE, COUNT(*) as cnt
                FROM assets a
                JOIN ZASSET za ON za.ZUUID = a.asset_id
                JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
                WHERE aaa.ZORIGINALFILESIZE IS NOT NULL AND aaa.ZORIGINALFILENAME IS NOT NULL
                GROUP BY aaa.ZORIGINALFILESIZE, aaa.ZORIGINALFILENAME
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC, (COUNT(*) - 1) * aaa.ZORIGINALFILESIZE DESC
                LIMIT ?
            """
        cur.execute(top_q, (show_clusters,))
        top_rows = cur.fetchall()
        for idx, (fname, dt, fsz, cnt) in enumerate(top_rows, 1):
            wasted = (cnt - 1) * (fsz or 0)
            print(f"  {idx:>2}. {fname:<25} ({format_size(fsz)} each) -> {cnt} copies, wastes {format_size(wasted)} (Date: {dt})")
        print("-" * 82 + "\n")

    conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Master Library Asset Distribution & Internal Duplicates Reporting Tool"
    )
    parser.add_argument(
        "--db",
        default=MEDIA_ORGANIZER_DB_PATH,
        help=f"Path to SQLite database (default: {MEDIA_ORGANIZER_DB_PATH})"
    )
    parser.add_argument(
        "--match-criteria",
        choices=["name_size", "size_date"],
        default="name_size",
        help="Criteria for detecting internal duplicates: 'name_size' (filename + size) or 'size_date' (date + size)"
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Output report formatted as GitHub Flavored Markdown"
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Export yearly report data to specified CSV path"
    )
    parser.add_argument(
        "--show-clusters",
        type=int,
        default=0,
        metavar="N",
        help="Display top N largest duplicate clusters"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fmt = "markdown" if args.markdown else "text"
    generate_library_duplicate_report(
        db_path=args.db,
        criteria=args.match_criteria,
        show_clusters=args.show_clusters,
        output_format=fmt,
        csv_path=args.csv_path
    )
