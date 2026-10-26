#!/usr/bin/env python3
"""
triage_folder_review.py

Interactive Folder Triage & Decision Flow:
Bubbles up non-duplicate triage assets from 'New' and 'Review-Similar'
ranked by temporal isolation distance (furthest date from master library),
and prompts the user to process the candidate's parent folder:
  - [I]gnore:   Mark entire folder as Ignored (skips all its assets, instantly
                re-evaluating and bubbling up the next unreviewed folder).
  - [S]chedule: Mark entire folder as Scheduled for library ingestion/curation.
  - [O]pen:     Reveal folder in macOS Finder to visually inspect files.
  - [N]ext:     Skip folder for now and see the next candidate.
  - [L]ist:     View all currently Ignored and Scheduled folders.
  - [Q]uit:     Save and exit.

Decisions are persisted in media_organizer.db table 'triage_folder_decisions'.

Usage:
  python3 scripts/triage_folder_review.py
  python3 scripts/triage_folder_review.py --category new
  python3 scripts/triage_folder_review.py --session 14
  python3 scripts/triage_folder_review.py --list
  python3 scripts/triage_folder_review.py --reset "folder_name"
"""

import os
import sys
import re
import bisect
import sqlite3
import argparse
import subprocess
import time
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LACIE_DESTINATION_ROOT
)


def ensure_decision_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS triage_folder_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_key TEXT UNIQUE,
            decision TEXT CHECK(decision IN ('ignored', 'scheduled', 'skipped')),
            triage_id TEXT,
            assets_count INTEGER DEFAULT 0,
            sample_asset TEXT,
            temporal_gap_days REAL,
            notes TEXT,
            created_at_utc TEXT DEFAULT (datetime('now')),
            updated_at_utc TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def get_stored_decisions(db_path):
    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='triage_folder_decisions'")
    if not cur.fetchone():
        conn.close()
        return {}
    cur.execute("SELECT folder_key, decision FROM triage_folder_decisions")
    decisions = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    return decisions


def record_folder_decision(db_path, folder_key, decision, triage_id, assets_count, sample_asset, gap_days, notes=None):
    conn = sqlite3.connect(db_path, timeout=30.0)
    ensure_decision_table(conn)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO triage_folder_decisions (
            folder_key, decision, triage_id, assets_count, sample_asset, temporal_gap_days, notes, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(folder_key) DO UPDATE SET
            decision = excluded.decision,
            assets_count = excluded.assets_count,
            sample_asset = excluded.sample_asset,
            temporal_gap_days = excluded.temporal_gap_days,
            notes = excluded.notes,
            updated_at_utc = datetime('now')
    """, (folder_key, decision, triage_id, assets_count, sample_asset, gap_days, notes))
    conn.commit()
    conn.close()


def format_size(bytes_val):
    if not bytes_val or bytes_val <= 0:
        return "0.0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def format_eta(seconds):
    """Formats remaining seconds into a compact, human-readable ETA string (e.g. '45s', '2m 15s', '1h 05m')."""
    if seconds is None or seconds < 0:
        return "--:--"
    s = int(seconds)
    if s >= 3600:
        h = s // 3600
        m = (s % 3600) // 60
        return f"{h}h {m:02d}m"
    elif s >= 60:
        m = s // 60
        sec = s % 60
        return f"{m}m {sec:02d}s"
    else:
        return f"{s}s"


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

    # 2. Filesystem birthtime / mtime
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

    return None


def open_folder_in_finder(folder_path):
    if not folder_path or not os.path.exists(folder_path):
        print(f"⚠️  Directory does not exist locally: {folder_path}")
        return
    try:
        subprocess.run(["open", folder_path], check=True)
        print(f"📂 Opened in macOS Finder: {folder_path}")
    except Exception as e:
        print(f"❌ Failed to open in Finder: {e}")


def list_decisions(db_path):
    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True)
    cur = conn.cursor()
    ensure_decision_table(conn)
    cur.execute("""
        SELECT folder_key, decision, triage_id, assets_count, sample_asset, temporal_gap_days, updated_at_utc
        FROM triage_folder_decisions
        ORDER BY decision, assets_count DESC
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("\n✨ No folder decisions recorded yet.")
        return

    print("\n" + "=" * 90)
    print("📋  RECORDED TRIAGE FOLDER DECISIONS")
    print("=" * 90)
    print(f"{'Decision':<11} | {'Assets':>6} | {'Max Gap':>11} | {'Triage':<6} | {'Folder Key'}")
    print("-" * 90)
    for f_key, dec, tid, cnt, sample, gap, updated in rows:
        badge = "🔴 IGNORED" if dec == 'ignored' else "🟢 SCHEDULED" if dec == 'scheduled' else "⚪ SKIPPED"
        gap_str = format_days_gap(gap)
        print(f"{badge:<11} | {cnt:>6,} | {gap_str:>11} | {str(tid):<6} | {f_key}")
    print("=" * 90 + "\n")


def reset_decision(db_path, folder_key):
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    if folder_key.lower() == "all":
        cur.execute("DELETE FROM triage_folder_decisions")
        print(f"🧹 Cleared ALL recorded folder decisions.")
    else:
        cur.execute("DELETE FROM triage_folder_decisions WHERE folder_key LIKE ?", (f"%{folder_key}%",))
        print(f"🧹 Removed decision for matching folders: {folder_key}")
    conn.commit()
    conn.close()


def run_interactive_review(
    db_path=MEDIA_ORGANIZER_DB_PATH,
    triage_root=TRIAGE_LACIE_DESTINATION_ROOT,
    category_filter="all",
    session_filter=None,
    auto_ignore_pattern=None
):
    if not os.path.isfile(db_path):
        print(f"❌ Database not found at: {db_path}", file=sys.stderr)
        sys.exit(1)

    # 1. Load Master Library timeline
    sys.stdout.write("📚 Loading Master Library timeline from database...\n")
    sys.stdout.flush()

    conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=30.0)
    cur = conn.cursor()
    cur.execute("""
        SELECT date_created_utc, asset_id, original_filename
        FROM assets
        WHERE date_created_utc IS NOT NULL
          AND date_created_utc >= '1980-01-01'
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

    ts_list = [item[0] for item in timeline]
    print(f"✅ Loaded {len(timeline):,} master library assets into timeline.")

    # 2. Query all non-duplicate triage assets
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
            ts.source_directory,
            tsa.original_rel_path
        FROM triage_session_assets tsa
        LEFT JOIN triage_sessions ts ON ts.triage_id = tsa.triage_id OR ts.id = tsa.triage_session_id
        WHERE {where_clause}
    """, params)
    triage_rows = cur.fetchall()
    conn.close()

    print(f"📂 Analyzing {len(triage_rows):,} triage assets across 'New' and 'Review-Similar' categories...")

    # Pre-extract dates and build asset items
    all_assets = []
    total_triage = len(triage_rows)
    t_start = time.time()
    last_print = t_start

    for idx_row, row in enumerate(triage_rows, 1):
        if total_triage >= 50:
            now = time.time()
            if idx_row == 1 or idx_row == total_triage or (now - last_print) >= 0.5 or idx_row % 50 == 0:
                last_print = now
                pct = (idx_row / total_triage) * 100
                elapsed = now - t_start
                eta_str = format_eta((total_triage - idx_row) / (idx_row / elapsed)) if elapsed > 0 and idx_row > 0 else "--:--"
                sys.stdout.write(f"\r⏳ Analyzing assets: {idx_row:,}/{total_triage:,} ({pct:.1f}%) | ETA: {eta_str}...")
                sys.stdout.flush()

        row_id, tid, fname, cat, sub_t, fsz, dest_p, src_dir, orig_rel = row
        p = dest_p
        if not p or not os.path.isfile(p):
            alt_cat = "New" if cat == "new" else "Review-Similar"
            p_cand = os.path.join(triage_root, alt_cat, str(tid), fname)
            if os.path.isfile(p_cand):
                p = p_cand

        src_base = os.path.basename(src_dir.rstrip('/')) if src_dir else f"Triage_{tid}"
        rel_dir = os.path.dirname(orig_rel) if orig_rel else ''
        folder_key = os.path.join(src_base, rel_dir).rstrip('/')

        # Determine physical directory for Finder open
        phys_folder = os.path.dirname(p) if (p and os.path.isfile(p)) else None
        if not phys_folder and src_dir and orig_rel:
            cand = os.path.dirname(os.path.join(src_dir, orig_rel))
            if os.path.isdir(cand):
                phys_folder = cand

        dt = extract_creation_date(fname, p)
        if not dt:
            continue

        target_ts = dt.timestamp()
        idx = bisect.bisect_left(ts_list, target_ts)
        before = timeline[idx - 1] if idx > 0 else None
        after = timeline[idx] if idx < len(timeline) else None

        gap_bef = (target_ts - before[0]) / 86400.0 if before else 999999.0
        gap_aft = (after[0] - target_ts) / 86400.0 if after else 999999.0
        min_gap = min(gap_bef, gap_aft)

        all_assets.append({
            "triage_id": tid,
            "filename": fname,
            "category": cat,
            "sub_type": sub_t,
            "file_size": fsz or 0,
            "file_path": p,
            "folder_key": folder_key,
            "physical_folder": phys_folder,
            "date": dt,
            "date_str": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "min_gap": min_gap,
            "gap_display": format_days_gap(min_gap),
            "before": before,
            "before_gap": gap_bef,
            "after": after,
            "after_gap": gap_aft
        })

    if total_triage >= 50:
        sys.stdout.write("\r" + " " * 90 + "\r")
        sys.stdout.flush()

    # Auto-ignore pattern check if provided
    decisions = get_stored_decisions(db_path)
    if auto_ignore_pattern:
        pat = re.compile(auto_ignore_pattern, re.IGNORECASE)
        auto_count = 0
        for item in all_assets:
            fk = item["folder_key"]
            if fk not in decisions and pat.search(fk):
                decisions[fk] = "ignored"
                record_folder_decision(
                    db_path, fk, "ignored", item["triage_id"], 1, item["filename"], item["min_gap"], "Auto-ignored via pattern"
                )
                auto_count += 1
        if auto_count > 0:
            print(f"🤖 Auto-ignored {auto_count} folders matching pattern: '{auto_ignore_pattern}'")

    # Main Interactive Review Loop
    while True:
        # Re-fetch decisions to account for new entries
        decisions = get_stored_decisions(db_path)

        # Group un-ignored assets by folder_key
        folders_map = {}
        for item in all_assets:
            fk = item["folder_key"]
            if decisions.get(fk) == "ignored":
                continue  # Filter out all ignored folders!
            if decisions.get(fk) == "scheduled":
                continue  # Already scheduled

            if fk not in folders_map:
                folders_map[fk] = []
            folders_map[fk].append(item)

        if not folders_map:
            print("\n🎉 All candidate folders have been processed or scheduled!")
            print("Run 'python3 scripts/triage_folder_review.py --list' to inspect recorded decisions.")
            break

        # Find the folder with the maximum isolated asset
        folder_rankings = []
        for fk, items in folders_map.items():
            top_asset = max(items, key=lambda x: x["min_gap"])
            folder_rankings.append((top_asset["min_gap"], top_asset, items, fk))

        # Sort descending by isolation gap
        folder_rankings.sort(key=lambda x: x[0], reverse=True)

        top_gap, top_asset, folder_items, folder_key = folder_rankings[0]

        # Display Candidate Screen
        print("\n" + "=" * 92)
        print(f"🔍 CANDIDATE ASSET & FOLDER REVIEW ({len(folder_rankings):,} unreviewed folders remaining)")
        print("-" * 92)
        b_cat = "🟢 NEW" if top_asset["category"] == "new" else "🟡 REVIEW-SIMILAR"
        print(f"Asset:           {top_asset['filename']} ({format_size(top_asset['file_size'])}) | {b_cat} [Triage {top_asset['triage_id']}]")
        print(f"Sub-Type:        {top_asset['sub_type']}")
        print(f"Capture Date:    {top_asset['date_str']}")
        print(f"Temporal Gap:    🌟 {top_asset['gap_display']} away from ANY library asset")

        bef = top_asset["before"]
        aft = top_asset["after"]
        b_desc = f"{bef[1][:10]} ({bef[3]}) [-{top_asset['before_gap']:.1f}d]" if bef else "None (Start)"
        a_desc = f"{aft[1][:10]} ({aft[3]}) [+{top_asset['after_gap']:.1f}d]" if aft else "None (End)"
        print(f"   ⬅️  Closest Before: {b_desc}")
        print(f"   ➡️  Closest After:  {a_desc}")
        print("-" * 92)
        print(f"📂 Folder Path:   {folder_key}")
        print(f"📦 Total Assets:  {len(folder_items):,} assets in this folder")

        if len(folder_items) > 1:
            samples = [it['filename'] for it in folder_items if it['filename'] != top_asset['filename']][:4]
            print(f"   Sample Siblings: {', '.join(samples)}")

        print("-" * 92)
        print("Actions:")
        print("  [I] Ignore Folder    -> Mark entire folder as Ignored (skips all its assets)")
        print("  [S] Schedule Folder  -> Mark folder as Scheduled for library ingestion")
        print("  [O] Open in Finder   -> Open folder in macOS Finder to visually inspect files")
        print("  [N] Next / Skip      -> Skip this folder for now and view the next candidate")
        print("  [L] List Decisions   -> View all currently Ignored & Scheduled folders")
        print("  [Q] Quit & Save      -> Exit review session")
        print("=" * 92)

        try:
            choice = input("👉 Select action [I/S/O/N/L/Q]: ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Review session paused.")
            break

        if choice == 'i':
            record_folder_decision(
                db_path, folder_key, "ignored", top_asset["triage_id"],
                len(folder_items), top_asset["filename"], top_asset["min_gap"]
            )
            print(f"🔴 Marked '{folder_key}' as IGNORED ({len(folder_items):,} assets skipped).")
            print("🔄 Re-evaluating remaining assets and updating rankings...")

        elif choice == 's':
            record_folder_decision(
                db_path, folder_key, "scheduled", top_asset["triage_id"],
                len(folder_items), top_asset["filename"], top_asset["min_gap"]
            )
            print(f"🟢 Marked '{folder_key}' as SCHEDULED for library intake ({len(folder_items):,} assets).")
            print("🔄 Re-evaluating remaining assets and updating rankings...")

        elif choice == 'o':
            target_p = top_asset.get("physical_folder")
            if target_p:
                open_folder_in_finder(target_p)
            else:
                print(f"⚠️ Could not locate physical directory on disk for: {folder_key}")
            # Do not advance, prompt again for same folder
            continue

        elif choice == 'n':
            # Temporary skip in memory for this session
            decisions[folder_key] = "skipped"
            print(f"⏭️  Skipped folder for this session.")

        elif choice == 'l':
            list_decisions(db_path)

        elif choice == 'q':
            print("👋 Exiting review session. Decisions saved to media_organizer.db.")
            break
        else:
            print("⚠️  Invalid option. Please choose I, S, O, N, L, or Q.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Interactive Triage Folder Review and Temporal Decision Flow"
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
        "--category",
        choices=["all", "new", "review_similar", "review"],
        default="all",
        help="Filter review to 'all', 'new', or 'review_similar' (default: all)"
    )
    parser.add_argument(
        "--session",
        help="Limit review to a specific triage session ID (e.g. 14, 29, 41)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all currently recorded Ignored and Scheduled folders"
    )
    parser.add_argument(
        "--reset",
        metavar="FOLDER_OR_ALL",
        help="Reset decision for a matching folder pattern, or 'all' to clear all decisions"
    )
    parser.add_argument(
        "--auto-ignore-pattern",
        metavar="REGEX",
        help="Regex pattern to automatically ignore matching folders (e.g. 'thumbnails|AppleDouble|Skype|Cache')"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.list:
        list_decisions(args.db)
    elif args.reset:
        reset_decision(args.db, args.reset)
    else:
        run_interactive_review(
            db_path=args.db,
            triage_root=args.triage_root,
            category_filter=args.category,
            session_filter=args.session,
            auto_ignore_pattern=args.auto_ignore_pattern
        )
