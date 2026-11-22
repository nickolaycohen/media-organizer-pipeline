# Commenting out below as references to this routine are being depricated
# def get_month_batch(cursor, current_code):
from db.connections import close as close_conn
import calendar
import difflib
#     """Get the next eligible batch based on the preceding_code for the given status code."""
#     cursor.execute('''
#         SELECT mb.month
#         FROM month_batches mb
#         LEFT JOIN batch_status bs ON mb.status_code = bs.preceding_code
#         WHERE mb.status_code = ?
#         ORDER BY mb.month DESC
#         LIMIT 1;
#     ''', (current_code,))
#     row = cursor.fetchone()
#     return row[0] if row else None

def get_next_code(cursor, current_code):
    """Get the next code based on the preceding code relation."""
    cursor.execute('''
        select b.code
        from batch_status b 
        where b.preceding_code = ? and length(code) = 3
        LIMIT 1;
    ''', (current_code,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_planned_month(cursor):
    cursor.execute("""
        SELECT planned_month 
        FROM planned_execution 
        WHERE active = 1 
        ORDER BY id ASC
        LIMIT 1
    """)
    row = cursor.fetchone()
    return row[0] if row else None

# def get_month_batch_album_verified(cursor):
#     """Get the next pending batch from month_batches."""
#     cursor.execute('''
#         SELECT month FROM month_batches
#         WHERE status_code = '100'
#         ORDER BY month DESC
#         LIMIT 1;
#     ''')
#     next_batch = cursor.fetchone()
#     return next_batch[0] if next_batch else None

def get_stage_transitions(cursor):
    cursor.execute("""
        SELECT code, preceding_code, full_description, transition_type, short_label
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND code NOT LIKE '%E'
        ORDER BY code ASC
    """)
    return cursor.fetchall()

def get_batch_statuses(cursor):
    cursor.execute("""
        SELECT 
            mb.month, 
            mb.status_code,
            COALESCE(a.total_assets, 0) as total_assets,
            COALESCE(a.fav_assets, 0) as fav_assets,
            a.max_updated as fav_synced
        FROM month_batches mb
        LEFT JOIN (
            SELECT 
                month, 
                COUNT(*) as total_assets,
                SUM(CASE WHEN google_favorite = 1 THEN 1 ELSE 0 END) as fav_assets,
                MAX(CASE WHEN google_favorite = 1 THEN updated_at_utc END) as max_updated
            FROM assets
            GROUP BY month
        ) a ON mb.month = a.month
        ORDER BY mb.month DESC
    """)
    return cursor.fetchall()

def get_latest_import_and_month(cursor, transition_type="pipeline"):
    """
    Fetch the latest import and complete month for a given transition type.
    Default is 'pipeline'.
    """
    cursor.execute(f"""
        SELECT (
            SELECT i.import_uuid
            FROM assets a
            JOIN imports i ON a.import_id = i.import_uuid
            WHERE a.month = mb2.month
            ORDER BY i.import_uuid DESC
            LIMIT 1
        ) AS latest_import,
        mb2.month
        FROM month_batches mb2
        WHERE mb2.month < strftime('%Y-%m', 'now')
          AND mb2.status_code IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM batch_status bs
            WHERE bs.preceding_code = mb2.status_code
              AND bs.transition_type = ?
              AND bs.code NOT LIKE '%E'
          )
        ORDER BY mb2.month DESC
        LIMIT 1;
     """, (transition_type,))
    return cursor.fetchone()


def ensure_planned_execution_override_column(cursor):
    """Idempotently add skip_month_selection_check column on planned_execution."""
    cursor.execute("PRAGMA table_info(planned_execution)")
    cols = {row[1] for row in cursor.fetchall()}
    if "skip_month_selection_check" not in cols:
        cursor.execute(
            "ALTER TABLE planned_execution ADD COLUMN skip_month_selection_check INTEGER DEFAULT 0"
        )


def should_skip_month_selection_check(cursor, month_str):
    """Return True if batch is bypassed or has an explicit queued override in planned_execution."""
    cursor.execute("SELECT COALESCE(is_bypassed, 0) FROM month_batches WHERE month = ?", (month_str,))
    row = cursor.fetchone()
    if row and int(row[0] or 0) == 1:
        return True

    cursor.execute("PRAGMA table_info(planned_execution)")
    cols = {r[1] for r in cursor.fetchall()}
    if "skip_month_selection_check" in cols:
        cursor.execute(
            """
            SELECT COALESCE(MAX(skip_month_selection_check), 0)
            FROM planned_execution
            WHERE planned_month = ? AND active = 1
            """,
            (month_str,),
        )
        ov = cursor.fetchone()
        if ov and int(ov[0] or 0) == 1:
            return True
    return False


def _format_expected_month_selection_album(month_str):
    """Convert YYYY-MM to '<MonthName> <YYYY>', e.g. '2026-09' -> 'September 2026'."""
    parts = str(month_str or "").strip().split("-", 1)
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        year = parts[0]
        month_num = int(parts[1])
        if 1 <= month_num <= 12:
            return f"{calendar.month_name[month_num]} {year}", year
    return str(month_str or "").strip(), ""


def check_apple_month_selection_album(cursor, month_str):
    """Verify whether batch YYYY-MM has an existing, non-empty album under 'Apple Photos Month Selection'."""
    expected_album, year = _format_expected_month_selection_album(month_str)
    expected_lower = expected_album.lower()

    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='media_organizer_albums' LIMIT 1"
    )
    has_mo_albums = cursor.fetchone() is not None

    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='media_organizer_album_assets' LIMIT 1"
    )
    has_mo_assets = cursor.fetchone() is not None

    candidates = []
    if has_mo_albums:
        if has_mo_assets:
            cursor.execute(
                """
                SELECT
                    moa.album_pk,
                    moa.album_name,
                    (SELECT COUNT(*) FROM media_organizer_album_assets moaa WHERE moaa.album_pk = moa.album_pk) AS member_count,
                    COALESCE(moa.asset_count, 0) AS raw_count
                FROM media_organizer_albums moa
                WHERE COALESCE(moa.is_folder, 0) = 0
                  AND (
                      LOWER(TRIM(COALESCE(moa.parent_folder_name, ''))) = 'apple photos month selection'
                      OR LOWER(COALESCE(moa.full_path, '')) LIKE '%/apple photos month selection/%'
                  )
                """
            )
            candidates = [
                (r[0], r[1], int(r[2] if r[2] is not None else (r[3] or 0)))
                for r in cursor.fetchall()
                if r[1]
            ]
        else:
            cursor.execute(
                """
                SELECT album_pk, album_name, COALESCE(asset_count, 0)
                FROM media_organizer_albums
                WHERE COALESCE(is_folder, 0) = 0
                  AND (
                      LOWER(TRIM(COALESCE(parent_folder_name, ''))) = 'apple photos month selection'
                      OR LOWER(COALESCE(full_path, '')) LIKE '%/apple photos month selection/%'
                  )
                """
            )
            candidates = [(r[0], r[1], int(r[2] or 0)) for r in cursor.fetchall() if r[1]]

    if not candidates:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='albums' LIMIT 1")
        if cursor.fetchone() is not None:
            cursor.execute(
                """
                SELECT album_pk, album_name
                FROM albums
                WHERE LOWER(TRIM(COALESCE(parent_folder_name, ''))) = 'apple photos month selection'
                """
            )
            album_rows = cursor.fetchall()
            if album_rows:
                cursor.execute(
                    "SELECT COUNT(*) FROM assets WHERE month = ? AND COALESCE(apple_photos_monthly_selection, 0) = 1",
                    (month_str,),
                )
                sel_row = cursor.fetchone()
                fallback_cnt = int(sel_row[0] or 0) if sel_row else 0
                candidates = [(r[0], r[1], fallback_cnt) for r in album_rows if r[1]]

    cursor.execute(
        "SELECT COUNT(*) FROM assets WHERE month = ? AND COALESCE(apple_photos_monthly_selection, 0) = 1",
        (month_str,),
    )
    sel_row = cursor.fetchone()
    batch_selected_count = int(sel_row[0] or 0) if sel_row else 0

    matched = None
    for apk, aname, acnt in candidates:
        if aname.strip().lower() == expected_lower:
            matched = (apk, aname, acnt)
            break

    if matched is None and year and candidates:
        same_year = {
            aname.strip().lower(): (apk, aname, acnt)
            for apk, aname, acnt in candidates
            if aname.strip().lower().endswith(f" {year}")
        }
        close = difflib.get_close_matches(expected_lower, list(same_year.keys()), n=1, cutoff=0.85)
        if close:
            matched = same_year[close[0]]

    if matched is None:
        return {
            "month": month_str,
            "expected_album": expected_album,
            "exists": False,
            "has_assets": False,
            "asset_count": 0,
            "matched_album_name": None,
            "album_pk": None,
            "batch_selected_count": batch_selected_count,
            "ready": False,
            "status": "missing",
        }

    apk, aname, acnt = matched
    has_assets = acnt > 0
    return {
        "month": month_str,
        "expected_album": expected_album,
        "exists": True,
        "has_assets": has_assets,
        "asset_count": acnt,
        "matched_album_name": aname.strip(),
        "album_pk": apk,
        "batch_selected_count": batch_selected_count,
        "ready": has_assets,
        "status": "ready" if has_assets else "empty",
    }