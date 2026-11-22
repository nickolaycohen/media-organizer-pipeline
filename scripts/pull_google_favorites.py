import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests
import logging
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH, PLANNER_REQUIRED_SCOPES
from google_photos import create_or_get_album, get_all_favorites, authenticate
from utils.logger import setup_logger
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_planned_month, check_apple_month_selection_album, should_skip_month_selection_check

MODULE_TAG = 'pull_google_favorites'
logger = setup_logger(LOG_PATH, MODULE_TAG)
for handler in logger.handlers:
    handler.setFormatter(
        logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s')
    )

# -------------------- Existing pull logic --------------------


def get_album_items(creds, album_id):
    """Fetches all media items from a specific album using provided credentials."""
    logger.info(f"📥 Fetching all media items from album ID: {album_id}")
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'}
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search'
    body = {'albumId': album_id, 'pageSize': 100}

    items = []
    while True:
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            logger.error(f"Failed to fetch album items: {response.text}")
            break
        data = response.json()
        items.extend(data.get('mediaItems', []))
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
        body['pageToken'] = next_page_token

    logger.info(f"✅ Retrieved {len(items)} media items from album.")
    return items


def main():
    skip_month_selection_flag = "--skip-month-selection-check" in sys.argv
    conn = get_connection()
    cursor = get_cursor()

    album_month = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith('-') else get_planned_month(cursor)
    if not album_month:
        logger.error("No active planned batch found. Please run the planner first.")
        close_conn()
        return

    # Pre-flight check: verify batch has its album populated under Apple Photos Month Selection
    ms_status = check_apple_month_selection_album(cursor, album_month)
    if ms_status["ready"]:
        logger.info(
            f"✅ Verified Apple Photos Month Selection album '{ms_status['matched_album_name']}' "
            f"({ms_status['asset_count']} asset(s)) for batch {album_month}."
        )
    else:
        allow_override = skip_month_selection_flag or should_skip_month_selection_check(cursor, album_month)
        reason = (
            f"album '{ms_status['matched_album_name']}' exists under 'Apple Photos Month Selection' but has 0 assets"
            if ms_status["exists"]
            else f"no album named '{ms_status['expected_album']}' found under 'Apple Photos Month Selection'"
        )
        if allow_override:
            logger.warning(
                f"⚠️ Apple Photos Month Selection check bypassed/overridden for {album_month} ({reason}). Proceeding with Google Favorites pull."
            )
        else:
            logger.error(
                f"❌ Blocked Google Favorites pull for {album_month}: {reason}. "
                f"Create/populate '{ms_status['expected_album']}' under 'Apple Photos Month Selection' in Apple Photos, "
                f"or re-run with --skip-month-selection-check."
            )
            close_conn()
            sys.exit(1)

    # --- Single, Consolidated Authentication ---
    # This script needs to create albums and read the entire library.
    # Authenticate once with a scope that covers all required actions.
    logger.info("Authenticating with full planner scopes (edit + readonly)...")
    creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)
    if not creds:
        logger.error("Authentication failed. Cannot proceed.")
        close_conn()
        sys.exit(1)

    album_title = f"Currently Curating - {album_month}"
    album_id = create_or_get_album(creds, album_title)

    all_favorites = get_all_favorites(creds)
    logger.info(f"📊 Total favorite items globally: {len(all_favorites)}")

    album_items = get_album_items(creds, album_id)
    logger.info(f"📊 Total media items in album '{album_title}': {len(album_items)}")

    favorite_set = {(f.get('filename'), f.get('mediaMetadata', {}).get('creationTime')) for f in all_favorites}
    matched = [item for item in album_items
               if (item.get('filename'), item.get('mediaMetadata', {}).get('creationTime')) in favorite_set]

    logger.info(f"🔍 Number of album items matched with global favorites: {len(matched)}")
    logger.info(f"ℹ️ Album items that are not favorites: {len(album_items) - len(matched)}")

    matched_sorted = sorted(
        matched,
        key=lambda x: x.get('mediaMetadata', {}).get('creationTime', '')
    )

    logger.info(f"✅ Matched {len(matched_sorted)} favorites from curated album.")

    # --- Idempotency Improvement ---
    # First, reset all google_favorite flags for the current month.
    # This ensures that if a photo was previously a favorite but is no longer,
    # its status is correctly updated in the database.
    logger.info(f"Resetting google_favorite flag for all assets in month {album_month} before update...")
    cursor.execute("UPDATE assets SET google_favorite = 0, updated_at_utc = datetime('now') WHERE month = ?", (album_month,))
    logger.info(f"Reset {cursor.rowcount} assets. Now applying current favorites.")
    commit()
    # --- End Improvement ---


    update_count = 0
    for item in matched_sorted:
        filename = item.get('filename')
        raw_creation_time = item.get('mediaMetadata', {}).get('creationTime', '')
        creation_time = raw_creation_time.replace('T', ' ').split('.')[0] if raw_creation_time else ''
        if filename and creation_time:
            cursor.execute("""
                UPDATE assets
                SET google_favorite = 1, updated_at_utc = datetime('now')
                WHERE original_filename = ? AND date_created_utc = ?
            """, (filename, creation_time))
            if cursor.rowcount:
                update_count += 1

    commit()
    close_conn()
    logger.info(f"✅ Updated {update_count} asset(s) in database as Google favorites.")

    for item in matched_sorted:
        filename = item.get('filename', 'N/A')
        creation_time = item.get('mediaMetadata', {}).get('creationTime', 'N/A')
        logger.info(f"⭐️ {filename} — {creation_time}")

def run_retroactive(creds):
    logger.info("Starting retroactive Google favorites sync for bypassed months...")
    conn = get_connection()
    cursor = get_cursor()

    cursor.execute("SELECT month FROM month_batches WHERE is_bypassed = 1")
    bypassed_months = [row[0] for row in cursor.fetchall()]
    if not bypassed_months:
        logger.info("ℹ️ No bypassed months found in database.")
        close_conn()
        return

    logger.info(f"🔍 Found bypassed months: {bypassed_months}")

    all_favorites = get_all_favorites(creds)
    logger.info(f"📊 Total favorite items globally: {len(all_favorites)}")
    favorite_set = {(f.get('filename'), f.get('mediaMetadata', {}).get('creationTime')) for f in all_favorites}

    for month in bypassed_months:
        logger.info(f"Processing bypassed month: {month}")
        album_title = f"Currently Curating - {month}"
        album_id = create_or_get_album(creds, album_title)
        if not album_id:
            logger.warning(f"⚠️ Could not find Google Photos album for {month}. Skipping.")
            continue

        album_items = get_album_items(creds, album_id)
        logger.info(f"📊 Total media items in Google album '{album_title}': {len(album_items)}")

        matched = [item for item in album_items
                   if (item.get('filename'), item.get('mediaMetadata', {}).get('creationTime')) in favorite_set]
        logger.info(f"🔍 Found {len(matched)} remote favorites in album '{album_title}'.")

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
                """, (filename, creation_time, month))
                if cursor.rowcount:
                    update_count += 1
                    logger.info(f"⭐️ Retroactively matched favorite: {filename} for month {month}")

        commit()
        logger.info(f"✅ Bypassed month {month}: Retroactively updated {update_count} asset(s) as Google favorites.")

    close_conn()

if __name__ == "__main__":
    if "--retroactive" in sys.argv:
        logger.info("Authenticating with full planner scopes for retroactive operations...")
        creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)
        if not creds:
            logger.error("Authentication failed. Cannot proceed.")
            sys.exit(1)
        run_retroactive(creds)
    else:
        main()