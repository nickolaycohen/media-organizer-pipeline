import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests
import logging
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from constants import LOG_PATH, MEDIA_ORGANIZER_DB_PATH, GOOGLE_PHOTOS_READONLY_SCOPES
from google_photos import create_or_get_album, get_all_favorites, authenticate
from utils.logger import setup_logger
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_planned_month

MODULE_TAG = 'pull_google_favorites'
logger = setup_logger(LOG_PATH, MODULE_TAG)
for handler in logger.handlers:
    handler.setFormatter(
        logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s')
    )

# -------------------- Existing pull logic --------------------


def get_album_items(album_id):
    # This function needs read-only access to get album contents.
    # The authenticate call inside get_all_favorites will likely handle this,
    # but for clarity, we can authenticate here as well if needed.
    creds = authenticate(scopes=GOOGLE_PHOTOS_READONLY_SCOPES)
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

    # DB calls
    conn = get_connection()
    cursor = get_cursor()

    album_month = get_planned_month(cursor)
    if not album_month:
        logger.error("No active planned batch found. Please run the planner first.")
        return

    album_title = f"Currently Curating - {album_month}"
    album_id = create_or_get_album(album_title)
    if not album_id:
        logger.error(f"Album '{album_title}' not found.")
        sys.exit(1)

    all_favorites = get_all_favorites()
    logger.info(f"📊 Total favorite items globally: {len(all_favorites)}")

    album_items = get_album_items(album_id)
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
    #con_close()
    logger.info(f"✅ Updated {update_count} asset(s) in database as Google favorites.")

    for item in matched_sorted:
        filename = item.get('filename', 'N/A')
        creation_time = item.get('mediaMetadata', {}).get('creationTime', 'N/A')
        logger.info(f"⭐️ {filename} — {creation_time}")

if __name__ == "__main__":
    main()