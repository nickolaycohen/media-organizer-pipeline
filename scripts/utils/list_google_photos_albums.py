import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import argparse
from google_photos import list_albums, authenticate
from constants import LOG_PATH, GOOGLE_PHOTOS_READONLY_SCOPES
from utils.logger import setup_logger

MODULE_TAG = 'list_google_photos_albums'

logger = setup_logger(LOG_PATH, MODULE_TAG)

def main():
    parser = argparse.ArgumentParser(description="List Google Photos albums and optionally filter by title.")
    parser.add_argument("--filter", help="Only show albums with titles containing this text (case-insensitive).")
    args = parser.parse_args()

    logger.info("📂 Fetching list of Google Photos albums...")
    if args.filter:
        logger.info(f"Filtering for albums containing: '{args.filter}'")

    found_albums = []
    creds = authenticate(scopes=GOOGLE_PHOTOS_READONLY_SCOPES)
    try:
        for album in list_albums(creds):
            title = album.get('title', 'Untitled')

            # Apply filter if provided
            if args.filter and args.filter.lower() not in title.lower():
                continue

            media_count = album.get('mediaItemsCount', '0')
            logger.info(f"Album: {title} ({media_count} items)")
            found_albums.append((title, media_count))
    except Exception as e:
        logger.error(f"Failed to list albums: {e}")

    logger.info(f"Found {len(found_albums)} albums matching the criteria.")


if __name__ == "__main__":
    main()