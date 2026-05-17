import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import google.auth.exceptions
from googleapiclient.discovery import build
import logging
from utils.logger import setup_logger, compute_file_hash
from constants import LOG_PATH, GOOGLE_DRIVE_READ_ONLY_SCOPES, GOOGLE_PHOTOS_APPEND_ONLY_SCOPES, GOOGLE_PHOTOS_READONLY_SCOPES, PLANNER_REQUIRED_SCOPES
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_planned_month


MODULE_TAG = 'google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_BASE_URL = 'https://photoslibrary.googleapis.com/v1'
TOKEN_FILE = os.path.abspath(os.path.join(SCRIPT_DIR, '../secrets/token.json'))
CLIENT_SECRET_FILE = os.path.abspath(os.path.join(SCRIPT_DIR, '../secrets/client_secret.json'))

def human_readable_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    p = 1024
    while size_bytes >= p and i < len(size_name)-1:
        size_bytes /= p
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"

def authenticate(scopes=None, force_refresh=False):
    if scopes is None:
        logger.error("[Google Photos] Authenticate called without specifying scopes. This is not allowed.")
        raise ValueError("Scopes must be provided to the authenticate function.")

    creds = None

    # Check for existing token and validate its scopes
    if os.path.exists(TOKEN_FILE):
        try:
            # Load credentials from file without specifying scopes first
            loaded_creds = Credentials.from_authorized_user_file(TOKEN_FILE)
            # Verify that all requested scopes are present in the token.
            if all(scope in loaded_creds.scopes for scope in scopes):
                creds = loaded_creds
            else:
                missing = [s for s in scopes if s not in loaded_creds.scopes]
                logger.warning(f"[Google Photos] Token missing required scopes: {missing}. Re-authenticating.")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                creds = None  # Force re-authentication
        except Exception as e:
            logger.warning(f"[Google Photos] Could not load token file. It might be corrupted or invalid. Re-authenticating. Error: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("[Google Photos] Credentials expired. Refreshing token...")
                creds.refresh(Request())
                # Save the refreshed token immediately to prevent stale token usage
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                logger.info(f"[Google Photos] Refreshed credentials saved to {TOKEN_FILE}")
            except google.auth.exceptions.RefreshError as e:
                logger.warning("[Google Photos] Token expired or revoked. Removing token and re-authenticating.")
                logger.error(f"Refresh error: {e}")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, scopes)
                creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        else:
            logger.info("[Google Photos] No valid credentials found. Starting new authentication flow.")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, scopes)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        logger.info(f"[Google Photos] New credentials saved to {TOKEN_FILE}")

    # Final validation: log exactly what scopes were granted by the server
    granted_scopes = getattr(creds, 'scopes', [])
    logger.info(f"[Google Photos] Authentication ready. Scopes granted: {granted_scopes}")
    
    if not all(scope in granted_scopes for scope in scopes):
        logger.error(f"❌ CRITICAL: The user did not grant all requested scopes. Missing: {[s for s in scopes if s not in granted_scopes]}")

    return creds

def create_or_get_album(creds, album_title):
    headers = {'Authorization': f'Bearer {creds.token}'}
    albums = []
    page_token = None

    # Paginate through all albums
    while True:
        params = {'pageSize': 50}
        if page_token:
            params['pageToken'] = page_token

        response = requests.get(f'{API_BASE_URL}/albums', headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(f'Failed to list albums: {response.text}')
        data = response.json()
        albums.extend(data.get('albums', []))
        page_token = data.get('nextPageToken')
        if not page_token:
            break

    # Find all matches
    matches = [a for a in albums if a.get('title') == album_title]
    if matches:
        album_id = matches[0]['id']
        media_count = matches[0].get('mediaItemsCount', '0')
        if len(matches) > 1:
            logger.warning(f"Multiple albums found with title '{album_title}'. Returning the first one (ID: {album_id}).")
        else:
            logger.info(f"Found existing album '{album_title}' (ID: {album_id}). mediaItemsCount: {media_count}")
        return album_id

    # Create album if not found
    # We use PLANNER_REQUIRED_SCOPES to ensure we don't lose existing library/drive access
    logger.info(f"Album '{album_title}' not found. Ensuring credentials have required permissions to create...")
    creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)

    headers = {'Authorization': f'Bearer {creds.token}'} # IMPORTANT: Re-create headers with the new token
    body = {'album': {'title': album_title}}
    response = requests.post(f'{API_BASE_URL}/albums', headers=headers, json=body)
    if response.status_code == 200:
        album_id = response.json()['id']
        logger.info(f"Created new album '{album_title}' (ID: {album_id}).")
        return album_id
    raise Exception(f'Failed to create album: {response.text}')


def upload_media(creds, file_path, album_id):
    headers = {
        'Authorization': f'Bearer {creds.token}',
        'Content-type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': os.path.basename(file_path),
        'X-Goog-Upload-Protocol': 'raw',
    }
    with open(file_path, 'rb') as file:
        upload_token_res = requests.post(f'{API_BASE_URL}/uploads', headers=headers, data=file)
    if upload_token_res.status_code != 200:
        raise Exception(f'Upload failed: {upload_token_res.text}')
    upload_token = upload_token_res.text
    create_body = {
        'newMediaItems': [
            {
                'description': '',
                'simpleMediaItem': {
                    'uploadToken': upload_token
                }
            }
        ],
        'albumId': album_id
    }
    create_response = requests.post(f'{API_BASE_URL}/mediaItems:batchCreate', headers={
        'Authorization': f'Bearer {creds.token}',
        'Content-type': 'application/json'
    }, json=create_body)
    if create_response.status_code != 200:
        raise Exception(f'Failed to create media item: {create_response.text}')

def get_all_favorites(creds):
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'}

    url = f'{API_BASE_URL}/mediaItems:search'
    body = {
        'filters': {
            'includeArchivedMedia': True,
            'featureFilter': {'includedFeatures': ['FAVORITES']}
        },
        'pageSize': 100
    }

    favorites = []
    page_count = 0

    while True:
        page_count += 1
        logger.info(f"[Google Photos API] POST mediaItems:search (Page {page_count}) - Fetching favorites data...")
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            logger.error(f"[Google Photos API] Error fetching favorites: {response.status_code} - {response.text}")
            break
        data = response.json()
        items = data.get('mediaItems', [])
        favorites.extend(items)

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
        body['pageToken'] = next_page_token

    if favorites:
        months = {}
        for item in favorites:
            c_time = item.get('mediaMetadata', {}).get('creationTime', '')
            if c_time:
                m = c_time[:7]
                months[m] = months.get(m, 0) + 1
        month_summary = ", ".join([f"{m}: {count}" for m, count in sorted(months.items(), reverse=True)])
        logger.info(f"✅ Finished fetching favorites data. Total: {len(favorites)}. Breakdown: {month_summary}")
    else:
        logger.info(f"✅ Finished fetching favorites data. Total: 0")
    return favorites

def check_google_quota(creds=None):
    """
    Retrieves the remaining Google Drive quota using current credentials.
    Returns the usable remaining quota in bytes (int), or 0 if retrieval fails.
    """
    if creds is None:
        # Default to PLANNER_REQUIRED_SCOPES to avoid overwriting a broad token 
        # with a narrow one (Scope Thrashing).
        creds = authenticate(scopes=PLANNER_REQUIRED_SCOPES)

    quota = get_google_storage_quota(creds)
    return quota.get("remaining", 0) if quota is not None else None

def get_google_storage_quota(creds):
    """
    Uses Google Drive API to retrieve storage quota (used, total, remaining in bytes).
    Returns a dict with keys: used, total, remaining (all in bytes).
    """
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        about = drive_service.about().get(fields='storageQuota').execute()
        quota = about.get('storageQuota', {})
        used = int(quota.get('usage', 0))
        total = int(quota.get('limit', 0))
        SAFETY_BUFFER = 100 * 1024 * 1024
        reported_remaining = (total - used) if total > 0 else None
        remaining = (reported_remaining - SAFETY_BUFFER) if reported_remaining is not None else None
        if remaining is not None and remaining < 0:
            remaining = 0
        logger.info(f"Google Storage Quota - Used: {human_readable_size(used)}, Total: {human_readable_size(total)}, Reported Remaining: {human_readable_size(reported_remaining) if reported_remaining is not None else 'Unlimited or unknown'}, Usable Remaining (with buffer): {human_readable_size(remaining) if remaining is not None else 'Unlimited or unknown'}")
        return {"used": used, "total": total, "remaining": remaining}
    except Exception as e:
        logger.error(f"Failed to retrieve Google storage quota: {e}")
        return None

def list_albums(creds):
    """
    Fetches and yields all albums from Google Photos.
    """
    headers = {'Authorization': f'Bearer {creds.token}'}
    url = f'{API_BASE_URL}/albums'
    page_token = None

    while True:
        params = {'pageSize': 50}
        if page_token:
            params['pageToken'] = page_token

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Will raise an exception for 4xx/5xx status
        data = response.json()
        yield from data.get('albums', [])
        page_token = data.get('nextPageToken')
        if not page_token:
            break
