import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pickle
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import google.auth.exceptions
from googleapiclient.discovery import build
import logging
from utils.logger import setup_logger, compute_file_hash
from constants import LOG_PATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db.connections import get_connection, get_cursor, commit, close as close_conn
from db.queries import get_planned_month


MODULE_TAG = 'google_photos'
logger = setup_logger(LOG_PATH, MODULE_TAG)
for handler in logger.handlers:
    handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))


SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary",
    'https://www.googleapis.com/auth/drive.readonly'
]

#     "https://www.googleapis.com/auth/photoslibrary.readonly"
#     'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',


DRIVE_SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly'
]

TOKEN_PATH = 'token.pickle'
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '../secrets/client_secret.json')
API_BASE_URL = 'https://photoslibrary.googleapis.com/v1'
TOKEN_FILE = 'secrets/token.json'
CLIENT_SECRET_FILE = 'secrets/client_secret.json'
 

# cursor = get_cursor()
# month = get_planned_month(cursor)
# if not month:
#     logger.error("No uploaded batch found.")
#     exit()
# logger.info(f"📤 Starting upload to Google Photos for month: {args.month}")
# for handler in logger.handlers:
#     handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] - %(levelname)s - %(message)s'))

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

def authenticate(scopes=SCOPES, force_refresh=False):
    if force_refresh and os.path.exists(TOKEN_FILE):
         os.remove(TOKEN_FILE)

    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    # Log currently loaded scopes if any
    current_scopes = getattr(creds, "scopes", None) if creds else None
    print("Loaded token scopes:", current_scopes)

    # Force new login if scopes are missing or token invalid
    scopes_ok = creds and set(scopes).issubset(set(getattr(creds, "scopes", [])))
    if not creds or not creds.valid or not scopes_ok:
        if creds and creds.expired and creds.refresh_token and scopes_ok:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, scopes=scopes)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')        # Log granted scopes after flow
        print("Granted scopes after flow:", getattr(creds, "scopes", None))
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    else:
        # Log valid token scopes if no flow needed
        print("Token is valid. Active scopes:", getattr(creds, "scopes", None))

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
    mediaCount = matches[0]['mediaItemsCount']
    if matches:
        if len(matches) > 1:
            print(f"⚠️ Multiple albums found with title '{album_title}'. Returning the first one (ID: {matches[0]['id']}).")
        else:
            print(f"Found existing album '{album_title}' (ID: {matches[0]['id']}). mediaItemsCount: {mediaCount}")
        return matches[0]['id']

    # Create album if not found
    body = {'album': {'title': album_title}}
    response = requests.post(f'{API_BASE_URL}/albums', headers=headers, json=body)
    if response.status_code == 200:
        album_id = response.json()['id']
        print(f"Created new album '{album_title}' (ID: {album_id}).")
        return album_id
    raise Exception(f'Failed to create album: {response.text}')

def get_album_favorites(creds):
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'}
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search'
    #body = {'albumId': album_id, 'pageSize': 100}
    body = {'filters': {'featureFilter': {'includedFeatures': ['FAVORITES']}}, 'pageSize': 100}

    favorites = []
    while True:
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            #logger.error(f"Failed to fetch favorites: {response.text}")
            break
        data = response.json()
        # Only keep items marked as favorite
        for item in data.get('mediaItems', []):
            if item.get('favorite', False):
                favorites.append(item)
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
        body['pageToken'] = next_page_token
    return favorites


def upload_media(creds, file_path, album_id):
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)

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
    #logger.info("📥 Fetching all favorite media items globally...")
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'}
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search'
    body = {'filters': {'featureFilter': {'includedFeatures': ['FAVORITES']}}, 'pageSize': 100}

    favorites = []
    while True:
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            logger.error(f"Failed to fetch favorites: {response.text}")
            break
        data = response.json()
        favorites.extend(data.get('mediaItems', []))
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
        body['pageToken'] = next_page_token

    #logger.info(f"✅ Retrieved {len(favorites)} favorite media items globally.")

    return favorites

def check_google_quota():
    """
    Retrieves the remaining Google Drive quota using current credentials.
    Returns the usable remaining quota in bytes (int), or 0 if retrieval fails.
    """
    creds = ensure_google_photos_credentials(DRIVE_SCOPES)
    quota = get_google_storage_quota(creds)
    return quota.get("remaining", 0) if quota is not None else 0

def ensure_google_photos_credentials(scopes,force_refresh=False):
    creds = None
    if force_refresh and os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except google.auth.exceptions.RefreshError:
                logger.warning("[Google Photos] Token expired or revoked. Removing token and re-authenticating.")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, scopes)
                creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, scopes)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        logger.info(f"[Google Photos] New credentials saved to {TOKEN_FILE}")

    logger.info("[Google Photos] Authentication ready")
    return creds

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
