import os
import pickle
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata',
]

TOKEN_PATH = 'token.pickle'
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '../secrets/client_secret.json')
API_BASE_URL = 'https://photoslibrary.googleapis.com/v1'

def authenticate():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    # Log currently loaded scopes if any
    current_scopes = getattr(creds, "scopes", None) if creds else None
    print("Loaded token scopes:", current_scopes)

    # Force new login if scopes are missing or token invalid
    scopes_ok = creds and set(SCOPES).issubset(set(getattr(creds, "scopes", [])))
    if not creds or not creds.valid or not scopes_ok:
        if creds and creds.expired and creds.refresh_token and scopes_ok:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
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
    # Search for album
    response = requests.get(f'{API_BASE_URL}/albums', headers=headers)
    if response.status_code == 200:
        albums = response.json().get('albums', [])
        for album in albums:
            if album['title'] == album_title:
                return album['id']
    # Create album if not found
    body = {'album': {'title': album_title}}
    response = requests.post(f'{API_BASE_URL}/albums', headers=headers, json=body)
    if response.status_code == 200:
        return response.json()['id']
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
