import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
import requests

TOKEN_PATH = "token.json"
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.readonly']

def authenticate():
    """Authenticate user and return an AuthorizedSession, with token caching."""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "client_secret.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for next run
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())

    return AuthorizedSession(creds)

def get_file_size(url):
    """Fetch file size from HEAD request."""
    try:
        r = requests.head(url, timeout=10)
        return int(r.headers.get('Content-Length', 0))
    except Exception:
        return 0

def main():
    authed_session = authenticate()
    all_photos = []
    next_page_token = None

    print("Fetching media items from Google Photos...")

    while True:
        url = "https://photoslibrary.googleapis.com/v1/mediaItems"
        params = {"pageSize": 100}
        if next_page_token:
            params["pageToken"] = next_page_token

        response = authed_session.get(url, params=params).json()
        print(response)  # DEBUG
        items = response.get("mediaItems", [])

        for item in items:
            url = item['baseUrl'] + "=d"  # download URL
            size = get_file_size(url)
            all_photos.append({
                "filename": item.get("filename"),
                "id": item["id"],
                "size": size
            })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    print(f"Total items checked: {len(all_photos)}")

    # Sort by size
    largest = sorted(all_photos, key=lambda x: x["size"], reverse=True)[:20]

    print("\nTop 20 Largest Photos:")
    for p in largest:
        print(f"{p['filename']} - {p['size']/1024/1024:.2f} MB")

if __name__ == '__main__':
    main()