import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_ORGANIZER_DB_PATH = os.path.join(BASE_DIR, '../db/media_organizer.db')
LOG_PATH = os.path.join(BASE_DIR, '../logs/media_organizer.log')

# Paths to the Apple Photos database and Media Organizer database
# For safety - during DEV will use a copy of the All-Media DB 
# Copy of the DB:
APPLE_PHOTOS_DB_PATH = '/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite'
STAGING_ROOT = '/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports/'
CURATED_EXPORT_DIR = '/Volumes/LaCie/Media Organizer/Google Photos/02-AICurrateList/'

APPLE_PHOTOS_DB_COPY_PATH = "/Volumes/Macintosh HD/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite"
APPLE_PHOTOS_DB_MARKER = APPLE_PHOTOS_DB_COPY_PATH + ".lastcopy"

# --- Google API Scopes ---
# https://developers.google.com/photos/overview/authorization
# https://developers.google.com/photos/library/reference/rest/v1/albums/get

# General purpose scopes for reading albums, creating albums, and reading content
GOOGLE_PHOTOS_GENERAL_SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary.readonly",
    "https://www.googleapis.com/auth/photoslibrary.appendonly"
]

# Scopes for uploading and managing app-created data
GOOGLE_PHOTOS_UPLOAD_SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
    "https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata",
    "https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata"
]
GOOGLE_PHOTOS_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata"
] 

# Scopes for creating albums and uploading media
GOOGLE_PHOTOS_CREATE_AND_UPLOAD_SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary.readonly", # For listing albums
    "https://www.googleapis.com/auth/photoslibrary.appendonly" # For uploading
]

# Read-only scope for checking storage quota via Drive API
GOOGLE_DRIVE_READ_ONLY_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Edit Access only
GOOGLE_PHOTOS_EDIT_ACCESS_SCOPES = ['https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata']
