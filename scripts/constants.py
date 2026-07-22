import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_ORGANIZER_DB_PATH = os.path.join(BASE_DIR, '../db/media_organizer.db')
LOG_PATH = os.path.join(BASE_DIR, '../logs/media_organizer.log')
APPLE_SCRIPT_LOG_PATH = os.path.join(BASE_DIR, '../logs/applescript_execution.log')

# Paths to the Apple Photos database and Media Organizer database
# For safety - during DEV will use a copy of the All-Media DB 
# Copy of the DB:
APPLE_PHOTOS_DB_PATH = '/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite'
STAGING_ROOT = '/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports/'
CURATED_EXPORT_DIR = '/Volumes/LaCie/Media Organizer/Google Photos/02-AICurrateList/'
TO_BE_CURATED_DIR = '/Volumes/LaCie/Media Organizer/ToBeCurated/'
CURATED_LACIE_DIR = '/Volumes/LaCie/Media Organizer/Curated/'
MOMENTS_EXPORT_DIR = TO_BE_CURATED_DIR

APPLE_PHOTOS_DB_COPY_PATH = "/Volumes/Macintosh HD/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite"
APPLE_PHOTOS_DB_MARKER = APPLE_PHOTOS_DB_COPY_PATH + ".lastcopy"

# Scoring Weights
AESTHETIC_SCORE_WEIGHT = 0.875
GOOGLE_FAVORITES_WEIGHT = 0.125
APPLE_SELECTION_WEIGHT = 0.15
APPLE_FEATURED_WEIGHT = 0.15

# Syncing Settings
MAX_RETRIES = 5
RETRY_DELAY = 30

# --- Google API Scopes ---
# https://developers.google.com/photos/overview/authorization
# https://developers.google.com/photos/library/reference/rest/v1/albums/get

# Full access to read, write, and create albums/media. Required for listing all albums and creating new ones.
GOOGLE_PHOTOS_EDIT_ACCESS_SCOPES = ['https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata']

# Read-only access to the entire library. Required for listing all albums and searching all media items (e.g., for favorites).
GOOGLE_PHOTOS_READONLY_SCOPES = ["https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata"]
 
# Append-only access. Allows uploading media but not reading library content.
GOOGLE_PHOTOS_APPEND_ONLY_SCOPES = ["https://www.googleapis.com/auth/photoslibrary.appendonly"]

# Read-only scope for checking storage quota via Drive API
GOOGLE_DRIVE_READ_ONLY_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Combined scopes required for the Pipeline Planner session
PLANNER_REQUIRED_SCOPES = GOOGLE_PHOTOS_EDIT_ACCESS_SCOPES + GOOGLE_PHOTOS_READONLY_SCOPES  + GOOGLE_DRIVE_READ_ONLY_SCOPES + GOOGLE_PHOTOS_APPEND_ONLY_SCOPES

# List of camera models considered "active sources" for import checks
ACTIVE_CAMERA_MODELS = ['iPhone 13 Pro Max', 'Canon EOS Rebel T7', 'iPhone 16 Pro', 'iPhone 12 Pro Max', 'iPhone 17 Pro Max']

# DEPRECATED/UNUSED - Kept for reference, but should be removed in the future.
# GOOGLE_PHOTOS_GENERAL_SCOPES = ["https://www.googleapis.com/auth/photoslibrary.readonly", "https://www.googleapis.com/auth/photoslibrary.appendonly"]
