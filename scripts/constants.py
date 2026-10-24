import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_ORGANIZER_DB_PATH = os.path.abspath(os.path.join(BASE_DIR, '../db/media_organizer.db'))
LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/media_organizer.log'))
APPLE_SCRIPT_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/applescript_execution.log'))
CURATION_THRESHOLD_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/curation_threshold_status.log'))
PUBLISHED_MOMENTS_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/published_moments_stats.log'))
UNPROCESSED_MONTHS_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/unprocessed_months.log'))
SCORING_BREAKDOWN_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/qualified_assets_scoring_breakdown.log'))
MEDIA_CLEANUP_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/media_cleanup_recommendations.log'))
QUARTILE_CLEANUP_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/quartile_cleanup_recommendations.log'))
BG_SERVICE_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/bg_copy_db_service.log'))
PUBLISHING_CANDIDATES_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/publishing_candidates.log'))
WEEKLY_MEMORY_LOG_PATH = PUBLISHING_CANDIDATES_LOG_PATH
PUBLISHING_RECOMMENDATIONS_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/publishing_recommendations.log'))
TRIAGE_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/triage_media_intake.log'))
MERGE_LOG_PATH = os.path.abspath(os.path.join(BASE_DIR, '../logs/merge_directories.log'))

# Paths to the Apple Photos database and Media Organizer database
# For safety - during DEV will use a copy of the All-Media DB 
# Copy of the DB:
APPLE_PHOTOS_LIBRARY_PATH = '/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary'
APPLE_PHOTOS_ORIGINALS_PATH = os.path.join(APPLE_PHOTOS_LIBRARY_PATH, 'originals')
APPLE_PHOTOS_DB_PATH = os.path.join(APPLE_PHOTOS_LIBRARY_PATH, 'database/Photos.sqlite')
STAGING_ROOT = '/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports/'
CURATED_EXPORT_DIR = '/Volumes/LaCie/Media Organizer/Google Photos/02-AICurrateList/'
TO_BE_CURATED_DIR = '/Volumes/LaCie/Media Organizer/ToBeCurated/'
CURATED_LACIE_DIR = '/Volumes/LaCie/Media Organizer/Curated/'
MOMENTS_EXPORT_DIR = TO_BE_CURATED_DIR
DEFAULT_MEDIA_PREPROCESSING_DIR = '/Volumes/LaCie/Storage/Priority 4/Media Preprocessing'
TRIAGE_INTAKE_FOLDER_NAME = 'TriageMediaIntake'
TRIAGE_DESTINATION_ROOT = os.path.join(DEFAULT_MEDIA_PREPROCESSING_DIR, TRIAGE_INTAKE_FOLDER_NAME)

# Multi-Drive Triage Roots
# DELL1TB02: Non-backed up cold storage (Priority 3) and disposable duplicates (Priority 4)
DELL_STORAGE_PRIORITY_3_DIR = '/Volumes/DELL1TB02/Storage/Priority 3'
DELL_STORAGE_PREPROCESSING_DIR = '/Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing'
TRIAGE_DELL_DESTINATION_ROOT = os.path.join(DELL_STORAGE_PREPROCESSING_DIR, TRIAGE_INTAKE_FOLDER_NAME)
TRIAGE_DUPLICATES_ROOT = os.path.join(TRIAGE_DELL_DESTINATION_ROOT, 'Duplicates')

# LaCie: Reliable active storage for review-similar and clean new media
TRIAGE_LACIE_DESTINATION_ROOT = TRIAGE_DESTINATION_ROOT
TRIAGE_REVIEW_ROOT = os.path.join(TRIAGE_LACIE_DESTINATION_ROOT, 'Review-Similar')
TRIAGE_NEW_ROOT = os.path.join(TRIAGE_LACIE_DESTINATION_ROOT, 'New')

# Recognized Media Extensions for Triage and Intake Processing
MEDIA_EXTENSIONS = {
    # Still Images & RAW Formats
    '.jpg', '.jpeg', '.png', '.heic', '.tif', '.tiff', '.webp', '.bmp', '.gif',
    '.dng', '.cr2', '.cr3', '.nef', '.arw', '.raw',
    # Video Formats
    '.mov', '.mp4', '.m4v', '.avi', '.mpg', '.mpeg', '.mkv', '.wmv',
    '.mts', '.m2ts', '.vob', '.webm', '.flv', '.3gp', '.mod'
}

APPLE_PHOTOS_DB_COPY_PATH = "/Volumes/Macintosh HD/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite"
APPLE_PHOTOS_DB_MARKER = APPLE_PHOTOS_DB_COPY_PATH + ".lastcopy"
APPLE_PHOTOS_DB_LOCK_PATH = APPLE_PHOTOS_DB_COPY_PATH + ".lock"
BG_SERVICE_PID_PATH = APPLE_PHOTOS_DB_COPY_PATH + ".service.pid"


# Scoring Weights
AESTHETIC_SCORE_WEIGHT = 0.875
GOOGLE_FAVORITES_WEIGHT = 0.125
APPLE_SELECTION_WEIGHT = 0.15
APPLE_FEATURED_WEIGHT = 0.15

# Syncing Settings
MAX_RETRIES = 5
RETRY_DELAY = 30

# Upload Settings
MAX_UPLOAD_FILE_SIZE_MB = 50
MAX_UPLOAD_FILE_SIZE_BYTES = MAX_UPLOAD_FILE_SIZE_MB * 1024 * 1024
SKIPPED_UPLOADS_ALBUM_NAME = "Google Upload Skipped Videos"

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
ACTIVE_CAMERA_MODELS = ['iPhone 13 Pro Max', 'Canon EOS Rebel T7', 'iPhone 16 Pro', 'iPhone 12 Pro Max', 'iPhone 17 Pro Max', 'iPhone 12']

# Device to primary owner mapping to track who physically carries/holds the assets
DEVICE_OWNER_MAPPING = {
    'iPhone 16 Pro': 'Nickolay',
    'iPhone 13 Pro Max': 'Nickolay',
    'iPhone 11 Pro': 'Nickolay',
    'iPhone 8 Plus': 'Nickolay',
    'iPhone 6s': 'Nickolay',
    'iPhone 5s': 'Nickolay',
    'iPhone 4': 'Nickolay',
    
    'iPhone 17 Pro Max': 'Wife',
    'iPhone 12 Pro Max': 'Wife',
    'iPhone 14 Pro Max': 'Wife',
    'iPhone 11 Pro Max': 'Wife',
    'iPhone XS Max': 'Wife',
    
    'iPhone 12': 'Kid',
    'iPhone 13 Pro': 'Kid',
    
    'Canon EOS Rebel T7': 'Shared',
    'Canon EOS 5D Mark IV': 'Shared',
    'Canon EOS 5D': 'Shared',
    'HERO12 Black': 'Shared',
    'HERO7 Silver': 'Shared',
}

# DEPRECATED/UNUSED - Kept for reference, but should be removed in the future.
# GOOGLE_PHOTOS_GENERAL_SCOPES = ["https://www.googleapis.com/auth/photoslibrary.readonly", "https://www.googleapis.com/auth/photoslibrary.appendonly"]
