import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_ORGANIZER_DB_PATH = os.path.join(BASE_DIR, '../db/media_organizer.db')
LOG_PATH = os.path.join(BASE_DIR, '../logs/media_organizer.log')

# Paths to the Apple Photos database and Media Organizer database
# For safety - during DEV will use a copy of the All-Media DB 
# Copy of the DB:
APPLE_PHOTOS_DB_PATH = '/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite'
STAGING_ROOT = '/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports/'
CURATED_EXPORT_DIR = '/Volumes/LaCie/Media Organizer/Google Photos/02-AICurrateList/'


