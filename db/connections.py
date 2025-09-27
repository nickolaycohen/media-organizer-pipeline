# db/connection.py
import sqlite3
from constants import MEDIA_ORGANIZER_DB_PATH

_conn = None

def get_connection():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH, timeout=30, detect_types=sqlite3.PARSE_DECLTYPES)
    return _conn

def get_cursor():
    return get_connection().cursor()

def commit():
    get_connection().commit()

def close():
    global _conn
    if _conn:
        _conn.close()
        _conn = None