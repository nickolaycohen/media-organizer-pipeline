import os
import sys
import shutil
import subprocess
import sqlite3
import argparse
import time
import re
import bisect
from datetime import datetime, timedelta

# Add project root and script dir to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.dirname(__file__))

from constants import (
    MEDIA_ORGANIZER_DB_PATH,
    TRIAGE_LOG_PATH,
    DEFAULT_MEDIA_PREPROCESSING_DIR,
    TRIAGE_INTAKE_FOLDER_NAME,
    TRIAGE_DESTINATION_ROOT,
    TRIAGE_LACIE_DESTINATION_ROOT,
    TRIAGE_DELL_DESTINATION_ROOT,
    APPLE_PHOTOS_ORIGINALS_PATH,
    MEDIA_EXTENSIONS
)
from utils.logger import setup_logger
from utils.finder_tags import apply_finder_tag

import warnings
import logging

try:
    import exifread
    HAS_EXIFREAD = True
    logging.getLogger("exifread").setLevel(logging.ERROR)
except ImportError:
    HAS_EXIFREAD = False

try:
    from PIL import Image
    import imagehash
    HAS_IMAGEHASH = True
    Image.MAX_IMAGE_PIXELS = None
    warnings.filterwarnings("ignore", category=Image.DecompressionBombWarning)
    warnings.filterwarnings("ignore", category=UserWarning, module="PIL")
except ImportError:
    HAS_IMAGEHASH = False

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    HAS_HEIF = True
except ImportError:
    HAS_HEIF = False

MODULE_TAG = "triage_media_intake"
logger = setup_logger(TRIAGE_LOG_PATH, MODULE_TAG)


IMAGE_EXTENSIONS_FOR_HASH = {
    '.jpg', '.jpeg', '.png', '.heic', '.tif', '.tiff', '.webp', '.bmp', '.dng'
}

DISCARD_FOLDER_KEYWORDS = {
    'delete', 'deleted', 'trash', 'discard', 'discarded',
    'junk', 'to_delete', 'todelete', 'remove', 'removed',
    'to_remove', 'bad', 'reject', 'rejected', 'temp_delete'
}

WEB_ARTIFACT_EXTS = {'.gif', '.png', '.ico', '.bmp', '.svg', '.webp'}
WEB_ARTIFACT_MAX_SIZE = 50 * 1024  # 50 KB

SEQ_REGEX = re.compile(r'^([a-zA-Z_-]+)(\d+)\.')

TRIAGE_FOLDER_TAGS = {
    "Duplicates": "Red",
    "Review-Similar": "Yellow",
    "Review-Similar/Upgrade-Candidates": "Orange",
    "New": "Green"
}

def ensure_triage_tables(conn):
    """Ensures triage_sessions and triage_session_assets tables exist in the DB with all required columns."""
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS triage_sessions (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            triage_id                   TEXT UNIQUE NOT NULL,
            source_directory            TEXT NOT NULL,
            destination_directory       TEXT NOT NULL,
            status                      TEXT NOT NULL DEFAULT 'running',
            total_files_scanned         INTEGER DEFAULT 0,
            duplicates_count            INTEGER DEFAULT 0,
            duplicates_bytes            INTEGER DEFAULT 0,
            compressed_duplicates_count INTEGER DEFAULT 0,
            compressed_duplicates_bytes INTEGER DEFAULT 0,
            review_similar_count        INTEGER DEFAULT 0,
            review_similar_bytes        INTEGER DEFAULT 0,
            upgrade_candidates_count    INTEGER DEFAULT 0,
            upgrade_candidates_bytes    INTEGER DEFAULT 0,
            discard_sim_count           INTEGER DEFAULT 0,
            discard_sim_bytes           INTEGER DEFAULT 0,
            web_artifact_count          INTEGER DEFAULT 0,
            web_artifact_bytes          INTEGER DEFAULT 0,
            new_count                   INTEGER DEFAULT 0,
            new_bytes                   INTEGER DEFAULT 0,
            dry_run                     INTEGER DEFAULT 0,
            started_at_utc              TEXT DEFAULT (datetime('now')),
            completed_at_utc            TEXT,
            visual_duplicate_count      INTEGER DEFAULT 0,
            moment_matched_count        INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS triage_session_assets (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            triage_session_id           INTEGER NOT NULL REFERENCES triage_sessions(id),
            triage_id                   TEXT NOT NULL,
            original_rel_path           TEXT NOT NULL,
            filename                    TEXT NOT NULL,
            file_size_bytes             INTEGER NOT NULL,
            category                    TEXT NOT NULL,
            sub_type                    TEXT,
            decision_reason             TEXT,
            destination_path            TEXT,
            created_at_utc              TEXT DEFAULT (datetime('now')),
            matched_asset_uuid          TEXT,
            matched_moment_name         TEXT,
            visual_similarity_score     REAL,
            hamming_distance            INTEGER,
            perceptual_hash             TEXT
        )
    """)

    # Safety migrations in case tables were previously created without these columns
    c.execute("PRAGMA table_info(triage_sessions)")
    ts_cols = {row[1] for row in c.fetchall()}
    if "visual_duplicate_count" not in ts_cols:
        c.execute("ALTER TABLE triage_sessions ADD COLUMN visual_duplicate_count INTEGER DEFAULT 0")
    if "moment_matched_count" not in ts_cols:
        c.execute("ALTER TABLE triage_sessions ADD COLUMN moment_matched_count INTEGER DEFAULT 0")

    c.execute("PRAGMA table_info(triage_session_assets)")
    tsa_cols = {row[1] for row in c.fetchall()}
    if "matched_asset_uuid" not in tsa_cols:
        c.execute("ALTER TABLE triage_session_assets ADD COLUMN matched_asset_uuid TEXT")
    if "matched_moment_name" not in tsa_cols:
        c.execute("ALTER TABLE triage_session_assets ADD COLUMN matched_moment_name TEXT")
    if "visual_similarity_score" not in tsa_cols:
        c.execute("ALTER TABLE triage_session_assets ADD COLUMN visual_similarity_score REAL")
    if "hamming_distance" not in tsa_cols:
        c.execute("ALTER TABLE triage_session_assets ADD COLUMN hamming_distance INTEGER")
    if "perceptual_hash" not in tsa_cols:
        c.execute("ALTER TABLE triage_session_assets ADD COLUMN perceptual_hash TEXT")

    conn.commit()

def determine_triage_id(db_path, lacie_dest_root=None, dell_dest_root=None, requested_id=None):
    """
    Determines the triage session ID.
    If requested_id is provided, uses it.
    Otherwise queries MAX(id) from DB and avoids collisions with existing folders across DELL1TB02 and LaCie.
    """
    if requested_id:
        return str(requested_id).strip()

    lacie_dest_root = lacie_dest_root or TRIAGE_LACIE_DESTINATION_ROOT
    dell_dest_root = dell_dest_root or TRIAGE_DELL_DESTINATION_ROOT

    next_num = 1
    try:
        conn = sqlite3.connect(db_path)
        ensure_triage_tables(conn)
        c = conn.cursor()
        c.execute("SELECT COALESCE(MAX(id), 0) FROM triage_sessions")
        row = c.fetchone()
        if row and row[0]:
            next_num = row[0] + 1
        conn.close()
    except Exception as e:
        logger.warning(f"Could not determine next triage_id from DB: {e}")

    # Ensure no folder collision across DELL1TB02 (Duplicates) and LaCie (Review-Similar, New)
    def session_exists(num):
        s_id = str(num)
        if os.path.exists(os.path.join(dell_dest_root, "Duplicates", s_id)):
            return True
        for cat in ["Review-Similar", "New"]:
            if os.path.exists(os.path.join(lacie_dest_root, cat, s_id)):
                return True
        if os.path.exists(os.path.join(lacie_dest_root, "Duplicates", s_id)):
            return True
        return False

    while session_exists(next_num):
        next_num += 1

    return str(next_num)

def tag_triage_folders(lacie_dest_root=None, dell_dest_root=None, triage_id=None):
    """
    Applies standard macOS Finder color tags to triage folders across both drives:
      - DELL1TB02:
          Duplicates/                       -> Red (Index 2)
          Duplicates/<triage_id>            -> Red (Index 2)
      - LaCie:
          Review-Similar/                  -> Yellow (Index 3)
          Review-Similar/<triage_id>       -> Yellow (Index 3)
          Review-Similar/.../Upgrade-Candidates -> Orange (Index 1)
          New/                             -> Green (Index 6)
          New/<triage_id>                  -> Green (Index 6)
    """
    lacie_dest_root = os.path.abspath(lacie_dest_root or TRIAGE_LACIE_DESTINATION_ROOT)
    dell_dest_root = os.path.abspath(dell_dest_root or TRIAGE_DELL_DESTINATION_ROOT)
    results = {}

    folder_mapping = {
        "Duplicates": (dell_dest_root, "Red"),
        "Review-Similar": (lacie_dest_root, "Yellow"),
        "New": (lacie_dest_root, "Green")
    }

    # 1. Tag top-level category folders if they exist
    for cat_name, (base_root, color) in folder_mapping.items():
        cat_path = os.path.join(base_root, cat_name)
        if os.path.exists(cat_path):
            ok = apply_finder_tag(cat_path, color)
            drive_label = "DELL1TB02" if "DELL1TB02" in base_root else "LaCie"
            results[f"{drive_label}: {cat_name}"] = (color, ok)
            logger.info(f"🏷️  Tagged top-level '{cat_path}' as {color} in Finder.")

    # 2. Tag session subfolders (e.g. Duplicates/<id>, Review-Similar/<id>, New/<id>)
    for cat_name, (base_root, color) in folder_mapping.items():
        cat_path = os.path.join(base_root, cat_name)
        if not os.path.exists(cat_path):
            continue

        session_dirs = [triage_id] if triage_id else sorted(os.listdir(cat_path))
        for s_dir in session_dirs:
            if not s_dir or str(s_dir).startswith('.'):
                continue
            sess_path = os.path.join(cat_path, str(s_dir))
            if os.path.isdir(sess_path):
                drive_label = "DELL1TB02" if "DELL1TB02" in base_root else "LaCie"
                key = f"{drive_label}: {cat_name}/{s_dir}"
                ok = apply_finder_tag(sess_path, color)
                results[key] = (color, ok)
                logger.info(f"🏷️  Tagged session folder '{sess_path}' as {color} in Finder.")

                # If Upgrade-Candidates exists inside Review-Similar/<id>, tag it Orange
                upg_path = os.path.join(sess_path, "Upgrade-Candidates")
                if os.path.exists(upg_path):
                    upg_key = f"{drive_label}: {cat_name}/{s_dir}/Upgrade-Candidates"
                    upg_ok = apply_finder_tag(upg_path, "Orange")
                    results[upg_key] = ("Orange", upg_ok)
                    logger.info(f"🏷️  Tagged '{upg_path}' as Orange in Finder.")

    return results

def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

def extract_exif_data(file_path):
    """
    Extracts (date_ymd, unix_timestamp) from file EXIF if available.
    """
    if not HAS_EXIFREAD:
        return None, None
    try:
        with open(file_path, 'rb') as f:
            tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)
            dt = tags.get('EXIF DateTimeOriginal') or tags.get('Image DateTime')
            if dt:
                val = str(dt).strip()
                if len(val) >= 19:
                    # Format: "YYYY:MM:DD HH:MM:SS"
                    date_part = val[:10].replace(':', '-')
                    time_part = val[11:19]
                    dt_str = f"{date_part} {time_part}"
                    try:
                        parsed = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        return date_part, parsed.timestamp()
                    except ValueError:
                        return date_part, None
                elif len(val) >= 10:
                    return val[:10].replace(':', '-'), None
    except Exception:
        pass
    return None, None

def compute_phash(file_path):
    """
    Computes perceptual hash (64-bit pHash) of an image file using Pillow and imagehash.
    Returns (imagehash.ImageHash, hex_str) or (None, None).
    """
    if not HAS_IMAGEHASH:
        return None, None
    try:
        with Image.open(file_path) as img:
            h = imagehash.phash(img)
            return h, str(h)
    except Exception:
        return None, None

class LibraryIndex:
    def __init__(self, db_path, originals_dir=None):
        self.db_path = db_path
        self.originals_dir = originals_dir or APPLE_PHOTOS_ORIGINALS_PATH
        self.has_originals = os.path.isdir(self.originals_dir)
        self.size_name_map = {}
        self.size_date_map = {}
        self.size_map = {}           # sz -> list of entries (for exact byte size match on UUID/library assets)
        self.name_date_map = {}      # (filename_lower, ymd) -> list of entries
        self.date_seq_map = {}       # date_ymd -> set of (prefix_lower, seq_num)
        self.date_ts_map = {}        # date_ymd -> sorted list of unix_ts
        self.date_assets_map = {}    # date_ymd -> list of entries (for visual candidate lookup)
        self.moment_assets_map = {}  # moment_name.lower() -> list of entries
        self.date_moments_map = {}   # date_ymd -> set of moment names
        self.lib_hash_cache = {}     # uuid -> (imagehash.ImageHash, hex_str)
        self.removed_assets_set = set() # (name_lower) of assets marked removed_from_source
        self.total_assets = 0

    def load(self):
        t0 = time.time()
        logger.info(f"📚 Loading All-Media library catalog from {self.db_path}...")
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Query ZASSET, ZADDITIONALASSETATTRIBUTES, and assets table for MomentsAlbumName
        query = """
            SELECT 
                aaa.ZORIGINALFILESIZE, 
                aaa.ZORIGINALFILENAME, 
                a.ZFILENAME, 
                a.ZUUID, 
                a.ZDIRECTORY,
                datetime(a.ZDATECREATED + 978307200, 'unixepoch') AS date_created,
                (a.ZDATECREATED + 978307200) AS unix_ts,
                ast.MomentsAlbumName
            FROM ZASSET a 
            JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
            LEFT JOIN assets ast ON ast.asset_id = a.ZUUID
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.total_assets = len(rows)

        for sz, orig_name, cur_name, uuid, zdir, dt, unix_ts, moment_name in rows:
            entry = {
                "uuid": uuid,
                "original_filename": orig_name or cur_name,
                "cur_filename": cur_name,
                "directory": zdir,
                "date_created": dt,
                "unix_ts": unix_ts,
                "sz": sz or 0,
                "moment_name": moment_name
            }
            fname = orig_name or cur_name or ''

            if sz:
                self.size_map.setdefault(sz, []).append(entry)
                if orig_name:
                    self.size_name_map[(sz, orig_name.lower())] = entry
                if cur_name and cur_name != orig_name:
                    self.size_name_map[(sz, cur_name.lower())] = entry
                if dt and len(dt) >= 10:
                    dt_ymd = dt[:10]
                    self.size_date_map[(sz, dt_ymd)] = entry

            # Name + Date map for Quality / Size comparison
            if dt and len(dt) >= 10 and fname and sz:
                dt_ymd = dt[:10]
                if orig_name:
                    k1 = (orig_name.lower(), dt_ymd)
                    self.name_date_map.setdefault(k1, []).append(entry)
                if cur_name and cur_name != orig_name:
                    k2 = (cur_name.lower(), dt_ymd)
                    self.name_date_map.setdefault(k2, []).append(entry)

            # Date -> Assets map & Moment index for scoped visual candidates
            if dt and len(dt) >= 10:
                dt_ymd = dt[:10]
                self.date_assets_map.setdefault(dt_ymd, []).append(entry)
                if moment_name:
                    self.date_moments_map.setdefault(dt_ymd, set()).add(moment_name)
                    self.moment_assets_map.setdefault(moment_name.lower(), []).append(entry)

            # Sequence parsing
            if dt and len(dt) >= 10 and fname:
                ymd = dt[:10]
                m = SEQ_REGEX.match(fname)
                if m:
                    pfx = m.group(1).lower()
                    snum = int(m.group(2))
                    if ymd not in self.date_seq_map:
                        self.date_seq_map[ymd] = set()
                    self.date_seq_map[ymd].add((pfx, snum))

                if unix_ts:
                    if ymd not in self.date_ts_map:
                        self.date_ts_map[ymd] = []
                    self.date_ts_map[ymd].append(unix_ts)

        for ymd in self.date_ts_map:
            self.date_ts_map[ymd].sort()

        # Check assets table for assets previously marked removed_from_source = 1
        try:
            cursor.execute("SELECT original_filename, date_created_utc FROM assets WHERE removed_from_source = 1")
            for r_name, r_dt in cursor.fetchall():
                if r_name:
                    self.removed_assets_set.add(r_name.lower())
        except sqlite3.OperationalError:
            pass

        conn.close()
        load_time = time.time() - t0
        logger.info(f"✅ Loaded {self.total_assets:,} library assets in {load_time:.2f}s.")
        logger.info(f"   - Size+Name Index: {len(self.size_name_map):,} keys")
        logger.info(f"   - Name+Date Index: {len(self.name_date_map):,} keys")
        logger.info(f"   - Date Clusters Indexed: {len(self.date_assets_map):,} dates")
        logger.info(f"   - Apple Photos Moments Indexed: {len(self.moment_assets_map):,} moments")
        if self.has_originals:
            logger.info(f"   - Apple Photos Originals Root: {self.originals_dir}")
        else:
            logger.warning(f"   - ⚠️ Apple Photos Originals Root not found at: {self.originals_dir} (visual hashing disabled)")

    def get_library_asset_hash(self, entry):
        """
        Retrieves or computes the perceptual hash for a library asset.
        Uses in-memory cache to ensure each library asset is hashed at most once.
        """
        uuid = entry.get("uuid")
        if uuid in self.lib_hash_cache:
            return self.lib_hash_cache[uuid]

        if not self.has_originals:
            return None, None

        zdir = entry.get("directory")
        curname = entry.get("cur_filename")
        if not zdir or not curname:
            return None, None

        lib_file_path = os.path.join(self.originals_dir, zdir, curname)
        if not os.path.exists(lib_file_path):
            self.lib_hash_cache[uuid] = (None, None)
            return None, None

        h_obj, h_str = compute_phash(lib_file_path)
        self.lib_hash_cache[uuid] = (h_obj, h_str)
        return h_obj, h_str

    def evaluate_asset(self, file_path, rel_path, file_size, filename, is_photos_library=False):
        """
        Multi-tier quality, duplicate & visual similarity evaluation:
        Returns (category, sub_type, reason, matched_entry, visual_meta)
        Categories: 'duplicate', 'review_similar', 'new'
        """
        fname_lower = filename.lower()
        rel_lower = rel_path.lower()
        ext = os.path.splitext(fname_lower)[1]

        visual_meta = {
            "matched_asset_uuid": None,
            "matched_moment_name": None,
            "visual_similarity_score": None,
            "hamming_distance": None,
            "perceptual_hash": None
        }

        # Extract capture date / timestamp from EXIF or fallback to path/filename
        exif_ymd, exif_ts = extract_exif_data(file_path)
        if not exif_ymd:
            date_match = re.search(r'20\d{2}[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])', rel_path)
            if date_match:
                exif_ymd = date_match.group(0).replace('_', '-')
            else:
                date_text_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})', rel_path, re.IGNORECASE)
                if date_text_match:
                    try:
                        m_str, d_str, y_str = date_text_match.groups()
                        dt_parsed = datetime.strptime(f"{m_str} {d_str} {y_str}", "%B %d %Y")
                        exif_ymd = dt_parsed.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        # Determine candidate dates (handling UTC vs local time zone shifts of +-1 day)
        candidate_dates = [exif_ymd] if exif_ymd else []
        if exif_ymd:
            try:
                dt_base = datetime.strptime(exif_ymd, "%Y-%m-%d")
                candidate_dates.append((dt_base + timedelta(days=1)).strftime("%Y-%m-%d"))
                candidate_dates.append((dt_base - timedelta(days=1)).strftime("%Y-%m-%d"))
            except ValueError:
                pass

        # Check if capture date aligns with any Apple Photos Moment
        if candidate_dates:
            for d_chk in candidate_dates:
                if d_chk in self.date_moments_map:
                    visual_meta["matched_moment_name"] = sorted(self.date_moments_map[d_chk])[0]
                    break

        # Check if intake file is a hashable image format
        is_image = ext in IMAGE_EXTENSIONS_FOR_HASH
        _h_state = {"computed": False, "obj": None, "str": None}

        def get_intake_hash():
            if not _h_state["computed"]:
                _h_state["computed"] = True
                if is_image and HAS_IMAGEHASH:
                    h_obj, h_str = compute_phash(file_path)
                    _h_state["obj"] = h_obj
                    _h_state["str"] = h_str
                    if h_str:
                        visual_meta["perceptual_hash"] = h_str
            return _h_state["obj"], _h_state["str"]

        # ---------------------------------------------------------
        # TIER 1: Exact Byte-Level Duplicates
        # ---------------------------------------------------------
        key_name = (file_size, fname_lower)
        if key_name in self.size_name_map:
            matched = self.size_name_map[key_name]
            visual_meta["matched_asset_uuid"] = matched["uuid"]
            if matched.get("moment_name"):
                visual_meta["matched_moment_name"] = matched["moment_name"]
            visual_meta["visual_similarity_score"] = 100.0
            visual_meta["hamming_distance"] = 0
            reason = f"Exact byte match ({format_size(file_size)}) and filename ({matched['original_filename']}) match All-Media asset {matched['uuid']}"
            if visual_meta.get("matched_moment_name"):
                reason += f" [Moment: {visual_meta['matched_moment_name']}]"
            return "duplicate", "identical", reason, matched, visual_meta

        if candidate_dates:
            for d_chk in candidate_dates:
                key_date = (file_size, d_chk)
                if key_date in self.size_date_map:
                    matched = self.size_date_map[key_date]
                    visual_meta["matched_asset_uuid"] = matched["uuid"]
                    if matched.get("moment_name"):
                        visual_meta["matched_moment_name"] = matched["moment_name"]
                    visual_meta["visual_similarity_score"] = 100.0
                    visual_meta["hamming_distance"] = 0
                    reason = f"Exact byte match ({format_size(file_size)}) and date ({d_chk}) match All-Media asset {matched['uuid']} ({matched['original_filename']})"
                    if visual_meta.get("matched_moment_name"):
                        reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                    return "duplicate", "identical", reason, matched, visual_meta

        # TIER 1C: Standalone exact byte-size duplicate (Photos Library UUID intake or exact size match)
        if (is_photos_library or re.match(r'^[0-9a-fA-F-]{36}', filename)) and file_size in self.size_map:
            candidates = self.size_map[file_size]
            matched = candidates[0]
            if candidate_dates:
                for c in candidates:
                    c_dt = c.get("date_created") or ""
                    if any(c_dt.startswith(d) for d in candidate_dates):
                        matched = c
                        break
            visual_meta["matched_asset_uuid"] = matched["uuid"]
            if matched.get("moment_name"):
                visual_meta["matched_moment_name"] = matched["moment_name"]
            visual_meta["visual_similarity_score"] = 100.0
            visual_meta["hamming_distance"] = 0
            reason = f"Exact byte match ({format_size(file_size)}) matches All-Media asset {matched['uuid']} ({matched['original_filename']})"
            if visual_meta.get("matched_moment_name"):
                reason += f" [Moment: {visual_meta['matched_moment_name']}]"
            return "duplicate", "identical", reason, matched, visual_meta

        # ---------------------------------------------------------
        # TIER 2: Quality & Size Comparison Against Same Photo
        # (Same filename & date, but different byte size)
        # ---------------------------------------------------------
        if candidate_dates:
            matching_lib_entries = None
            for d_chk in candidate_dates:
                if (fname_lower, d_chk) in self.name_date_map:
                    matching_lib_entries = self.name_date_map[(fname_lower, d_chk)]
                    break

            if matching_lib_entries:
                lib_entry = matching_lib_entries[0]
                lib_sz = lib_entry["sz"]
                visual_meta["matched_asset_uuid"] = lib_entry["uuid"]
                if lib_entry.get("moment_name"):
                    visual_meta["matched_moment_name"] = lib_entry["moment_name"]

                # Compute visual similarity against library entry if possible
                if self.has_originals:
                    intake_h_obj, _ = get_intake_hash()
                    if intake_h_obj:
                        lib_h_obj, lib_h_str = self.get_library_asset_hash(lib_entry)
                        if lib_h_obj:
                            dist = intake_h_obj - lib_h_obj
                            sim = max(0.0, 1.0 - (dist / 64.0)) * 100.0
                            visual_meta["hamming_distance"] = dist
                            visual_meta["visual_similarity_score"] = round(sim, 2)
                if visual_meta["visual_similarity_score"] is None:
                    visual_meta["visual_similarity_score"] = 100.0
                    visual_meta["hamming_distance"] = 0

                if lib_sz > 0:
                    diff = file_size - lib_sz
                    pct = (diff / lib_sz) * 100

                    if file_size <= lib_sz * 1.02:
                        # Intake file is smaller or essentially equal -> Library holds superior master!
                        reason = f"Compressed duplicate of All-Media asset {lib_entry['uuid']} ({format_size(file_size)} vs {format_size(lib_sz)} in library, {pct:.1f}%; Library is superior)"
                        if visual_meta.get("matched_moment_name"):
                            reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                        return "duplicate", "compressed", reason, lib_entry, visual_meta
                    else:
                        # Intake file is significantly larger (>2%) -> Potential Upgrade Candidate!
                        reason = f"🌟 Potential Upgrade: Incoming file is larger/higher quality ({format_size(file_size)} vs {format_size(lib_sz)} in All-Media, +{pct:.1f}%; Asset: {lib_entry['uuid']})"
                        if visual_meta.get("matched_moment_name"):
                            reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                        return "review_similar", "upgrade_candidate", reason, lib_entry, visual_meta

        # ---------------------------------------------------------
        # TIER 2.5: Scoped Visual Similarity & Perceptual Hashing
        # (Detects duplicates or near-duplicates across different filenames)
        # ---------------------------------------------------------
        if is_image and self.has_originals:
            candidates = []
            if candidate_dates:
                for d_chk in candidate_dates:
                    if d_chk in self.date_assets_map:
                        for entry in self.date_assets_map[d_chk]:
                            if entry not in candidates:
                                candidates.append(entry)

            # Also check if any Moment name is mentioned in the path
            for m_lower, m_entries in self.moment_assets_map.items():
                if len(m_lower) > 3 and m_lower in rel_lower:
                    for me in m_entries:
                        if me not in candidates:
                            candidates.append(me)

            if candidates:
                # Filter candidates to image extensions
                cand_images = [
                    c for c in candidates 
                    if os.path.splitext(c.get("cur_filename", "").lower())[1] in IMAGE_EXTENSIONS_FOR_HASH
                    or os.path.splitext(c.get("original_filename", "").lower())[1] in IMAGE_EXTENSIONS_FOR_HASH
                ]

                if cand_images:
                    intake_h_obj, _ = get_intake_hash()
                    if intake_h_obj:
                        # If timestamp available, sort candidates by time proximity
                        if exif_ts and len(cand_images) > 1:
                            cand_images.sort(key=lambda c: abs((c.get("unix_ts") or 0) - exif_ts))

                        best_match = None
                        min_dist = None
                        best_sim = None

                        # Test candidates (limit to closest 50 if huge set)
                        for cand in cand_images[:50]:
                            cand_h_obj, cand_h_str = self.get_library_asset_hash(cand)
                            if not cand_h_obj:
                                continue
                            dist = intake_h_obj - cand_h_obj
                            sim = max(0.0, 1.0 - (dist / 64.0)) * 100.0

                            if min_dist is None or dist < min_dist:
                                min_dist = dist
                                best_match = cand
                                best_sim = sim

                            # Exact visual match found, stop checking
                            if dist == 0:
                                break
                            # High confidence match found, break early
                            if dist <= 3:
                                break

                        if min_dist is not None:
                            if min_dist <= 3:
                                # High-Confidence Visual Duplicate (>=95.3% match)
                                lib_sz = best_match.get("sz", 0)
                                visual_meta["matched_asset_uuid"] = best_match["uuid"]
                                if best_match.get("moment_name"):
                                    visual_meta["matched_moment_name"] = best_match["moment_name"]
                                visual_meta["visual_similarity_score"] = round(best_sim, 2)
                                visual_meta["hamming_distance"] = min_dist

                                if file_size <= lib_sz * 1.02:
                                    # Library is superior or equal
                                    pct = ((file_size - lib_sz) / lib_sz * 100) if lib_sz else 0
                                    reason = f"Visual duplicate of All-Media asset {best_match['uuid']} ({best_match['original_filename']}) - {best_sim:.1f}% match (dist {min_dist}), {format_size(file_size)} vs {format_size(lib_sz)} in library ({pct:+.1f}%); Library is superior"
                                    if visual_meta.get("matched_moment_name"):
                                        reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                                    return "duplicate", "visual_duplicate", reason, best_match, visual_meta
                                else:
                                    # Intake file is higher quality / larger!
                                    pct = ((file_size - lib_sz) / lib_sz * 100) if lib_sz else 0
                                    reason = f"🌟 Visual Upgrade Candidate: Higher quality ({format_size(file_size)} vs {format_size(lib_sz)} in library, +{pct:.1f}%) matching asset {best_match['uuid']} ({best_match['original_filename']}) with {best_sim:.1f}% visual match (dist {min_dist})"
                                    if visual_meta.get("matched_moment_name"):
                                        reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                                    return "review_similar", "upgrade_candidate", reason, best_match, visual_meta

                            elif 4 <= min_dist <= 8:
                                # Visually Similar Photo / Burst / Moment Sibling (87.5% - 93.8% match)
                                visual_meta["matched_asset_uuid"] = best_match["uuid"]
                                if best_match.get("moment_name"):
                                    visual_meta["matched_moment_name"] = best_match["moment_name"]
                                visual_meta["visual_similarity_score"] = round(best_sim, 2)
                                visual_meta["hamming_distance"] = min_dist
                                reason = f"Visually similar photo to All-Media asset {best_match['uuid']} ({best_match['original_filename']}) - {best_sim:.1f}% match (dist {min_dist})"
                                if visual_meta.get("matched_moment_name"):
                                    reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                                return "review_similar", "visual_similar", reason, best_match, visual_meta

        # ---------------------------------------------------------
        # TIER 3: Suspected Discards, Burst Siblings, or Web Junk
        # ---------------------------------------------------------
        # 3A: Path Intent Keywords
        path_segments = [seg.strip().lower() for seg in rel_lower.replace('\\', '/').split('/')]
        for seg in path_segments[:-1]:
            for kw in DISCARD_FOLDER_KEYWORDS:
                if kw in seg:
                    reason = f"Located inside folder marked for deletion ('{seg}')"
                    return "review_similar", "discard_folder", reason, None, visual_meta

        # 3B: Previously removed from source
        if fname_lower in self.removed_assets_set:
            reason = "Matches an asset previously removed from Apple Photos library (marked removed_from_source in DB)"
            return "review_similar", "previously_removed", reason, None, visual_meta

        # 3C: Web graphic / UI snippet
        if ext in WEB_ARTIFACT_EXTS and file_size < WEB_ARTIFACT_MAX_SIZE:
            reason = f"Web graphic / UI snippet artifact ({ext}, {format_size(file_size)})"
            return "review_similar", "web_artifact", reason, None, visual_meta

        # 3D: Sequence burst sibling on same date
        if exif_ymd and exif_ymd in self.date_seq_map:
            m = SEQ_REGEX.match(filename)
            if m:
                pfx = m.group(1).lower()
                snum = int(m.group(2))
                existing_nums = [n for p, n in self.date_seq_map[exif_ymd] if p == pfx]
                if existing_nums:
                    min_diff = min(abs(n - snum) for n in existing_nums)
                    if min_diff <= 15:
                        closest_num = min(existing_nums, key=lambda n: abs(n - snum))
                        reason = f"Sequence burst sibling on {exif_ymd} ({pfx}{snum} is {min_diff} shots away from existing library asset {pfx}{closest_num})"
                        if visual_meta.get("matched_moment_name"):
                            reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                        return "review_similar", "sequence_burst", reason, None, visual_meta

        # 3E: Temporal burst proximity within 30 seconds
        if exif_ymd and exif_ts and exif_ymd in self.date_ts_map:
            ts_list = self.date_ts_map[exif_ymd]
            idx = bisect.bisect_left(ts_list, exif_ts)
            candidates_to_check = []
            if idx > 0:
                candidates_to_check.append(ts_list[idx - 1])
            if idx < len(ts_list):
                candidates_to_check.append(ts_list[idx])

            for c_ts in candidates_to_check:
                diff = abs(c_ts - exif_ts)
                if diff <= 30.0:
                    reason = f"Temporal burst proximity on {exif_ymd} ({diff:.1f}s away from an existing library photo)"
                    if visual_meta.get("matched_moment_name"):
                        reason += f" [Moment: {visual_meta['matched_moment_name']}]"
                    return "review_similar", "temporal_burst", reason, None, visual_meta

        # ---------------------------------------------------------
        # TIER 4: High-Confidence Clean New Media
        # ---------------------------------------------------------
        reason = "Confirmed new asset (not in library, clean sequence)"
        if visual_meta.get("matched_moment_name"):
            reason += f" [Aligned with Moment: {visual_meta['matched_moment_name']}]"
        return "new", "clean_media", reason, None, visual_meta

def unlock_path(path):
    """
    Clears macOS immutable flags (uchg, nouappnd) and Time Machine ACLs
    (such as 'group:everyone deny delete,delete_child,writeattr') to allow moving/deleting files.
    """
    try:
        os.chflags(path, 0)
    except (AttributeError, OSError):
        pass
    try:
        subprocess.run(["chmod", "-N", path], capture_output=True, check=False)
    except Exception:
        pass
    try:
        os.chmod(path, os.stat(path).st_mode | 0o200)
    except OSError:
        pass

def safe_move(src, dest):
    dest_dir = os.path.dirname(dest)
    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(dest):
        src_sz = os.path.getsize(src)
        dst_sz = os.path.getsize(dest)
        if src_sz == dst_sz:
            # Identical duplicate already at destination; remove source
            try:
                os.remove(src)
            except (PermissionError, OSError):
                unlock_path(os.path.dirname(src))
                unlock_path(src)
                os.remove(src)
            return dest
        # Disambiguate collision
        base, ext = os.path.splitext(dest)
        idx = 1
        while os.path.exists(f"{base}_dup{idx}{ext}"):
            idx += 1
        dest = f"{base}_dup{idx}{ext}"

    try:
        shutil.move(src, dest)
    except (PermissionError, OSError):
        # Auto-unlock source, source parent directory, and destination if Time Machine ACLs or locks are present
        unlock_path(os.path.dirname(src))
        unlock_path(src)
        unlock_path(dest_dir)
        shutil.move(src, dest)

    return dest

def safe_copy(src, dest):
    """
    Safely copies a file to destination preserving timestamps and metadata (shutil.copy2).
    Disambiguates name collisions by appending _dupX.
    Leaves the source file 100% untouched (read-only mode).
    """
    dest_dir = os.path.dirname(dest)
    os.makedirs(dest_dir, exist_ok=True)

    if os.path.exists(dest):
        try:
            src_sz = os.path.getsize(src)
            dst_sz = os.path.getsize(dest)
            if src_sz == dst_sz:
                # Identical duplicate already at destination; skip re-copy
                return dest
        except OSError:
            pass

        # Disambiguate collision
        base, ext = os.path.splitext(dest)
        idx = 1
        while os.path.exists(f"{base}_dup{idx}{ext}"):
            idx += 1
        dest = f"{base}_dup{idx}{ext}"

    try:
        shutil.copy2(src, dest)
    except (PermissionError, OSError):
        unlock_path(dest_dir)
        shutil.copy2(src, dest)

    return dest

def clean_empty_directories(root_dir, protected_subdirs):
    protected_paths = {os.path.abspath(p) for p in protected_subdirs}
    for current_root, dirs, files in os.walk(root_dir, topdown=False):
        current_abs = os.path.abspath(current_root)
        if any(current_abs == p or current_abs.startswith(p + os.sep) for p in protected_paths):
            continue
        if current_abs == os.path.abspath(root_dir):
            continue

        try:
            if not os.listdir(current_root):
                try:
                    os.rmdir(current_root)
                except (PermissionError, OSError):
                    unlock_path(os.path.dirname(current_root))
                    unlock_path(current_root)
                    os.rmdir(current_root)
                logger.debug(f"🧹 Cleaned empty directory: {current_root}")
        except Exception as e:
            logger.debug(f"Could not remove directory {current_root}: {e}")

def find_photos_library_parent(path):
    """
    Returns the photoslibrary directory path if path is inside a .photoslibrary or .photolibrary bundle.
    Returns None if path is a normal directory.
    """
    cur = os.path.abspath(path)
    while cur and cur != os.path.dirname(cur):
        if cur.lower().endswith('.photoslibrary') or cur.lower().endswith('.photolibrary'):
            return cur
        cur = os.path.dirname(cur)
    return None

def triage_intake_folder(target_dir, lacie_dest_root=None, dell_dest_root=None, triage_id=None, dry_run=False, clean_empty=True, photos_library=None, ignore_duplicates=False):
    target_dir = os.path.abspath(target_dir)
    lacie_dest_root = os.path.abspath(lacie_dest_root or TRIAGE_LACIE_DESTINATION_ROOT)
    dell_dest_root = os.path.abspath(dell_dest_root or TRIAGE_DELL_DESTINATION_ROOT)
    is_photos_library = bool(photos_library)
    if is_photos_library:
        clean_empty = False

    triage_id = determine_triage_id(MEDIA_ORGANIZER_DB_PATH, lacie_dest_root, dell_dest_root, requested_id=triage_id)

    # Attach triage_id to the logger so all logs contain [Triage ID: <triage_id>]
    setup_logger(TRIAGE_LOG_PATH, MODULE_TAG, extra_fields={"triage_id": triage_id})

    # Multi-Drive Destination Hierarchy:
    # DELL1TB02: Duplicates/<triage_id> (or skipped if ignore_duplicates)
    # LaCie:     Review-Similar/<triage_id>, New/<triage_id>
    duplicates_dir = os.path.join(dell_dest_root, "Duplicates", triage_id) if not ignore_duplicates else None
    review_dir = os.path.join(lacie_dest_root, "Review-Similar", triage_id)
    new_dir = os.path.join(lacie_dest_root, "New", triage_id)
    session_dest_desc = (
        f"DELL1TB02: [Skipped Duplicates] | LaCie: [Review-Similar|New]/{triage_id}"
        if ignore_duplicates
        else f"DELL1TB02: Duplicates/{triage_id} | LaCie: [Review-Similar|New]/{triage_id}"
    )

    logger.info(f"🚀 Starting Media Intake Triage [Triage ID: {triage_id}]")
    if is_photos_library:
        logger.info(f"   - Mode:                     🛡️ READ-ONLY PHOTOS LIBRARY REPLICA MODE")
        logger.info(f"   - Photos Library Package:   {photos_library}")
        print(f"🛡️  Apple Photos Library Read-Only Mode Active: {photos_library}")
        print(f"   Mode: Full Replica (Copy Net-New to LaCie & Duplicates to DELL1TB02; source 100% untouched)")
    logger.info(f"   - Source Directory:        {target_dir}")
    if ignore_duplicates:
        logger.info("   - Duplicates Destination:  ⚡ SKIPPED (Ignore Duplicates Mode - Duplicates will NOT be moved/copied)")
        print("⚡ Ignore Duplicates mode active: duplicates will be identified but NOT moved/copied to DELL1TB02.")
    else:
        logger.info(f"   - Duplicates Destination:  {duplicates_dir} (DELL1TB02)")
    logger.info(f"   - Review & New (LaCie):    {lacie_dest_root}/[Review-Similar|New]/{triage_id}")
    if dry_run:
        logger.info("🔎 DRY RUN MODE: No files will be moved/copied and DB will not be modified.")

    if not os.path.exists(target_dir):
        logger.error(f"❌ Target intake directory does not exist: {target_dir}")
        return

    # Check DELL1TB02 availability (only required if copying duplicates)
    if not dry_run and not ignore_duplicates and not os.path.exists("/Volumes/DELL1TB02"):
        logger.error("❌ DELL1TB02 drive is not mounted at /Volumes/DELL1TB02! Please connect DELL1TB02 to store disposable duplicates.")
        raise FileNotFoundError("DELL1TB02 volume not mounted: /Volumes/DELL1TB02")

    # Record session in DB as running
    db_conn = None
    db_session_pk = None
    if not dry_run:
        try:
            db_conn = sqlite3.connect(MEDIA_ORGANIZER_DB_PATH)
            ensure_triage_tables(db_conn)
            cur = db_conn.cursor()
            cur.execute("""
                INSERT INTO triage_sessions (
                    triage_id, source_directory, destination_directory, status, dry_run, started_at_utc
                ) VALUES (?, ?, ?, 'running', 0, datetime('now'))
            """, (triage_id, target_dir, session_dest_desc))
            db_session_pk = cur.lastrowid
            db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to record triage session in DB: {e}")

    try:
        # Load All-Media library catalog
        index = LibraryIndex(MEDIA_ORGANIZER_DB_PATH)
        index.load()

        logger.info(f"🔍 Scanning intake directory: {target_dir}")
        print(f"🔍 Scanning intake directory: {target_dir}")
        candidate_files = []
        photos_libraries_detected = set()
        ignored_dir_names = {
            "triagemediaintake", "$recycle.bin", ".spotlight-v100", ".trashes", ".fseventsd"
        }
        ignored_file_names = {
            ".ds_store", "thumbs.db", "desktop.ini"
        }
        ignored_library_subdirs = {
            "resources", "private", "database", "scopes", "derivatives"
        }

        lacie_dest_abs = os.path.abspath(lacie_dest_root)
        dell_dest_abs = os.path.abspath(dell_dest_root)
        last_scan_time = time.time()

        for root, dirs, files in os.walk(target_dir):
            root_abs = os.path.abspath(root)
            # Never scan inside destination root directories on either drive
            if root_abs == lacie_dest_abs or root_abs.startswith(lacie_dest_abs + os.sep):
                continue
            if root_abs == dell_dest_abs or root_abs.startswith(dell_dest_abs + os.sep):
                continue

            rel_root = os.path.relpath(root, target_dir)
            first_segment = rel_root.split(os.sep)[0].lower() if rel_root != '.' else ''

            # If scanning inside or around TriageMediaIntake, skip the destination folder itself
            if first_segment in ignored_dir_names:
                continue

            # If scanning inside an Apple Photos Library, ignore thumbnail caches, private, database, and scopes
            if is_photos_library:
                rel_parts = [p.lower() for p in rel_root.split(os.sep) if p and p != '.']
                if rel_parts and rel_parts[0] in ignored_library_subdirs:
                    continue

            for f in sorted(files):
                if f.startswith('.') or f.lower() in ignored_file_names:
                    continue
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue

                full_p = os.path.join(root, f)
                rel_p = os.path.relpath(full_p, target_dir)

                # Check if file resides inside an Apple Photos Library package
                lib_parent = find_photos_library_parent(full_p)
                if lib_parent:
                    photos_libraries_detected.add(lib_parent)

                try:
                    sz = os.path.getsize(full_p)
                    candidate_files.append((full_p, rel_p, f, sz))
                except OSError as e:
                    logger.warning(f"Could not stat {full_p}: {e}")

            now = time.time()
            if len(candidate_files) > 0 and (now - last_scan_time) >= 1.0:
                last_scan_time = now
                sys.stdout.write(f"\r🔍 Found {len(candidate_files):,} candidate files so far...")
                sys.stdout.flush()

        if candidate_files:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

        logger.info(f"📂 Found {len(candidate_files):,} candidate media files in intake directory.")
        print(f"📂 Found {len(candidate_files):,} candidate media files in intake directory.")

        if not candidate_files:
            logger.info("✨ No media files found in intake directory to triage.")
            if db_conn and db_session_pk:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'completed', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
                db_conn.close()
            return

        # Step 2: Live Photo companion grouping
        grouped_by_stem = {}
        for full_p, rel_p, f, sz in candidate_files:
            dirname = os.path.dirname(rel_p)
            stem, ext = os.path.splitext(f)
            # If filename is an Apple Photos UUID with an underscore suffix like <UUID>_3 or <UUID>_1,
            # normalize to base UUID so Live Photo companion videos group with their still photo
            clean_stem = re.sub(r'_\d+$', '', stem) if re.match(r'^[0-9a-fA-F-]{36}_\d+$', stem) else stem
            key = (dirname, clean_stem.lower())
            if key not in grouped_by_stem:
                grouped_by_stem[key] = []
            grouped_by_stem[key].append((full_p, rel_p, f, sz, ext.lower()))

        # Step 3: Evaluate and Triage into 3 Categories
        duplicates_list = []
        review_list = []
        new_list = []

        # Sub-breakdowns for reporting
        identical_dup_count = 0
        identical_dup_bytes = 0
        compressed_dup_count = 0
        compressed_dup_bytes = 0
        visual_dup_count = 0
        visual_dup_bytes = 0

        upgrade_count = 0
        upgrade_bytes = 0
        visual_sim_count = 0
        visual_sim_bytes = 0
        discard_sim_count = 0
        discard_sim_bytes = 0
        web_artifact_count = 0
        web_artifact_bytes = 0
        moment_matched_count = 0

        total_dup_bytes = 0
        total_review_bytes = 0
        total_new_bytes = 0

        total_groups = len(grouped_by_stem)
        last_progress_time = time.time()

        for idx, ((dirname, stem_lower), file_group) in enumerate(grouped_by_stem.items(), 1):
            now = time.time()
            pct = (idx / total_groups) * 100
            if idx == 1 or idx == total_groups or (now - last_progress_time) >= 0.5 or idx % 25 == 0:
                last_progress_time = now
                sys.stdout.write(
                    f"\r⏳ Evaluating intake assets: {idx:,}/{total_groups:,} ({pct:.1f}%) | "
                    f"Dups: {len(duplicates_list):,} | Review: {len(review_list):,} | New: {len(new_list):,}..."
                )
                sys.stdout.flush()

            # Periodic progress log to file every 10% or at milestones
            if idx == 1 or idx % max(1, total_groups // 10) == 0 or idx == total_groups:
                logger.info(
                    f"⏳ Evaluation Progress: {idx:,}/{total_groups:,} ({pct:.0f}%) | "
                    f"Duplicates: {len(duplicates_list):,}, Review-Similar: {len(review_list):,}, New: {len(new_list):,}"
                )

            primary_candidates = [item for item in file_group if item[4] not in ('.mov', '.mp4')]
            video_candidates = [item for item in file_group if item[4] in ('.mov', '.mp4')]

            if primary_candidates:
                p_full, p_rel, p_f, p_sz, p_ext = primary_candidates[0]
                cat, sub_type, reason, matched, visual_meta = index.evaluate_asset(p_full, p_rel, p_sz, p_f, is_photos_library=is_photos_library)

                if visual_meta and visual_meta.get("matched_moment_name"):
                    moment_matched_count += len(file_group)

                if cat == "duplicate":
                    if sub_type == "visual_duplicate":
                        visual_dup_count += len(file_group)
                        visual_dup_bytes += sum(it[3] for it in file_group)
                    elif sub_type == "compressed":
                        compressed_dup_count += len(file_group)
                        compressed_dup_bytes += sum(it[3] for it in file_group)
                    else:
                        identical_dup_count += len(file_group)
                        identical_dup_bytes += sum(it[3] for it in file_group)

                    for item in primary_candidates:
                        duplicates_list.append((item[0], item[1], item[2], item[3], reason, sub_type, visual_meta))
                        total_dup_bytes += item[3]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to duplicate {p_f} ({reason})"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        duplicates_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason, sub_type, v_meta))
                        total_dup_bytes += v_item[3]

                elif cat == "review_similar":
                    if sub_type == "upgrade_candidate":
                        upgrade_count += len(file_group)
                        upgrade_bytes += sum(it[3] for it in file_group)
                    elif sub_type == "visual_similar":
                        visual_sim_count += len(file_group)
                        visual_sim_bytes += sum(it[3] for it in file_group)
                    elif sub_type == "web_artifact":
                        web_artifact_count += len(file_group)
                        web_artifact_bytes += sum(it[3] for it in file_group)
                    else:
                        discard_sim_count += len(file_group)
                        discard_sim_bytes += sum(it[3] for it in file_group)

                    for item in primary_candidates:
                        review_list.append((item[0], item[1], item[2], item[3], reason, sub_type, visual_meta))
                        total_review_bytes += item[3]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to review-similar asset {p_f} ({reason})"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        review_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason, sub_type, v_meta))
                        total_review_bytes += v_item[3]

                else:
                    for item in primary_candidates:
                        new_list.append((item[0], item[1], item[2], item[3], reason, sub_type, visual_meta))
                        total_new_bytes += item[3]
                    for v_item in video_candidates:
                        v_reason = f"Companion Live Photo video to new asset {p_f}"
                        v_meta = visual_meta.copy() if visual_meta else {}
                        v_meta["perceptual_hash"] = None
                        new_list.append((v_item[0], v_item[1], v_item[2], v_item[3], v_reason, sub_type, v_meta))
                        total_new_bytes += v_item[3]

            else:
                # Standalone videos
                for v_item in video_candidates:
                    v_full, v_rel, v_f, v_sz, v_ext = v_item
                    cat, sub_type, reason, matched, visual_meta = index.evaluate_asset(v_full, v_rel, v_sz, v_f, is_photos_library=is_photos_library)

                    if visual_meta and visual_meta.get("matched_moment_name"):
                        moment_matched_count += 1

                    if cat == "duplicate":
                        if sub_type == "compressed":
                            compressed_dup_count += 1
                            compressed_dup_bytes += v_sz
                        else:
                            identical_dup_count += 1
                            identical_dup_bytes += v_sz
                        duplicates_list.append((v_full, v_rel, v_f, v_sz, reason, sub_type, visual_meta))
                        total_dup_bytes += v_sz
                    elif cat == "review_similar":
                        if sub_type == "upgrade_candidate":
                            upgrade_count += 1
                            upgrade_bytes += v_sz
                        elif sub_type == "web_artifact":
                            web_artifact_count += 1
                            web_artifact_bytes += v_sz
                        else:
                            discard_sim_count += 1
                            discard_sim_bytes += v_sz
                        review_list.append((v_full, v_rel, v_f, v_sz, reason, sub_type, visual_meta))
                        total_review_bytes += v_sz
                    else:
                        new_list.append((v_full, v_rel, v_f, v_sz, reason, sub_type, visual_meta))
                        total_new_bytes += v_sz

        sys.stdout.write("\r" + " " * 95 + "\r")
        sys.stdout.flush()

        # Step 4: Summary Output & Detailed Logging
        print("\n" + "=" * 78)
        print(f"📊 MEDIA INTAKE 3-WAY TRIAGE SUMMARY [Triage ID: {triage_id}]")
        print("=" * 78)
        print(f"Source Directory:      {target_dir}")
        print(f"Destination Hierarchy: {session_dest_desc}")
        print(f"Total Files Scanned:   {len(candidate_files):,}")
        print("-" * 78)
        print(f"1. Duplicates Total:                                {len(duplicates_list):>5,} assets ({format_size(total_dup_bytes):>10})")
        print(f"   - Identical Matches:                             {identical_dup_count:>5,} assets ({format_size(identical_dup_bytes):>10})")
        print(f"   - Compressed Copies (Library is Superior):      {compressed_dup_count:>5,} assets ({format_size(compressed_dup_bytes):>10})")
        if visual_dup_count > 0:
            print(f"   - 👁️ Visual Duplicates (>=95% Match):             {visual_dup_count:>5,} assets ({format_size(visual_dup_bytes):>10})")
        print("-" * 78)
        print(f"2. Review-Similar Total:                            {len(review_list):>5,} assets ({format_size(total_review_bytes):>10})")
        print(f"   - 🌟 Upgrade Candidates (Intake is Better):      {upgrade_count:>5,} assets ({format_size(upgrade_bytes):>10})")
        if visual_sim_count > 0:
            print(f"   - 👁️ Visually Similar Photos (88-94% Match):      {visual_sim_count:>5,} assets ({format_size(visual_sim_bytes):>10})")
        print(f"   - Suspected Discard / Burst Siblings:           {discard_sim_count:>5,} assets ({format_size(discard_sim_bytes):>10})")
        print(f"   - Web / UI Graphic Artifacts (<50KB):           {web_artifact_count:>5,} assets ({format_size(web_artifact_bytes):>10})")
        if moment_matched_count > 0:
            print(f"   - 📍 Apple Photos Moment Matches:                 {moment_matched_count:>5,} assets")
        print("-" * 78)
        print(f"3. New (High-Confidence Clean Assets):              {len(new_list):>5,} assets ({format_size(total_new_bytes):>10})")
        print("=" * 78 + "\n")

        logger.info(f"Triage result: {len(duplicates_list):,} Duplicates ({format_size(total_dup_bytes)}), {len(review_list):,} Review-Similar ({format_size(total_review_bytes)}), {len(new_list):,} New ({format_size(total_new_bytes)}) [Visual Dups: {visual_dup_count}, Moments: {moment_matched_count}]")

        # Log individual files
        for src_full, rel_p, fname, sz, reason, sub_t, vmeta in duplicates_list:
            v_info = ""
            if vmeta and vmeta.get("visual_similarity_score") is not None:
                v_info = f" | Sim: {vmeta['visual_similarity_score']:.1f}% (dist {vmeta.get('hamming_distance')})"
            if vmeta and vmeta.get("matched_moment_name"):
                v_info += f" | Moment: {vmeta['matched_moment_name']}"
            logger.info(f"  [DUPLICATE]      {rel_p} ({format_size(sz)}) -> DELL1TB02: Duplicates/{triage_id}/{rel_p} | Reason: {reason}{v_info}")

        for src_full, rel_p, fname, sz, reason, sub_t, vmeta in review_list:
            sub_folder = f"Review-Similar/{triage_id}/Upgrade-Candidates" if sub_t == "upgrade_candidate" else f"Review-Similar/{triage_id}"
            v_info = ""
            if vmeta and vmeta.get("visual_similarity_score") is not None:
                v_info = f" | Sim: {vmeta['visual_similarity_score']:.1f}% (dist {vmeta.get('hamming_distance')})"
            if vmeta and vmeta.get("matched_moment_name"):
                v_info += f" | Moment: {vmeta['matched_moment_name']}"
            logger.info(f"  [REVIEW-SIMILAR] {rel_p} ({format_size(sz)}) -> LaCie: {sub_folder}/{rel_p} | Reason: {reason}{v_info}")

        for src_full, rel_p, fname, sz, reason, sub_t, vmeta in new_list:
            v_info = ""
            if vmeta and vmeta.get("matched_moment_name"):
                v_info += f" | Moment: {vmeta['matched_moment_name']}"
            logger.info(f"  [NEW]            {rel_p} ({format_size(sz)}) -> LaCie: New/{triage_id}/{rel_p} | Reason: {reason}{v_info}")

        if dry_run:
            print(f"🔍 [DRY RUN] Triage ID: {triage_id}")
            print(f"   Destination Hierarchy:")
            if ignore_duplicates:
                print(f"   - DELL1TB02: [Skipped Duplicates - not copying/moving duplicates]")
            else:
                print(f"   - DELL1TB02: {dell_dest_root}/Duplicates/{triage_id}/")
            print(f"   - LaCie:     {lacie_dest_root}/[Review-Similar|New]/{triage_id}/")
            act_w = "replicate (copy)" if is_photos_library else "move"
            print(f"   Would {act_w}:")
            if ignore_duplicates:
                print(f"   - {len(duplicates_list):,} duplicates IDENTIFIED (Skipped copying/moving; preserved in source)")
            else:
                print(f"   - {len(duplicates_list):,} duplicates to DELL1TB02: '{duplicates_dir}/'")
            if visual_dup_count > 0:
                print(f"     └─ {visual_dup_count:,} visual duplicates (>=95% match, different filename)")
            if upgrade_count > 0:
                print(f"   - {upgrade_count:,} upgrade candidates to LaCie: '{review_dir}/Upgrade-Candidates/'")
            if visual_sim_count > 0:
                print(f"     └─ {visual_sim_count:,} visually similar photos (88-94% match)")
            print(f"   - {len(review_list) - upgrade_count:,} review items to LaCie: '{review_dir}/'")
            print(f"   - {len(new_list):,} clean new assets to LaCie: '{new_dir}/'")
            if moment_matched_count > 0:
                print(f"   📍 Apple Photos Moment Matches: {moment_matched_count:,} assets aligned with moments")
            print(f"🏷️  [DRY RUN] Would apply macOS Finder color tags to destination folders:")
            if not ignore_duplicates:
                print(f"   - DELL1TB02: '{dell_dest_root}/Duplicates' (and /{triage_id})         -> Red 🔴")
            print(f"   - LaCie:     '{lacie_dest_root}/Review-Similar' (and /{triage_id})    -> Yellow 🟡")
            if upgrade_count > 0:
                print(f"   - LaCie:     '{review_dir}/Upgrade-Candidates'                 -> Orange 🟠")
            print(f"   - LaCie:     '{lacie_dest_root}/New' (and /{triage_id})               -> Green 🟢")
            print(f"📝 Logs appended to: {TRIAGE_LOG_PATH}")
            return

        # Step 5: Moving / Replicating files
        total_to_move = (0 if ignore_duplicates else len(duplicates_list)) + len(review_list) + len(new_list)
        action_verb = "Replicating (Copying)" if is_photos_library else "Moving"
        logger.info(f"🚚 {action_verb} {total_to_move:,} triaged files into destination folders on DELL1TB02 and LaCie...")
        print(f"🚚 {action_verb} {total_to_move:,} triaged files into destination folders...")
        moved_duplicates = 0
        moved_review = 0
        moved_new = 0
        moved_total = 0
        last_move_time = time.time()

        def report_move_progress():
            nonlocal last_move_time
            now = time.time()
            pct = (moved_total / total_to_move) * 100 if total_to_move else 100.0
            if moved_total == 1 or moved_total == total_to_move or (now - last_move_time) >= 0.5 or moved_total % 25 == 0:
                last_move_time = now
                sys.stdout.write(f"\r🚚 {action_verb} files: {moved_total:,}/{total_to_move:,} ({pct:.1f}%)...")
                sys.stdout.flush()
            if moved_total == 1 or moved_total % max(1, total_to_move // 10) == 0 or moved_total == total_to_move:
                logger.info(f"🚚 {action_verb} Progress: {moved_total:,}/{total_to_move:,} ({pct:.0f}%)")

        if ignore_duplicates:
            logger.info(f"⚡ Ignore Duplicates mode active: skipping {len(duplicates_list):,} duplicate file transfers.")
            print(f"⚡ Ignore Duplicates mode active: skipping {len(duplicates_list):,} duplicate file transfers.")
            moved_duplicates = len(duplicates_list)
        else:
            for src_full, rel_p, fname, sz, reason, sub_t, vmeta in duplicates_list:
                dest_full = os.path.join(duplicates_dir, rel_p)
                if is_photos_library:
                    try:
                        safe_copy(src_full, dest_full)
                        moved_duplicates += 1
                    except Exception as e:
                        logger.error(f"❌ Failed to copy duplicate {src_full} to {dest_full}: {e}")
                else:
                    is_in_library = bool(find_photos_library_parent(src_full))
                    if is_in_library:
                        # Do NOT move files out of a .photoslibrary bundle to protect package integrity
                        moved_duplicates += 1
                        logger.debug(f"  [PRESERVED IN LIBRARY] {src_full} -> Classified as Duplicate")
                    else:
                        try:
                            safe_move(src_full, dest_full)
                            moved_duplicates += 1
                        except Exception as e:
                            logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")
                moved_total += 1
                report_move_progress()

        for src_full, rel_p, fname, sz, reason, sub_t, vmeta in review_list:
            if sub_t == "upgrade_candidate":
                dest_full = os.path.join(review_dir, "Upgrade-Candidates", rel_p)
            else:
                dest_full = os.path.join(review_dir, rel_p)
            if is_photos_library:
                try:
                    safe_copy(src_full, dest_full)
                    moved_review += 1
                except Exception as e:
                    logger.error(f"❌ Failed to copy review asset {src_full} to {dest_full}: {e}")
            else:
                is_in_library = bool(find_photos_library_parent(src_full))
                if is_in_library:
                    # Do NOT move files out of a .photoslibrary bundle to protect package integrity
                    moved_review += 1
                    logger.debug(f"  [PRESERVED IN LIBRARY] {src_full} -> Classified as Review-Similar")
                else:
                    try:
                        safe_move(src_full, dest_full)
                        moved_review += 1
                    except Exception as e:
                        logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")
            moved_total += 1
            report_move_progress()

        for src_full, rel_p, fname, sz, reason, sub_t, vmeta in new_list:
            dest_full = os.path.join(new_dir, rel_p)
            if is_photos_library:
                try:
                    safe_copy(src_full, dest_full)
                    moved_new += 1
                except Exception as e:
                    logger.error(f"❌ Failed to copy new asset {src_full} to {dest_full}: {e}")
            else:
                is_in_library = bool(find_photos_library_parent(src_full))
                if is_in_library:
                    # Do NOT move files out of a .photoslibrary bundle to protect package integrity
                    moved_new += 1
                    logger.debug(f"  [PRESERVED IN LIBRARY] {src_full} -> Classified as New")
                else:
                    try:
                        safe_move(src_full, dest_full)
                        moved_new += 1
                    except Exception as e:
                        logger.error(f"❌ Failed to move {src_full} to {dest_full}: {e}")
            moved_total += 1
            report_move_progress()

        if total_to_move > 0:
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

        trans_verb = "Copied (Replicated)" if is_photos_library else "Moved"
        if ignore_duplicates:
            logger.info(f"✅ Skipped transferring {moved_duplicates:,} duplicates (Ignore Duplicates Mode)")
            print(f"✅ Skipped transferring {moved_duplicates:,} duplicates (Ignore Duplicates Mode; preserved in source)")
        else:
            logger.info(f"✅ {trans_verb} {moved_duplicates:,} duplicates to DELL1TB02: {duplicates_dir}")
            print(f"✅ {trans_verb} {moved_duplicates:,} duplicates to DELL1TB02: {duplicates_dir}")
        logger.info(f"✅ {trans_verb} {moved_review:,} review assets to LaCie: {review_dir}")
        logger.info(f"✅ {trans_verb} {moved_new:,} new assets to LaCie: {new_dir}")
        print(f"✅ {trans_verb} {moved_review:,} review assets to LaCie: {review_dir}")
        print(f"✅ {trans_verb} {moved_new:,} new assets to LaCie: {new_dir}")

        # Step 6: Clean empty directories left behind
        if clean_empty and not is_photos_library:
            logger.info("🧹 Cleaning empty source directories...")
            protected_subdirs = [
                lacie_dest_root,
                os.path.join(lacie_dest_root, "Review-Similar"),
                os.path.join(lacie_dest_root, "New"),
                review_dir,
                new_dir,
                dell_dest_root,
                os.path.join(dell_dest_root, "Duplicates"),
                *photos_libraries_detected
            ]
            if duplicates_dir:
                protected_subdirs.append(duplicates_dir)
            clean_empty_directories(target_dir, protected_subdirs=protected_subdirs)

        # Step 7: Apply macOS Finder color tags
        logger.info("🏷️  Applying macOS Finder color tags on DELL1TB02 and LaCie...")
        tag_results = tag_triage_folders(lacie_dest_root=lacie_dest_root, dell_dest_root=dell_dest_root, triage_id=triage_id)

        # Step 8: Update database records
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("""
                    UPDATE triage_sessions SET
                        status = 'completed',
                        total_files_scanned = ?,
                        duplicates_count = ?,
                        duplicates_bytes = ?,
                        compressed_duplicates_count = ?,
                        compressed_duplicates_bytes = ?,
                        review_similar_count = ?,
                        review_similar_bytes = ?,
                        upgrade_candidates_count = ?,
                        upgrade_candidates_bytes = ?,
                        discard_sim_count = ?,
                        discard_sim_bytes = ?,
                        web_artifact_count = ?,
                        web_artifact_bytes = ?,
                        new_count = ?,
                        new_bytes = ?,
                        visual_duplicate_count = ?,
                        moment_matched_count = ?,
                        completed_at_utc = datetime('now')
                    WHERE id = ?
                """, (
                    len(candidate_files),
                    moved_duplicates, total_dup_bytes,
                    compressed_dup_count, compressed_dup_bytes,
                    moved_review, total_review_bytes,
                    upgrade_count, upgrade_bytes,
                    discard_sim_count, discard_sim_bytes,
                    web_artifact_count, web_artifact_bytes,
                    moved_new, total_new_bytes,
                    visual_dup_count,
                    moment_matched_count,
                    db_session_pk
                ))

                asset_rows = []
                for src_full, rel_p, fname, sz, reason, sub_t, vmeta in duplicates_list:
                    dest_p = os.path.join(duplicates_dir, rel_p) if duplicates_dir else "[Skipped Duplicates]"
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, fname, sz, "duplicate", sub_t, reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))
                for src_full, rel_p, fname, sz, reason, sub_t, vmeta in review_list:
                    sub_f = "Upgrade-Candidates" if sub_t == "upgrade_candidate" else ""
                    dest_p = os.path.join(review_dir, sub_f, rel_p) if sub_f else os.path.join(review_dir, rel_p)
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, fname, sz, "review_similar", sub_t, reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))
                for src_full, rel_p, fname, sz, reason, sub_t, vmeta in new_list:
                    dest_p = os.path.join(new_dir, rel_p)
                    vmeta = vmeta or {}
                    asset_rows.append((
                        db_session_pk, triage_id, rel_p, fname, sz, "new", sub_t or "clean_media", reason, dest_p,
                        vmeta.get("matched_asset_uuid"), vmeta.get("matched_moment_name"),
                        vmeta.get("visual_similarity_score"), vmeta.get("hamming_distance"),
                        vmeta.get("perceptual_hash")
                    ))

                cur.executemany("""
                    INSERT INTO triage_session_assets (
                        triage_session_id, triage_id, original_rel_path, filename,
                        file_size_bytes, category, sub_type, decision_reason, destination_path,
                        matched_asset_uuid, matched_moment_name, visual_similarity_score,
                        hamming_distance, perceptual_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, asset_rows)
                db_conn.commit()
                logger.info(f"💾 Logged triage session '{triage_id}' and {len(asset_rows):,} asset movements in DB.")
            except Exception as e:
                logger.error(f"Failed to update triage session in DB: {e}")
            finally:
                db_conn.close()

        print(f"✅ Triage Complete! [Triage ID: {triage_id}]")
        print(f"   Destination Hierarchy: {session_dest_desc}")
        if ignore_duplicates:
            print(f"   - {len(duplicates_list):,} duplicates skipped (not moved/copied; preserved in source)")
            print(f"   - {moved_review:,} review assets {'replicated' if is_photos_library else 'moved'} to LaCie: '{review_dir}/'")
            print(f"   - {moved_new:,} clean new assets {'replicated' if is_photos_library else 'moved'} to LaCie: '{new_dir}/'")
        elif is_photos_library:
            print(f"   - {moved_duplicates:,} duplicates replicated to DELL1TB02: '{duplicates_dir}/'")
            print(f"   - {moved_review:,} review assets replicated to LaCie: '{review_dir}/'")
            print(f"   - {moved_new:,} clean new assets replicated to LaCie: '{new_dir}/'")
        elif photos_libraries_detected:
            print(f"   - {moved_duplicates:,} duplicates evaluated (Preserved inside Photos Library package)")
            print(f"   - {moved_review:,} review assets evaluated (Preserved inside Photos Library package)")
            print(f"   - {moved_new:,} clean assets evaluated (Preserved inside Photos Library package)")
        else:
            print(f"   - {moved_duplicates:,} duplicates moved to DELL1TB02: '{duplicates_dir}/'")
            print(f"   - {moved_review:,} review assets moved to LaCie: '{review_dir}/'")
            print(f"   - {moved_new:,} clean assets moved to LaCie: '{new_dir}/'")
        if tag_results:
            print(f"🏷️  Finder Color Tags Applied:")
            for folder_rel, (color, ok) in tag_results.items():
                icon = "🔴" if color == "Red" else "🟡" if color == "Yellow" else "🟠" if color == "Orange" else "🟢"
                status = "✅" if ok else "⚠️"
                print(f"   {status} {folder_rel:<45} -> {color} {icon}")
        print(f"📝 Full audit log: {TRIAGE_LOG_PATH}")

        if is_photos_library:
            print("\n" + "=" * 80)
            print("🛡️  PHOTOS MEDIA LIBRARY PROCESSED (READ-ONLY FULL REPLICA MODE)")
            print("-" * 80)
            print("The following Apple Photos Library package was processed as a read-only source:")
            print(f"   📦 {photos_library}")
            print("\nAll assets were evaluated and replicated:")
            print(f"   • {moved_duplicates:,} duplicates copied to DELL1TB02: '{duplicates_dir}/'")
            print(f"   • {moved_review:,} review assets copied to LaCie: '{review_dir}/'")
            print(f"   • {moved_new:,} clean new assets copied to LaCie: '{new_dir}/'")
            print("\nSource Library Protection:")
            print("   ✅ 100% READ-ONLY: Source files and folders remain completely unmodified.")
            print("\n👉 ACTION REQUIRED:")
            print(f"   The source Photos Library package ({photos_library})")
            print("   must now be MANUALLY PROCESSED or DELETED when you have verified the intake.")
            print("=" * 80 + "\n")
            logger.warning(
                f"🛡️ Photos Media Library processed in Read-Only Full Replica Mode: {photos_library}. "
                "Source files remain 100% untouched."
            )
        elif photos_libraries_detected:
            print("\n" + "=" * 80)
            print("⚠️  NOTICE: Photos Media Library Package(s) Detected & Processed:")
            for lib_p in sorted(photos_libraries_detected):
                print(f"   📦 {lib_p}")
            print("\n   To protect the Apple Photos package integrity and prevent bundle corruption,")
            print("   assets inside .photoslibrary were evaluated and indexed in the database")
            print("   WITHOUT moving or deleting files from the package.")
            print("   👉 This Photos Media Library file needs to be MANUALLY processed or deleted.")
            print("=" * 80 + "\n")
            logger.warning(
                f"⚠️ Photos Media Library package(s) detected: {sorted(photos_libraries_detected)}. "
                "Files evaluated and indexed in DB without moving from the package."
            )

    except KeyboardInterrupt:
        logger.warning(f"\n⚠️ Triage session '{triage_id}' cancelled by user (KeyboardInterrupt).")
        print(f"\n\n⚠️ Triage session '{triage_id}' cancelled by user (Ctrl-C). No files were moved.")
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'cancelled', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
            except Exception:
                pass
            finally:
                db_conn.close()
        return

    except Exception as e:
        logger.error(f"❌ Triage session failed unexpectedly: {e}", exc_info=True)
        if db_conn and db_session_pk:
            try:
                cur = db_conn.cursor()
                cur.execute("UPDATE triage_sessions SET status = 'failed', completed_at_utc = datetime('now') WHERE id = ?", (db_session_pk,))
                db_conn.commit()
            except Exception:
                pass
            finally:
                db_conn.close()
        raise

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Triage media files from an intake folder into 3 categories across 2 drives:\n"
            "  - DELL1TB02: /Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/Duplicates/<triage_id>\n"
            "  - LaCie:     /Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/[Review-Similar|New]/<triage_id>"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("target_dir", nargs="?", default=DEFAULT_MEDIA_PREPROCESSING_DIR,
                        help=f"Source intake directory to scan and triage (default: {DEFAULT_MEDIA_PREPROCESSING_DIR})")
    parser.add_argument("--lacie-dest-dir", "--dest-dir", dest="lacie_dest_dir", default=None,
                        help=f"LaCie base destination directory for Review-Similar and New (default: {TRIAGE_LACIE_DESTINATION_ROOT})")
    parser.add_argument("--dell-dest-dir", dest="dell_dest_dir", default=None,
                        help=f"DELL1TB02 base destination directory for Duplicates (default: {TRIAGE_DELL_DESTINATION_ROOT})")
    parser.add_argument("--triage-id", default=None,
                        help="Custom triage session ID (default: auto-increment integer ID from database)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate the triage process without moving any files or modifying DB.")
    parser.add_argument("--ignore-duplicates", action="store_true",
                        help="Skip moving/copying duplicate files to DELL1TB02 (only route Review-Similar and New to LaCie).")
    parser.add_argument("--no-clean", action="store_true",
                        help="Do not delete empty directories left behind in the intake folder.")
    parser.add_argument("--tag-only", action="store_true",
                        help="Apply macOS Finder color tags to existing triage folders without running triage scan.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.tag_only:
        lacie_root = os.path.abspath(args.lacie_dest_dir or TRIAGE_LACIE_DESTINATION_ROOT)
        dell_root = os.path.abspath(args.dell_dest_dir or TRIAGE_DELL_DESTINATION_ROOT)
        print(f"🏷️  Applying macOS Finder tags to triage folders:")
        print(f"   - DELL1TB02: {dell_root}")
        print(f"   - LaCie:     {lacie_root}")
        res = tag_triage_folders(lacie_dest_root=lacie_root, dell_dest_root=dell_root, triage_id=args.triage_id)
        for folder_rel, (color, ok) in res.items():
            icon = "🔴" if color == "Red" else "🟡" if color == "Yellow" else "🟠" if color == "Orange" else "🟢"
            status = "✅" if ok else "⚠️"
            print(f"   {status} {folder_rel:<45} -> {color} {icon}")
        if not res:
            print("   ℹ️ No triage folders (Duplicates, Review-Similar, New) found to tag.")
    else:
        triage_intake_folder(
            args.target_dir,
            lacie_dest_root=args.lacie_dest_dir,
            dell_dest_root=args.dell_dest_dir,
            triage_id=args.triage_id,
            dry_run=args.dry_run,
            clean_empty=not args.no_clean,
            ignore_duplicates=args.ignore_duplicates
        )

