# 📋 Media Organizer Pipeline Overview

This pipeline handles the curation, export, and publishing of family photos, from organizing assets in Apple Photos to exporting and sharing curated memories.

Pipeline executor format:

> python3 scripts/pipeline_executor.py --from 0 --to 2

---

## 📂 Stage 0 – Initialization & Setup

| Step | Label       | Description                                              | Script/Tool               | Status    |
| ---- | ----------- | -------------------------------------------------------- | ------------------------- | --------- |
| 0.1  | Init DB     | Create schema, set up tables and views                   | `storage_status.py`       | ✅ Stable |
| 0.2  | Field Check | Verify Photos DB schema fields (safe fallback detection) | `storage_status.py`       | ✅ Stable |
| 0.3  | Sync Meta   | Copy Apple Photos metadata and create query view         | `sync_photos_metadata.py` | ✅ Stable |

---

## 📦 Stage 1 – Monthly Batch Detection

| Step | Label        | Description                                            | Script/Tool                 | Status     |
| ---- | ------------ | ------------------------------------------------------ | --------------------------- | ---------- |
| 1.1  | Detect Gaps  | Identify month(s) missing from processed batch records | `generate_month_batches.py` | ✅ Working |
| 1.2  | Create Batch | Create new `month_batches` entries; detect orphans     | `generate_month_batches.py` | ✅ Working |

---

## 📤 Stage 2 – Apple Photos Export & Upload Prep

| Step  | Label          | Description                                              | Script/Tool                      | Status      |
| ----- | -------------- | -------------------------------------------------------- | -------------------------------- | ----------- |
| 2.1   | Check Album    | Verify Smart Album for export exists                     | `verify_smart_album.py`          | ✅ Required |
| 2.2   | Export Assets  | AppleScript export from Photos to monthly staging folder | `export_photos_applescript.scpt` | ✅ Working  |
| 2.3   | Verify Folder  | Confirm export folder exists and matches expected batch  | `verify_staging.py`              | ✅ Working  |
| 2.3.5 | Sync Metadata  | Extract metadata into DB from staging files              | `sync_photos_assets.py`          | ✅ Working  |
| 2.4   | Upload to G.Ph | Upload curated batch to Google Photos API                | `upload_to_google_photos.py`     | ✅ Working  |

---

### Stage 3 – AI Review & Asset Scoring

| #     | Step                        | Description                                                                          | Tool / Script                        | Status    |
| ----- | --------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------ | --------- |
| 3.1   | Wait for Google AI          | Wait for Google Photos to generate Memories and Highlights (auto-curated)            | Manual (Google Photos)               | ✅ Manual |
| 3.2   | Star Curated Items          | Manually star the best photos suggested by Google (or personal choice)               | Manual (Google Photos)               | ✅ Manual |
| 3.2.5 | Pull Google Favorites       | Use Google Photos API to fetch starred media and update `is_google_favorite` in DB   | `pull_google_favorites.py`           | ✅ Done   |
| 3.3   | Sync Apple Aesthetic Scores | Enrich DB with aesthetic scores and metadata from Apple Photos                       | `sync_photos_assets.py`              | ✅ Done   |
| 3.4   | Rank Assets by Score        | Combine Apple score + Google Favorite flag using weighted score; store session in DB | `rank_assets_by_score.py`            | ✅ Done   |
| 3.5   | Export Ranked Assets        | Export scored assets to curated folder for human review                              | Built into `rank_assets_by_score.py` | ✅ Done   |

### Stage 4 – Final Curation & Cleanup

| #     | Step                           | Description                                                           | Tool / Script                       | Status     |
| ----- | ------------------------------ | --------------------------------------------------------------------- | ----------------------------------- | ---------- |
| 4.1   | Final Human Review             | Manual curation of exported assets (visual/manual check)              | Manual                              | ✅ Manual  |
| 4.1.5 | Export Human-Approved Assets   | Copy human-approved subset to a separate monthly subfolder in staging | _(TBD: `export_curated_subset.py`)_ | ❌ Pending |
| 4.2   | Delete Non-Favorites in Google | Compare exported approved set with Google Photos and delete others    | `delete_nonfavorites_google.py`     | ❌ Pending |
| 4.3   | Archive Final Staging Folder   | Move finished folders to archive location or external storage         | `archive_staging_folder.py`         | ❌ Pending |

### Stage 5 – Publishing & Sharing

| #   | Step                    | Description                                                                            | Tool / Script        | Status     |
| --- | ----------------------- | -------------------------------------------------------------------------------------- | -------------------- | ---------- |
| 5.1 | Publish Curated Photos  | Push final approved assets to platforms (e.g., Shutterfly, Walmart, photo books, etc.) | Manual or future API | ❌ Planned |
| 5.2 | Track Published Assets  | Record which assets were printed/shared in the DB                                      | _(Planned)_          | ❌ Planned |
| 5.3 | Backup Curated Assets   | Backup to external drives or cloud storage                                             | Manual               | ✅ Manual  |
| 5.4 | Share via Google Photos | Use Google sharing features to send curated albums                                     | Google Photos        | ✅ Manual  |

### Stage 6 – Reporting & Status (Optional)

| #   | Step                 | Description                                                              | Tool / Script | Status     |
| --- | -------------------- | ------------------------------------------------------------------------ | ------------- | ---------- |
| 6.1 | View Pipeline Status | Show completion status of each stage and batch                           | _(Planned)_   | ❌ Planned |
| 6.2 | Export Status Report | CSV or HTML report of all curated assets, scoring, and publishing status | _(Planned)_   | ❌ Planned |

## 📂 Step 2.1: Verify Smart Album (Manual)

Before proceeding with exporting photos:

- **Check that the Smart Album** corresponding to the current month batch (e.g., **April 2025**) **exists** in Apple Photos.
- If the Smart Album **does not exist**, the process will **stop** and display a message:  
  _"Smart Album for the current batch does not exist. Please create it manually in Apple Photos."_

Once the Smart Album is created, you can continue the process.

💡 You can run this script in dry-run mode to skip the AppleScript export and only verify existence:

> python3 scripts/verify_smart_album.py --dry-run

> ℹ️ Step 2.3.5 (`sync_photos_assets.py`) is a required step for syncing and enriching asset metadata before uploading to Google Photos.

## 📌 Step 3.1.5 – Pull Favorites from Google Photos

After Google Photos auto-curates memories and before manual review, this step pulls the list of media items marked as ⭐ Favorites in Google Photos.

- Downloads favorite status using the API
- Matches favorites to the assets in the local database (by filename and timestamp)
- Updates `assets` table with `is_google_favorite = true` for matching records

## 📌 Step 3.2 – Manual Curation via Google Photos App

After uploading a full month of photos to Google Photos, wait for the auto-curated **Memory Collection** to appear (e.g., “Best of March 2025”). These are not Albums and are not accessible via API.

Use the Google Photos app (mobile or web) to view the generated Memories, and ⭐️ mark your favorite photos manually. These starred photos will later be retrieved via API in the next step (3.3) for export, curation, and cleanup.

> ℹ️ Step 3.3 (`sync_photos_assets.py`) is optional and is used primarily for pre-upload filtering, scoring analysis, or offline reporting. It is not required for the standard pipeline flow.

---

## 🗂️ Database Schema

### Table: `imports`

Tracks imports from Apple Photos.

|        Column        |        Type         | Description                             |
| :------------------: | :-----------------: | :-------------------------------------- |
|          id          | INTEGER PRIMARY KEY | Unique ID                               |
|         uuid         |        TEXT         | Import session UUID                     |
| import_timestamp_utc |      DATETIME       | Timestamp of import                     |
|     import_name      |        TEXT         | Human-friendly display name             |
|        device        |        TEXT         | Device name (from camera metadata)      |
|        album         |        TEXT         | Album name if provided during import    |
|     asset_count      |       INTEGER       | Number of assets in the import session  |
|   months_detected    |        TEXT         | Comma-separated list of included months |
|    created_at_utc    |      DATETIME       | When record was added to the database   |

### Table: `month_batches`

Tracks batches to be processed and their status.

|              Column               |        Type         | Description                           |
| :-------------------------------: | :-----------------: | :------------------------------------ |
|                id                 | INTEGER PRIMARY KEY | Unique ID                             |
|               month               |   TEXT (YYYY-MM)    | Month identifier                      |
|              status               |        TEXT         | Batch status (`pending`, `completed`) |
|       batch_created_at_utc        |      DATETIME       | When batch created                    |
|   staging_folder_created_at_utc   |      DATETIME       | Folder creation timestamp             |
|  staging_folder_verified_at_utc   |      DATETIME       | Staging folder verified               |
|       export_started_at_utc       |      DATETIME       | Export start timestamp                |
|      export_finished_at_utc       |      DATETIME       | Export end timestamp                  |
|       upload_started_at_utc       |      DATETIME       | Upload start timestamp                |
|      upload_finished_at_utc       |      DATETIME       | Upload end timestamp                  |
| google_highlights_detected_at_utc |      DATETIME       | When Google highlights detected       |
|    final_curation_done_at_utc     |      DATETIME       | When final curation completed         |
|     cleanup_completed_at_utc      |      DATETIME       | Cleanup completion timestamp          |

### Table: `assets`

Stores asset-level metadata and status flags.

|        Column         |        Type         | Description                               |
| :-------------------: | :-----------------: | :---------------------------------------- |
|          id           | INTEGER PRIMARY KEY | Unique ID                                 |
|       asset_id        |        TEXT         | Apple Photos asset UUID                   |
|       file_hash       |        TEXT         | SHA-1 file hash of the original asset     |
|         month         |        TEXT         | Month identifier (YYYY-MM)                |
|       import_id       |        TEXT         | Related import UUID                       |
|    aesthetic_score    |        FLOAT        | Apple Photos aesthetic ML score (0.0–1.0) |
|   original_filename   |        TEXT         | Filename of the asset                     |
|   date_created_utc    |      DATETIME       | Original creation timestamp (UTC)         |
|   imported_date_utc   |      DATETIME       | When imported into Apple Photos (UTC)     |
| score_imported_at_utc |      DATETIME       | When aesthetic score was synced (UTC)     |
|    google_favorite    |       INTEGER       | 1 if starred in Google Photos, else 0     |
|  uploaded_to_google   |       INTEGER       | 1 if uploaded, else 0                     |
|    created_at_utc     |      DATETIME       | When added to Media Organizer DB          |
|    updated_at_utc     |      DATETIME       | Last update timestamp                     |

### Table: `log_entries`

Tracks pipeline operations, errors, and timestamps.

|     Column     |    Type    | Description                     |
| :------------: | :--------: | :------------------------------ |
|       id       | INTEGER PK | Unique ID                       |
|  module_name   |    TEXT    | Script/module name              |
|   log_level    |    TEXT    | Info, Debug, Warning, Error     |
|  message_text  |    TEXT    | Logged message content          |
| created_at_utc |  DATETIME  | When the log entry was recorded |

### Table: `smart_albums`

Caches Apple Photos Smart Album metadata.

|     Column      |    Type    | Description                     |
| :-------------: | :--------: | :------------------------------ |
|       id        | INTEGER PK | Unique ID                       |
|      title      |    TEXT    | Album name (e.g. "2025-04")     |
|  parent_folder  |    TEXT    | Folder under which album exists |
|    full_path    |    TEXT    | Full album path (for reference) |
| verified_at_utc |  DATETIME  | Last verified timestamp         |

---

# ✨ Notes

- **Manual Smart Album creation**: AppleScript cannot create Smart Albums — must be manually created before export.
- **Dry-run mode**: Available in critical scripts like verification and uploads.
- **Orphan Handling**: Orphaned folders will be renamed, _not deleted_ immediately.
- **Failure recovery**: Pipeline designed to be safe, resumable, and auditable at each stage.

---

# 🏁 End of README (Draft 1)

Setup virtual environment:

> python3 -m venv .venv

Activate virtual environment:

> source .venv/bin/activate

> pip3 install -r requirements.txt

TODO: Start to think how to handle the reupload of the same month - either because assets count has been changed or because more space is availble on the drive
- in upload scropt - if current status is 399 need to reset the list of uploaded files like 2025-05
