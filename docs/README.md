# 📋 Media Organizer Pipeline Overview

This pipeline handles the curation, export, and publishing of family photos, from organizing assets in Apple Photos to exporting and sharing curated memories.

## 🚀 Pipeline Execution Map

The system is orchestrated by two primary scripts.

### 1. Planning Phase (`scripts/pipeline_planner.py`)

Runs every time you start a session. It handles the "Bootstrap" steps:

1.  **Refresh DB**: `copy_all_media_photos_db.py`
2.  **Schema Check**: `storage_manager_main.py`
3.  **Raw Sync**: `sync_photos_raw.py`
4.  **Derived Sync**: `sync_photos_derived.py`
5.  **Batch Detection**: `generate_month_batches.py`

### 2. Execution Phase (`scripts/pipeline_executor.py`)

Carries out the "Stage" steps for the planned month:

- **100 - Verify**: `verify_smart_album.py`
- **200 - Export**: `export_photos_applescript.py`
- **210 - Dedupe**: `deduplicate_assets.py`
- **400 - Upload**: `upload_to_google_photos.py`
- **550 - Favorites**: `pull_google_favorites.py`
- **600 - Rank**: `rank_assets_by_score.py`
- **650 - Cleanup**: `delete_google_assets.py`

---

## 🛠️ Daily Workflow

The pipeline is split into two phases: **Planning** and **Execution**.

### Step 1: Plan the Session

```bash
# Interactive mode (recommended)
python3 scripts/pipeline_planner.py

# Headless mode (automatically applies bootstrap and confirms transitions)
python3 scripts/pipeline_planner.py --auto-apply
```

### 2. Execute the Plan

Once a month is "planned," run the executor to perform the actual file operations (exporting, deduplicating, uploading).

```bash
# Run the planned execution
python3 scripts/pipeline_executor.py

# Safety check - log actions without performing them
python3 scripts/pipeline_executor.py --dry-run
```

---

## 📂 Stage 0 – Initialization & Setup

| Step | Label        | Description                                          | Script/Tool                   | Status    |
| ---- | ------------ | ---------------------------------------------------- | ----------------------------- | --------- |
| 0.0  | Refresh DB   | Copy Apple Photos SQLite DB for safe processing      | `copy_all_media_photos_db.py` | ✅ Stable |
| 0.1  | Init/Migrate | Create schema and apply pending migrations           | `storage_manager_main.py`     | ✅ Stable |
| 0.2  | Raw Sync     | Copy raw ZASSET and attribute tables from Photos     | `sync_photos_raw.py`          | ✅ Stable |
| 0.3  | Derived Sync | Extract scores, creation dates, and calculate months | `sync_photos_derived.py`      | ✅ Stable |

---

## 📦 Stage 1 – Monthly Batch Detection (Handled by Planner)

| Step | Label        | Description                                            | Script/Tool                 | Status     |
| ---- | ------------ | ------------------------------------------------------ | --------------------------- | ---------- |
| 1.1  | Detect Gaps  | Identify month(s) missing from processed batch records | `generate_month_batches.py` | ✅ Working |
| 1.2  | Create Batch | Create new `month_batches` entries; detect orphans     | `generate_month_batches.py` | ✅ Working |

---

## 📤 Stage 2 – Apple Photos Export & Upload Prep

| Step  | Label          | Description                                              | Script/Tool                       | Status      |
| ----- | -------------- | -------------------------------------------------------- | --------------------------------- | ----------- |
| 2.1   | Check Album    | Verify Smart Album for export exists                     | `verify_smart_album.py`           | ✅ Required |
| 2.2   | Export Assets  | AppleScript export from Photos to monthly staging folder | `export_photos_applescript.py`    | ✅ Working  |
| 2.3   | Verify Folder  | Confirm export folder exists and matches expected batch  | `scripts/utils/verify_staging.py` | ✅ Working  |
| 2.3.5 | Sync Metadata  | Sync derived metadata (scores/dates) from Photos         | `sync_photos_derived.py`          | ✅ Working  |
| 2.4   | Upload to G.Ph | Upload curated batch to Google Photos API                | `upload_to_google_photos.py`      | ✅ Working  |

---

### Stage 3 – AI Review & Asset Scoring

| #     | Step                        | Description                                                                          | Tool / Script                        | Status    |
| ----- | --------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------ | --------- |
| 3.1   | Wait for Google AI          | Wait for Google Photos to generate Memories and Highlights (auto-curated)            | Manual (Google Photos)               | ✅ Manual |
| 3.2   | Star Curated Items          | Manually star the best photos suggested by Google (or personal choice)               | Manual (Google Photos)               | ✅ Manual |
| 3.2.5 | Pull Google Favorites       | Use Google Photos API to fetch starred media and update `is_google_favorite` in DB   | `pull_google_favorites.py`           | ✅ Done   |
| 3.3   | Sync Apple Aesthetic Scores | Enrich DB with aesthetic scores and metadata from Apple Photos                       | `sync_photos_derived.py`             | ✅ Done   |
| 3.4   | Rank Assets by Score        | Combine Apple score + Google Favorite flag using weighted score; store session in DB | `rank_assets_by_score.py`            | ✅ Done   |
| 3.5   | Export Ranked Assets        | Export scored assets to curated folder for human review                              | Built into `rank_assets_by_score.py` | ✅ Done   |

### Stage 4 – Final Curation & Cleanup

| #     | Step                         | Description                                                           | Tool / Script                       | Status     |
| ----- | ---------------------------- | --------------------------------------------------------------------- | ----------------------------------- | ---------- |
| 4.1   | Final Human Review           | Manual curation of exported assets (visual/manual check)              | Manual                              | ✅ Manual  |
| 4.1.5 | Export Human-Approved Assets | Copy human-approved subset to a separate monthly subfolder in staging | _(TBD: `export_curated_subset.py`)_ | ❌ Pending |
| 4.2   | Drive Cleanup                | Manual cleanup of Google Photos assets to free storage                | `delete_google_assets.py`           | ✅ Manual  |
| 4.3   | Archive Final Staging Folder | Move finished folders to archive location or external storage         | `archive_staging_folder.py`         | ❌ Pending |

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

## 📌 Step 3.1.5 – Pull Favorites from Google Photos

After Google Photos auto-curates memories and before final ranking, this step pulls the list of media items marked as ⭐ Favorites in Google Photos.

- Downloads favorite status using the API
- Matches favorites to the assets in the local database (by filename and timestamp)
- Updates `assets` table with `is_google_favorite = true` for matching records

## 📌 Step 3.2 – Manual Curation via Google Photos App

After uploading a full month of photos to Google Photos, wait for the auto-curated **Memory Collection** to appear (e.g., “Best of March 2025”). These are not Albums and are not accessible via API.

Use the Google Photos app (mobile or web) to view the generated Memories, and ⭐️ mark your favorite photos manually. These starred photos will later be retrieved via API in the next step (550) for export, curation, and cleanup.

> ℹ️ Step 0.3 (`sync_photos_derived.py`) is required for syncing and enriching asset metadata (aesthetic scores) before uploading to Google Photos.

---

## 🗂️ Database Schema

The pipeline database structure and table definitions can be found in the Database Schema Reference.

---

## 🛠️ Utilities

### Reset Batch State

If you need to re-process a month (e.g., you deleted files from Google and want to re-upload), use this script to reset the database flags and status code.

```bash
python3 scripts/utils/reset_batch_state.py 2026-03
```

This clears the `uploaded_to_google` and `google_favorite` flags for the month and sets the status back to `210` (Ready to Upload).

### List Google Photos Albums

Useful for diagnostic checks and verifying that curation albums are being created correctly.

```bash
python3 scripts/list_google_photos_albums.py --filter "Curating"
```

### Verify Staging Folder

Perform a manual sanity check on a specific staging folder to ensure file counts and naming conventions match.

```bash
python3 scripts/verify_staging.py 2026-03
```

### Create Ranked Assets View

A maintenance tool to ensure the SQL view used for asset ranking and scoring is correctly initialized in the database.

```bash
python3 scripts/create_ranked_assets_view.py
```

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

Latest observations from 10/11/2025 development:

- older months Ex 2025-02 and 2025-01 do not properly pull the favorites from Google Photos - most likely related to the API deprication and scope. Need to develop a plan to reupload those assets.
  -- following planner suggestion - 2024-12 should upload new files to Photos ...
