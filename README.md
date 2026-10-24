# 📋 Media Organizer Pipeline Overview

This pipeline handles the curation, export, and publishing of family photos, using a two-track workflow to manage monthly photo ingest/cleanup and weekly event-based memory curation.

> 💡 **Note**: To view the graphical flowcharts in this documentation (such as [pipeline_architecture.md](file:///Users/nickolaycohen/dev/media-organizer-pipeline/docs/pipeline_architecture.md)), you will need a Mermaid viewer/preview extension installed in your editor (e.g. **Markdown Preview Mermaid Support** for VS Code).

## 🚀 Two-Track Pipeline Architecture

The system is orchestrated by `scripts/pipeline_planner.py` which prompts you to select your track after running the initial bootstrap steps:

### Track 1: Batch Management (Mode `[B]`)
Handles the monthly ingest, sync, upload to Google Photos, ranking, and storage cleanup.
- **100 - Verify**: `verify_export_album.py` (Checks asset readiness in Media Organizer DB)
- **200 - Export**: `export_photos_wrapper.py` (Fast direct $O(1)$ stat export from `originals/` with Live Photo companions)
- **210 - Dedupe**: `deduplicate_assets.py` (De-duplicates temporary staging files)
- **400 - Upload**: `upload_to_google_photos.py` (Uploads to Google Photos and automatically cleans local staging folder)
- **550 - Favorites**: `pull_google_favorites.py` (Syncs Google Photos favorites for the active curated album)
- **600 - Rank**: `rank_assets_by_score.py` (Exports top curated assets to `02-AICurrateList/`, with fallback to master originals)
- **650 - Complete**: `delete_google_assets.py` (Retains assets in Google Photos 5TB cloud archive and ensures local staging is cleaned)

### Track 2: Memory Feature & Publishing (Mode `[M]`)
An independent, time-based workflow for curating and publishing specific events/memories.
- **Identify Candidate**: Ranks Moments by average aesthetic score to suggest a weekly featured Moment.
- **Sync Proposed**: Automatically creates `ToBeCurated/[MomentName]` in Apple Photos and populates it with top ranked assets.
- **Manual Curation**: User reviews and copies final selection to `Curated/[MomentName]` inside Apple Photos.
- **Local Export**: Automatically exports the Apple Photos `Curated` album to the LaCie drive filesystem.
- **Publication Tracking**: Tracks when files are manually uploaded/published to Shutterfly.

---

### 📥 Pre-Intake Triage Gateway (`triage_media_intake.py`)
Acts as the intermediary between raw storage discovery (`drive-audit-cli` wizard) and Apple Photos ingestion (`All-Media.photoslibrary`).
- **Scans**: An intake folder (defaults to `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing`).
- **Compares**: Compares every asset against all 212,000+ assets in `media_organizer.db` using exact byte size, original filename, and EXIF timestamp cascade, plus Live Photo companion pairing.
- **Multi-Drive Triage**: Partitions incoming files across physical drives based on confidence and backup policy:
  - `Duplicates/<session_id>` ➔ **DELL1TB02** (🟠 **Orange Tag**): 100% verified duplicate copies already cataloged in `All-Media.photoslibrary`. Routed to non-backed up disposable storage.
  - `Review-Similar/<session_id>` ➔ **LaCie** (🔴 **Red Tag**): Assets suspected of having been intentionally pruned or discarded from the library (deletion keywords, burst siblings within $\le 30$s, web thumbnails). Routed to primary storage for human review.
  - `New/<session_id>` ➔ **LaCie** (🟢 **Green Tag**): Pristine, high-confidence novel media not found in Apple Photos, ready for direct library import.
- **🔗 Live Photo Companion Pairing**: Pairs `.JPG` + `.MOV` companion files so companion videos correctly follow their duplicate still photo classifications.
- **Package Safeguards**: If an intake directory contains `.photoslibrary` bundles, assets inside them are never deleted; bundle structure is preserved and an end-of-run banner alert reminds the user to manually manage the library package.
- **Cleans**: Automatically prunes empty source directories so categorized assets bubble up cleanly.
- **Bypasses Locks**: Automatically unlocks macOS Time Machine ACLs (`chmod -N`) and immutable flags (`chflags 0`) on the fly.

### 🛡️ Apple Photos Library Triage Gateway (`triage_photos_library.py`)
A dedicated standalone intake and deduplication utility specifically designed for Apple Photos Library packages (`.photoslibrary` / `.photolibrary`):
- **100% Read-Only Full Replica Mode**: The source library bundle is strictly read-only (zero moves, zero deletions, zero modifications).
- **Original Filename Restoration with UUID Fallback**: Automatically extracts camera filenames (`IMG_1234.JPG`, `GOPR0012.JPG`, `<UUID>_3.mov` ➔ `IMG_1234.MOV`) from the internal SQLite database (`Photos.sqlite`). If an asset's original filename cannot be resolved, it cleanly falls back on extracting and using its official UUID (`<UUID>.<ext>`), stripping internal artifacts like `_3.mov`.
- **Targeted Traversal**: Automatically navigates internal `originals/` or `Masters/` while skipping internal thumbnail caches (`resources/`), face crops, and internal SQLite databases (`private/`, `database/`, `scopes/`).
- **Resilient Matching**: Normalizes Apple Photos UUID companion video stems and evaluates assets against `All-Media.photoslibrary` catalog indexes for fast Tier 1/2 matching.
- **Safe Replication & Ignore Duplicates**: Copies confirmed duplicates to DELL1TB02 (`Duplicates/<triage_id>`) and novel assets to LaCie (`New/<triage_id>`). Use `--ignore-duplicates` to skip copying duplicate files entirely, replicating only `Review-Similar` and `New` assets to LaCie.
- **Action Alert**: Prompts user upon completion that the source library package remains untouched and can be manually reviewed or deleted.

### 🔧 UUID Session Reconciler (`reconcile_triage_session_movs.py`)
Companion utility to audit past triage sessions containing Apple Photos UUID files, verifying exact byte matches against `All-Media.photoslibrary` and relocating confirmed duplicates from `New/` to `Duplicates/`.

### 📊 Master Library Duplicates Report (`report_library_duplicates.py`)
Generates comprehensive asset distribution and duplicate analysis across all 212,000+ assets in `All-Media.photoslibrary` by year (1969–2026), calculating duplicate percentages, unique counts, and wasted redundant storage.

### 🛡️ Intake Triage Deduplication Report (`report_triage_deduplication.py`)
Audits all intake triage sessions across external hard drives, reporting cumulative duplicate volume diverted, review-similar routed, novel media discovered, and individual per-session breakdowns.

### ⏳ Triage Temporal Isolation Report (`report_triage_isolated_assets.py`)
Analyzes non-duplicate assets in `New` and `Review-Similar`, computing the closest preceding and succeeding master library assets by date and bubbling up the most temporally isolated, unique media.

```bash
# Preview standard intake triage without moving files (dry run)
python3 scripts/triage_media_intake.py --dry-run

# Triage default folder (/Volumes/LaCie/Storage/Priority 4/Media Preprocessing)
python3 scripts/triage_media_intake.py

# Triage an Apple Photos Library as an explicit read-only source (Full Replica mode)
python3 scripts/triage_photos_library.py "/Volumes/Extreme Pro/tmp/iPhone13ProMax Library.photoslibrary" --dry-run
python3 scripts/triage_photos_library.py "/Volumes/Extreme Pro/tmp/iPhone13ProMax Library.photoslibrary"

# Triage a Photos Library skipping duplicate copies (only replicate Review-Similar and New to LaCie)
python3 scripts/triage_photos_library.py "/path/to/Library.photoslibrary" --ignore-duplicates

# Reconcile UUID assets from a previous triage session (e.g. session 37)
python3 scripts/reconcile_triage_session_movs.py --triage-id 37

# Generate Master Library yearly asset distribution & duplicate report
python3 scripts/report_library_duplicates.py
python3 scripts/report_library_duplicates.py --show-clusters 5

# Generate Intake Triage deduplication summary & inspect specific session
python3 scripts/report_triage_deduplication.py
python3 scripts/report_triage_deduplication.py --session 41

# Rank non-duplicate triage assets bubbling up the most temporally isolated/unique media
python3 scripts/report_triage_isolated_assets.py --top 30
python3 scripts/report_triage_isolated_assets.py --min-gap-days 7
```

---

## 🛠️ Daily Workflow

The pipeline is split into two phases: **Database Ingestion/Refresh** and **Interactive Planning & Execution**.

### Step 1: Run the Database Refresh Service (Background)

The database copy, SQLite index verification/repair, and metadata sync operations are offloaded to an independent background service. This prevents blocking your interactive planning session:

```bash
# Run the database refresh and metadata sync sequence in a separate terminal window
python3 scripts/bg_copy_db_service.py
```
This runs the full ingestion pipeline: copying the database copy, running storage manager migrations, syncing raw tables, and syncing derived attributes (aesthetic scores, dates).

### Step 2: Plan the Session (Interactive)

The planner starts up near-instantly since the database refresh runs in the background:

```bash
# Run the interactive planning menu
python3 scripts/pipeline_planner.py
```

* **Locking**: The planner and background service use a shared bidirectional lock. The planner will automatically wait and probe every 10 seconds if a background sync is currently running, and the background service will exit immediately if you are currently using the interactive planner to prevent write contention.
* **Stale Warnings**: At startup, the planner checks if the source Photos database contains changes since the last background refresh. If it does, it will log a warning indicating that a refresh is recommended.

### Step 3: Execute the Plan (For Batch Management)
Once a monthly batch is planned under Batch Management, run the executor to perform the file operations.
```bash
python3 scripts/pipeline_executor.py
```

---



## 📂 Stage 0 – Initialization & Setup (Handled in background by `bg_copy_db_service.py`)

All of the following steps are executed sequentially by running `python3 scripts/bg_copy_db_service.py`:

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

### Reconstruct Apple Photos Album Structure & Members (Disaster Recovery)

In the event of a catastrophic failure, corruption, or rebuild of the Apple Photos library, this tool uses `media_organizer.db` as an authoritative backup to recreate all folders, albums, and member assets under **"Media Organizer on LaCie"**.

- **Automatic Backups**: The background refresh service (`bg_copy_db_service.py` via `sync_photos_derived.py`) automatically snapshots all folder paths, album kinds, and member asset UUIDs into `media_organizer_albums` and `media_organizer_album_assets`.
- **Scheduled Snapshot**: Scheduled daily via cron at 5:30 AM before the 6:00 AM database backup (logs to `logs/reconstruct_apple_albums.log`).
- **Manual Snapshot**: You can take a manual backup snapshot anytime:
  ```bash
  python3 scripts/reconstruct_apple_albums.py --backup
  ```
- **Dry-Run Preview**: Preview the hierarchy, album names, and asset counts before making changes:
  ```bash
  python3 scripts/reconstruct_apple_albums.py --recreate --dry-run
  ```
- **Full Reconstruction**: Recreate all folders, albums, and restore member assets in Apple Photos:
  ```bash
  python3 scripts/reconstruct_apple_albums.py --recreate
  ```
- **Selective Reconstruction**: Recreate only a specific subfolder tree (e.g. `Curated`, `Moments`, `Retired`) or a single album:
  ```bash
  python3 scripts/reconstruct_apple_albums.py --recreate --folder "Curated"
  python3 scripts/reconstruct_apple_albums.py --recreate --album "2026 - Venice"
  ```
- **Verification**: Compare existing Apple Photos albums and item counts against the database backup:
  ```bash
  python3 scripts/reconstruct_apple_albums.py --verify --folder "Curated"
  ```

---

### 📥 Pre-Intake Media Triage Gateway (`triage_media_intake.py`)

A high-performance, two-drive intake gateway bridging raw storage discovery (`drive-audit-cli`) and Apple Photos ingestion (`All-Media.photoslibrary`). It indexes all 212,000+ library assets into in-memory lookup hashes ($O(1)$ size + filename / EXIF cascade), pairs Live Photo companions (`.JPG` + `.MOV`), and routes uncurated files across physical drives with automatic macOS Finder color tagging.

#### 🎯 Multi-Drive Destination Architecture
To protect primary backed-up storage, assets are partitioned across physical drives based on confidence:
- **DELL1TB02 (Non-backed up cold/disposable tier)**:
  - `Duplicates/<triage_id>` (🟠 **Orange Tag**): 100% verified duplicate copies already cataloged in `All-Media.photoslibrary`. Safe to unmount, ignore from cloud backup, or delete in bulk.
- **LaCie (Primary backed-up storage)**:
  - `Review-Similar/<triage_id>` (🔴 **Red Tag**): Assets suspected of having been intentionally pruned or discarded from the library (deletion keywords in folder names, burst/sequence siblings within $\le 30$s of library photos, low-res web thumbnails, or screenshot artifacts). Held for human review.
  - `New/<triage_id>` (🟢 **Green Tag**): Pristine, high-confidence novel media not found in Apple Photos, ready for direct library import.

#### 🛡️ Apple Photos Package Safeguards
If a scanned intake folder contains an Apple Photos Library bundle (`.photoslibrary` / `.photolibrary`):
- **Zero In-Place Deletions**: Files inside the package are safely copied rather than moved.
- **Directory Protection**: The internal bundle structure is strictly preserved (never pruned).
- **Session Alert**: A prominent completion banner notifies the user that a Photos Library package was encountered and should be manually reviewed or deleted.
*(Note: To directly scan and process a `.photoslibrary` bundle itself, use the dedicated standalone utility [`triage_photos_library.py`](#%EF%B8%8F-apple-photos-library-intake--replication-gateway-triage_photos_librarypy) detailed below).*

#### ⚙️ CLI Options & Help Reference
| Flag / Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `target_dir` | Positional | `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing` | Source intake directory to scan and triage |
| `--dell-dest-dir` | Option | `/Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake` | Destination root on DELL1TB02 for `Duplicates` |
| `--lacie-dest-dir`, `--dest-dir` | Option | `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake` | Destination root on LaCie for `Review-Similar` and `New` |
| `--triage-id` | Option | Auto-increment integer | Custom session ID for session subdirectories (recorded in `triage_sessions` DB table) |
| `--dry-run` | Flag | `False` | Simulates file classification, destination paths, and space calculations without moving files or modifying DB |
| `--no-clean` | Flag | `False` | Prevents automatic pruning of empty source subdirectories after moving assets |
| `--tag-only` | Flag | `False` | Re-applies macOS Finder color tags (🟠 Orange, 🔴 Red, 🟢 Green) to existing triage output folders without re-scanning |

#### 📋 Examples
```bash
# 1. Preview triage run on default intake folder (safe simulation)
python3 scripts/triage_media_intake.py --dry-run

# 2. Triage default intake folder across DELL1TB02 and LaCie
python3 scripts/triage_media_intake.py

# 3. Triage a specific external hard drive or folder
python3 scripts/triage_media_intake.py "/Volumes/DELL1TB02/Archive/Pictures Extra Sequence"

# 4. Use a specific custom session ID
python3 scripts/triage_media_intake.py "/path/to/media" --triage-id "2026-Batch-A"

# 5. Re-apply Finder color tags to all existing triage session directories
python3 scripts/triage_media_intake.py --tag-only
```

---

### 🛡️ Apple Photos Library Intake & Replication Gateway (`triage_photos_library.py`)

A specialized, standalone intake and deduplication engine dedicated to processing Apple Photos Library packages (`.photoslibrary` / `.photolibrary`). It enables safe ingestion of legacy, unbundled, or un-openable Apple Photos libraries (such as those recovered from Time Machine backups, older macOS versions, or iOS system extractions) in **100% Read-Only Full Replica mode**.

#### 🌟 Key Capabilities & Safety Guarantees
- **100% Read-Only Source Guarantee**: The source `.photoslibrary` is strictly protected:
  - **Zero Moves or Deletions**: Files inside the package are never moved, deleted, renamed, or modified.
  - **Zero Bundle Mutation**: No empty folder pruning is performed within the source library tree.
- **Original Filename Restoration with UUID Fallback**:
  - Automatically queries the library's internal SQLite database (`database/Photos.sqlite`, `database/photos.db`, or `Photos.sqlite`) using an ephemeral local copy to bypass mechanical disk seek latency.
  - Resolves internal UUID filenames (`0000609D-6414-4E51-A7CF-83B6283E8632.jpeg` ➔ `IMG_4614.JPG`) and Live Photo companion video suffixes (`<UUID>_3.mov` ➔ `IMG_4614.MOV`).
  - Evaluates assets using their true camera filenames for instant Tier 1/2 catalog matching against `All-Media.photoslibrary`.
  - **UUID Extraction Fallback**: If an asset's original filename is missing, NULL, or unresolvable in SQLite, the system automatically falls back on extracting the clean 36-character UUID from the database (`ZASSET.ZUUID`) or parsing it directly from the filename/path. Companion videos (`<UUID>_3.mov`) are cleanly normalized to `<UUID>.mov`, ensuring Live Photo pairing and clean file exports even without original camera names.
  - Replicates novel (`New/`) and review (`Review-Similar/`) media with restored original filenames (with automatic collision disambiguation `_dup1`, `_dup2` if identical filenames occur across dates), while preserving original relative paths in `triage_session_assets` for full traceability.
- **Intelligent Bundle Traversal**:
  - Automatically targets inner media storage (`originals/` or legacy `Masters/`).
  - Automatically excludes internal thumbnail caches (`resources/`), face crops, derivatives, and SQLite internal state (`private/`, `database/`, `scopes/`).
- **Non-Destructive Full Replication**:
  - Confirmed duplicates are copied to DELL1TB02 (`Duplicates/<triage_id>/...`) using `shutil.copy2` (preserving metadata and EXIF).
  - Suspected discard/burst siblings or upgrade candidates are copied to LaCie (`Review-Similar/<triage_id>/...`).
  - Truly net-new assets are safely copied to LaCie (`New/<triage_id>/...`), ready for clean import into your master library.
- **Live Photo Companion & UUID Normalization**:
  - Automatically resolves Live Photo companion videos named with Apple Photos UUID patterns (`<UUID>_3.mov` ➔ `<UUID>`), ensuring companion videos inherit the exact classification of their still photo counterpart (`.heic` / `.jpeg`).
  - Queries exact byte sizes (`aaa.ZORIGINALFILESIZE`) across all 212,000+ assets in `All-Media.photoslibrary` to recognize standalone UUID assets even when original camera filenames are unavailable.
- **Finder Color Tagging & Audit Trail**:
  - Automatically applies macOS Finder color tags (🟠 Orange on DELL1TB02, 🔴 Red and 🟢 Green on LaCie).
  - Records session history and per-asset classifications in `triage_sessions` and `triage_session_assets` SQLite tables.
- **Post-Scan Manual Action Alert**: Displays an explicit notification banner at session completion reminding the user that the source library package remains completely intact and can now be safely reviewed or deleted manually.

#### ⚙️ CLI Options & Help Reference
| Flag / Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `library_path` | Positional | *(Required)* | Path to the Apple Photos Library bundle (`.photoslibrary` / `.photolibrary`) |
| `--dell-dest-dir` | Option | `/Volumes/DELL1TB02/Storage/Priority 4/Media Preprocessing/TriageMediaIntake` | Destination root on DELL1TB02 for `Duplicates` |
| `--lacie-dest-dir`, `--dest-dir` | Option | `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake` | Destination root on LaCie for `Review-Similar` and `New` |
| `--triage-id` | Option | Auto-increment integer | Custom session ID for session subdirectories (recorded in `triage_sessions` DB table) |
| `--ignore-duplicates` | Flag | `False` | Skips copying duplicate files to DELL1TB02; only replicates `Review-Similar` and `New` assets to LaCie |
| `--dry-run` | Flag | `False` | Simulates file classification, destination paths, and space calculations without copying files or modifying DB |

#### 📋 Examples
```bash
# 1. Preview triage run on a Photos Library bundle (safe simulation)
python3 scripts/triage_photos_library.py "/Volumes/Extreme Pro/tmp/iPhone13ProMax Library.photoslibrary" --dry-run

# 2. Triage Photos Library in 100% Read-Only Full Replica mode (copies duplicates to DELL, new/review to LaCie)
python3 scripts/triage_photos_library.py "/Volumes/Extreme Pro/tmp/iPhone13ProMax Library.photoslibrary"

# 3. Triage Photos Library skipping duplicate copies (only replicates novel/review media to LaCie)
python3 scripts/triage_photos_library.py "/Volumes/Extreme Pro/tmp/iPhone13ProMax Library.photoslibrary" --ignore-duplicates

# 4. Use custom session ID and verify destination paths
python3 scripts/triage_photos_library.py "/Volumes/Backup/OldPhotos.photoslibrary" --triage-id "2026-iPhone-Intake"
```

---

### 🔧 Triage Session UUID Asset Reconciler (`reconcile_triage_session_movs.py`)

A specialized post-triage remediation utility designed for intake runs where source folders were extracted from Apple Photos libraries (using internal UUID filenames like `<UUID>.mov`, `<UUID>_3.mov`, `<UUID>.heic` instead of original camera filenames). It cross-references assets categorized in `New/<triage_id>` against both the session's duplicate photo mapping and the global Apple Photos database (`ZORIGINALFILESIZE` in `All-Media.photoslibrary`).

#### 🌟 Key Capabilities
- **Live Photo Companion Resolution**: Automatically links `<UUID>_*.mov` companion videos to their counterpart `<UUID>.heic` / `<UUID>.jpeg` still photos that were already identified as duplicates in the same session, re-classifying them as identical duplicates.
- **Exact Byte-Size Master Matching**: Queries `ZORIGINALFILESIZE` across all 212,000+ assets in `All-Media.photoslibrary` to identify standalone videos or photos with 100% exact byte matches against library originals.
- **Physical Relocation & Space Recovery**: Safely moves confirmed duplicate assets from `/Volumes/LaCie/.../New/<session>` to `/Volumes/DELL1TB02/.../Duplicates/<session>`, freeing primary drive storage.
- **Net-New Asset Preservation**: Keeps only verified, truly novel media in `New/<session>`.
- **Atomic Database Updates**: Synchronizes `triage_session_assets` (updating categories, sub-types, and match reasons) and `triage_sessions` (updating `duplicates_count`, `duplicates_bytes`, `new_count`, `new_bytes`).
- **Automatic Folder Cleanup**: Prunes empty directories in `New/<triage_id>` once duplicates are relocated.

#### ⚙️ CLI Options & Help Reference
| Flag / Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--triage-id` | Option | `37` | Triage session identifier to audit and reconcile |
| `--dry-run` | Flag | `False` | Analyzes assets, companion mappings, and byte-size matches without moving files or updating the database |

#### 📋 Examples
```bash
# 1. Preview reconciliation for session 37 without moving files or altering DB
python3 scripts/reconcile_triage_session_movs.py --triage-id 37 --dry-run

# 2. Reconcile session 37, relocate duplicate MOV/media to DELL1TB02, and commit DB records
python3 scripts/reconcile_triage_session_movs.py --triage-id 37
```

---

### 📊 Master Catalog Duplicates & Distribution Report (`report_library_duplicates.py`)

Generates comprehensive asset distribution and duplicate analysis for the primary master library (`All-Media.photoslibrary`) from `media_organizer.db`.

#### 🌟 Key Capabilities
- **Yearly Distribution**: Breaks down 212,000+ library assets by year (1969 to present), showing file counts, storage footprints, unique counts, and duplicate percentages.
- **Configurable Match Criteria**: Detect duplicates by `--match-criteria name_size` (same exact byte size + camera filename, default) or `--match-criteria size_date` (same exact byte size + creation date).
- **Cluster Metrics**: Identifies duplicate clusters, counts redundant extra copies, and calculates total reclaimable disk space.
- **Deep Inspection**: View the top largest duplicate clusters with `--show-clusters N`.
- **Markdown & CSV Export**: Outputs formatted terminal tables, GitHub-flavored Markdown (`--markdown`), or exports to CSV (`--csv <path>`).

```bash
# Default terminal report
python3 scripts/report_library_duplicates.py

# Inspect top 5 largest duplicate clusters with file names, sizes, and dates
python3 scripts/report_library_duplicates.py --show-clusters 5

# Export to CSV for spreadsheet analysis
python3 scripts/report_library_duplicates.py --csv library_duplicates_report.csv
```

---

### 🛡️ Intake Triage Deduplication Report (`report_triage_deduplication.py`)

Audits all intake triage sessions across external hard drives, reporting cumulative duplicate volume diverted, review-similar routed, novel media discovered, and individual per-session breakdowns.

#### 🌟 Key Capabilities
- **Cumulative Intake Metrics**: Displays total scanned media files, confirmed duplicates diverted/skipped, review-similar assets, and high-confidence new media.
- **Storage Savings**: Quantifies total redundant storage eliminated (GB saved) and overall deduplication efficiency percentage.
- **Session Breakdown**: Lists every completed triage session with source folder, date, file counts, and duplicate rates.
- **Deep-Dive Session Inspection**: Drill into any individual session with `--session <id>` to see its detailed classification breakdown (`identical`, `visual_duplicate`, `upgrade_candidate`, `temporal_burst`, `clean_media`) and sample file decision reasons.

```bash
# View cumulative triage metrics and all session breakdowns
python3 scripts/report_triage_deduplication.py

# Drill down into a specific triage session (e.g. Session 41)
python3 scripts/report_triage_deduplication.py --session 41

# Output as GitHub Flavored Markdown
python3 scripts/report_triage_deduplication.py --markdown
```

---

### ⏳ Triage Intake Temporal Isolation & Uniqueness Report (`report_triage_isolated_assets.py`)

Analyzes non-duplicate assets across both `New` and `Review-Similar` intake folders under `/Volumes/LaCie/Storage/Priority 4/Media Preprocessing/TriageMediaIntake/`.

For every triage asset, it searches the master library catalog (`All-Media.photoslibrary`) for the closest preceding and succeeding assets by capture date, calculating the nearest-neighbor temporal isolation gap. Assets are stored in a temporary table and ranked with the furthest temporal distance bubbling up to the top, exposing the most distinct, unique, and unrepresented events.

#### 🌟 Key Capabilities
- **Binary Search Timeline Lookup**: Performs $O(\log N)$ nearest-neighbor lookups against 212,000+ cataloged assets in sub-second time.
- **Nearest Neighbors Display**: Pinpoints both the closest preceding asset (`Closest Before: YYYY-MM-DD (filename) [-Nd]`) and closest succeeding asset (`Closest After: YYYY-MM-DD (filename) [+Nd]`).
- **Isolation Distribution**: Automatically categorizes assets into:
  - 🚀 **Ultra-Isolated (> 30 days gap)**: Completely distinct events/shoots with zero proximate library coverage.
  - 🏖️ **Moderately Isolated (7–30 days)**: Gaps of 1 to 4 weeks.
  - 📍 **Clustered (1–7 days)**: Media taken in the same general week.
  - ⏱️ **Tight Neighbors (< 1 day)**: Captured alongside existing library shoots.
- **Configurable Filters**: Filter by category (`--category {new, review_similar}`), minimum isolation days (`--min-gap-days D`), or session (`--session ID`).
- **Export & Persistence**: Output as formatted terminal tables, GitHub-flavored Markdown (`--markdown`), CSV (`--csv <path>`), or save into a permanent database table (`--save-db-table <name>`).

```bash
# Show top 30 most temporally isolated assets across all intake sessions
python3 scripts/report_triage_isolated_assets.py --top 30

# Filter to assets with at least 14 days of temporal isolation from any master photo
python3 scripts/report_triage_isolated_assets.py --min-gap-days 14

# Filter only clean 'New' assets
python3 scripts/report_triage_isolated_assets.py --category new --top 25

# Export full ranking to CSV
python3 scripts/report_triage_isolated_assets.py --csv triage_isolated_assets.csv
```

---

### 📁 Recursive Directory Merger & Deduplication Utility (`merge_directories.py`)

A production-grade directory consolidation and migration utility for macOS and Linux. It recursively merges all files from a source directory into a destination directory, preserving the nested relative folder hierarchy, deduplicating identical files, resolving collisions, and auto-bypassing macOS file locks and Time Machine ACLs.

#### 🌟 Key Capabilities
- **Move vs Copy Mode**: Default `--move` mode transfers files and prunes empty source folders; `--copy` mode replicates content without altering the source tree.
- **Byte-by-Byte Chunk Deduplication**: Compares candidate collisions using 64KB chunk-based byte streams:
  - In **Move Mode**: Safely removes the redundant source copy (safe deduplication without duplicating data).
  - In **Copy Mode**: Skips re-copying existing identical files.
- **Collision Strategies**: For non-identical files with matching relative paths, choose `--on-collision {rename,overwrite,skip,newer}` (defaults to non-destructive rename appending `_dup1`, `_dup2`).
- **macOS Time Machine ACL & Lock Bypass**: Automatically removes immutable flags (`chflags 0`) and Time Machine deny ACLs (`chmod -N`) whenever a `PermissionError (Errno 13)` is encountered, ensuring smooth transfers even from locked system backup folders.
- **Dynamic Progress & Audit Logging**: Real-time progress bar in the terminal plus structured audit logging to `logs/merge_directories.log`.
- **Dry-Run Mode**: Full preview simulation reporting total file count, volume size, collision preview, and planned actions.

#### ⚙️ CLI Options & Help Reference
| Flag / Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `source` | Positional | *(Required)* | Source directory containing files to merge |
| `destination` | Positional | *(Required)* | Destination directory to merge into |
| `--move` | Flag | Enabled (Default) | Moves files from source to destination and deletes emptied source folders |
| `--copy` | Flag | `False` | Copies files from source to destination, leaving source files intact |
| `--on-collision` | Choice | `rename` | Collision resolution for non-identical files: `rename` (`_dupX`), `overwrite`, `skip`, or `newer` |
| `--dry-run` | Flag | `False` | Simulates file scanning, size totals, and collision outcomes without transferring files or altering directories |
| `--no-clean` | Flag | `False` | In move mode, prevents deletion of empty source folders after files have been moved |

#### 📋 Examples
```bash
# 1. Preview merge between two folders or drives (safe simulation)
python3 scripts/merge_directories.py "/Volumes/LaCie/Storage/Priority 3" "/Volumes/DELL1TB02/Storage/Priority 3" --dry-run

# 2. Move and merge files recursively (default move mode with auto-cleanup of empty source folders)
python3 scripts/merge_directories.py "/Volumes/LaCie/Storage/Priority 3" "/Volumes/DELL1TB02/Storage/Priority 3"

# 3. Copy files recursively without removing source files
python3 scripts/merge_directories.py "/path/to/source" "/path/to/destination" --copy

# 4. Overwrite conflicting files at the destination if different
python3 scripts/merge_directories.py "/path/to/source" "/path/to/destination" --on-collision overwrite

# 5. Skip non-identical files that already exist at destination
python3 scripts/merge_directories.py "/path/to/source" "/path/to/destination" --on-collision skip
```

---

# ✨ Notes

- **Direct Database Export**: No manual Smart Album creation needed — exports are handled directly from the Photos database and library `originals/` directory with $O(1)$ stat lookup.
- **5TB Cloud Archive**: Photos uploaded to Google Photos remain stored permanently; batches do not require manual deletion from Google Photos.
- **Automated Local Staging Cleanup**: Staging folders under `01-MonthlyExports` are automatically purged after upload (`status 400`) and post-ranking (`status 600`), keeping local disk usage bounded to the active batch (~10–30 GB).
- **Staging Cleanup Utility**: Use `python3 scripts/cleanup_staging_exports.py [--dry-run]` to audit or reclaim staging space at any time.
- **Dry-run mode**: Available in critical scripts like verification, cleanup, and uploads.
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

---

## 📌 Track 2 – Memory Feature & Publishing Workflow (Weekly Schedule)

This is an independent workflow focused on curating and publishing specific events/memories (Moments) once their source batches have been processed (status ≥ 600).

### Curation Lifecycle Stages:
- **M100 - Selected / Candidate**: Moment is ranked and selected for weekly promotion based on aesthetic quality.
- **M200 - Sync Proposed**: Pipeline runs AppleScript to sync proposed ranked assets from Apple Photos to the `ToBeCurated/[MomentName]` folder.
- **M300 - Curating**: User manually selects assets inside Apple Photos and copies them to the `Curated/[MomentName]` folder.
- **M400 - Exported**: Pipeline runs `export_curated_album.py` to copy Apple Photos `Curated` assets to the filesystem curated directory `/Volumes/LaCie/Media Organizer/Curated/[MomentName]`.
- **M450 - Partially Published**: Some curated assets have been successfully published/uploaded to Shutterfly/YouTube and recorded in the database, but others remain unpublished.
- **M500 - Published**: User uploads all the curated assets to Shutterfly, and the publication is tracked in the database.

To manage memories, run `python3 scripts/pipeline_planner.py` and select `[M]`.
