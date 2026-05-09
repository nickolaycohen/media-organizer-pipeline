# 🗂️ Database Schema Reference

This document defines the structure of the `media_organizer.db` SQLite database.

## Tables

### 1. `imports`

Tracks import sessions from Apple Photos.
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY | Unique ID |
| `uuid` | TEXT | Import session UUID |
| `import_timestamp_utc` | DATETIME | Timestamp of import |
| `import_name` | TEXT | Human-friendly display name |
| `camera_make` | TEXT | Manufacturer (e.g. Apple) |
| `camera_model` | TEXT | Specific model (e.g. iPhone 16 Pro) |
| `album` | TEXT | Album name if provided during import |
| `asset_count` | INTEGER | Number of assets in the import session |
| `months_detected` | TEXT | Comma-separated list of included months |
| `status_code` | TEXT | Current lifecycle status of the import |
| `min_filename` | TEXT | Smallest filename (alphabetically) in the import |
| `max_filename` | TEXT | Largest filename (alphabetically) in the import |
| `sequencing_confirmed` | INTEGER | 1 if user confirmed sequencing is reasonable, else 0 |
| `execution_id` | TEXT | UUID of the pipeline session that processed it |
| `created_at_utc` | DATETIME | When record was added to the database |

### 2. `month_batches`

Tracks the status and timestamps of monthly processing units.
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY | Unique ID |
| `month` | TEXT (YYYY-MM) | Month identifier |
| `status_code` | TEXT | Current lifecycle status code (see Batch Status Codes) |
| `batch_created_at_utc` | DATETIME | When batch record was created |
| `staging_folder_created_at_utc` | DATETIME | Folder creation timestamp |
| `staging_folder_verified_at_utc` | DATETIME | Staging folder verified |
| `export_started_at_utc` | DATETIME | Export start timestamp |
| `export_finished_at_utc` | DATETIME | Export end timestamp |
| `upload_started_at_utc` | DATETIME | Upload start timestamp |
| `upload_finished_at_utc` | DATETIME | Upload end timestamp |
| `google_highlights_detected_at_utc` | DATETIME | When Google highlights detected |
| `final_curation_done_at_utc` | DATETIME | When final curation completed |
| `cleanup_completed_at_utc` | DATETIME | Cleanup completion timestamp |

### 3. `assets`

Stores asset-level metadata, aesthetic scores, and sync flags.
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PRIMARY KEY | Unique ID |
| `asset_id` | TEXT | Apple Photos asset UUID |
| `file_hash` | TEXT | SHA-256 file hash |
| `month` | TEXT | Month identifier (YYYY-MM) |
| `import_id` | TEXT | Related import UUID |
| `aesthetic_score` | FLOAT | Apple Photos aesthetic ML score (0.0–1.0) |
| `original_filename` | TEXT | Filename of the asset |
| `date_created_utc` | DATETIME | Original creation timestamp (UTC) |
| `imported_date_utc` | DATETIME | When imported into Apple Photos (UTC) |
| `score_imported_at_utc` | DATETIME | When aesthetic score was synced (UTC) |
| `google_favorite` | INTEGER | 1 if starred in Google Photos, else 0 |
| `uploaded_to_google` | INTEGER | 1 if uploaded, else 0 |
| `created_at_utc` | DATETIME | When added to Media Organizer DB |
| `updated_at_utc` | DATETIME | Last update timestamp |

### 4. `log_entries`

Tracks pipeline operations and script errors.
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PK | Unique ID |
| `module_name` | TEXT | Script/module name |
| `log_level` | TEXT | Info, Debug, Warning, Error |
| `message_text` | TEXT | Logged message content |
| `created_at_utc` | DATETIME | When the log entry was recorded |

### 5. `smart_albums`

Caches Apple Photos Smart Album metadata.
| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER PK | Unique ID |
| `title` | TEXT | Album name (e.g. "2025-04") |
| `parent_folder` | TEXT | Folder under which album exists |
| `full_path` | TEXT | Full album path (for reference) |
| `verified_at_utc` | DATETIME | Last verified timestamp |

### 6. `batch_status`

Defines the pipeline state machine.
| Column | Type | Description |
| :--- | :--- | :--- |
| `code` | TEXT (PK) | Status code (e.g., '100', '210', '399') |
| `preceding_code` | TEXT | The code required before this transition |
| `short_label` | TEXT | Human-readable label |
| `full_description` | TEXT | Detailed stage description |
| `transition_type` | TEXT | `pipeline`, `manual`, or `retryable` |
