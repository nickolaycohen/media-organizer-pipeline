### 📦 Batch Status Table

This table defines all batch lifecycle statuses used across the Media Organizer Pipeline.

| Code | Short Label         | Full Description                                 | Stage | Script (if applicable)               |
| ---- | ------------------- | ------------------------------------------------ | ----- | ------------------------------------ |
| 000  | `init`              | Batch initialized and added to DB                | 1.2   | `generate_month_batches.py`          |
| 100  | `album_verified`    | Smart Album verified for current month           | 2.1   | `verify_smart_album.py`              |
| 200  | `exported`          | Photos exported to staging                       | 2.2   | `export_photos_applescript.py`       |
| 210  | `deduplicated`      | Duplicate assets removed; ready for upload       | 2.2.5 | `deduplicate_assets.py`              |
| 399  | `partial_upload`    | Partial upload completed (insufficient space)    | 2.4   | `upload_to_google_photos.py`         |
| 400  | `uploaded`          | Full upload to Google Photos completed           | 2.4   | `upload_to_google_photos.py`         |
| 550  | `favorites_pulled`  | Google Photos favorites pulled and synced        | 3.2.5 | `pull_google_favorites.py`           |
| 600  | `ranked`            | Assets ranked and exported for human review      | 3.4   | `rank_assets_by_score.py`            |
| 700  | `approved_exported` | Human-approved assets exported to curated folder | 4.1.5 | `export_curated_subset.py` (planned) |
| 800  | `cleaned`           | Non-favorites removed from Google Photos         | 4.2   | `delete_nonfavorites_google.py`      |
| 900  | `archived`          | Staging folder archived and batch finalized      | 4.3   | `archive_staging_folder.py`          |
