### 📦 Batch Status Table

This table defines all batch lifecycle statuses stored in the `batch_status` SQLite table across the Media Organizer Pipeline.

| Code | Short Label | Full Description | Stage | Script Invoked | Transition Type | Preceding Code |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **000** | `added` | Batch initialized and added to DB | 1.2 | `generate_month_batches.py` | `pipeline` | — |
| **100** | `assets_verified` | Batch assets verified in Media Organizer DB | 2.1 | `verify_export_album.py {month}` | `pipeline` | 000 |
| **100E**| `verify_failed` | Batch asset verification failed | 2.1 | `verify_export_album.py {month}` | `pipeline` | 000 |
| **200** | `exported` | Batch assets exported directly to staging | 2.2 | `export_photos_wrapper.py {month}` | `pipeline` | 100 |
| **200E**| `export_failed` | Direct export to staging failed | 2.2 | `export_photos_wrapper.py {month}` | `pipeline` | 100 |
| **210** | `deduplicate_assets` | Remove duplicate assets based on extension and size | 2.2.5 | `deduplicate_assets.py {month}` | `pipeline` | 200 |
| **399** | `uploading_to_google` | Upload to Google Account from Staging Folder (Partial) | 2.4 | `upload_to_google_photos.py {month}` | `pipeline` | 210 |
| **400** | `uploaded_to_google` | Upload to Google Account from Staging Folder (Full) | 2.4 | `upload_to_google_photos.py {month}` | `retryable` | 399 |
| **400E**| `Upload Error` | Upload to Google Photos failed | 2.4 | `upload_to_google_photos~.py {month}` | `pipeline` | 399 |
| **500** | `Google Photos Curation` | Google AI creating Memories for the curated month | 3.1 | *(Manual human confirmation or bypass)* | `manual` | 400 |
| **550** | `pull_google_favorites` | Pull Google Photos Favorites and update asset flags | 3.2 | `pull_google_favorites.py` | `pipeline` | 500 |
| **600** | `ranked` | Assets ranked and exported for human review | 3.4 | `rank_assets_by_score.py {month}` | `pipeline` | 550 |
| **650** | `cloud_retained` | Batch completed with cloud archive retention and local staging cleanup | 4.2 | `delete_google_assets.py {month}` | `pipeline` | 600 |
