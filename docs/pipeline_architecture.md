# 📋 Media Organizer Pipeline Architecture

This document outlines the pipeline orchestration flow and design patterns for both Track 1 (Monthly Ingest) and Track 2 (Weekly Moments Curation) workflows.

---

## 🚀 Two-Track Pipeline Architecture

The system is orchestrated by `scripts/pipeline_planner.py` which manages two independent tracks:

### Track 1: Batch Management (Monthly Ingest & Storage Lifecycle)
This track handles the lifecycle of raw photos for each month, starting with smart album detection and ending with Google Drive cleanup.

```mermaid
flowchart TD
    classDef state fill:#1e1e2e,stroke:#cdd6f4,stroke-width:1px,color:#cdd6f4;
    classDef action fill:#313244,stroke:#a6adc8,stroke-width:1px,color:#cdd6f4;
    classDef decision fill:#45475a,stroke:#f9e2af,stroke-width:1px,color:#f9e2af;
    
    Start([Start Batch]) --> T1_000["000: Initialized in DB"]:::state
    T1_000 --> Act_Verify["verify_smart_album.py"]:::action
    Act_Verify --> T1_100["100: Smart Album Verified"]:::state
    
    T1_100 --> Act_Export["export_photos_wrapper.py"]:::action
    Act_Export --> T1_200["200: Exported to Staging"]:::state
    
    T1_200 --> Act_Dedupe["deduplicate_assets.py"]:::action
    Act_Dedupe --> T1_210["210: Deduplicated & Ready"]:::state
    
    T1_210 --> Act_Upload["upload_to_google_photos.py"]:::action
    Act_Upload --> Dec_Quota{"Did all files fit?"}:::decision
    
    Dec_Quota -- "No (Partial)" --> T1_399["399: Partial Upload"]:::state
    Dec_Quota -- "Yes (Full)" --> T1_400["400: Upload Completed"]:::state
    
    T1_399 --> Act_Upload
    T1_400 --> T1_500["500: Wait for Google AI Curation"]:::state
    
    T1_500 --> Dec_Favs{"Google AI generated Favorites?\n(Or Manual Starring done?)"}:::decision
    
    Dec_Favs -- "Yes (Standard)" --> T1_550["550: Pull Google Favorites"]:::state
    Dec_Favs -- "No (Bypass Branch)" --> T1_500_Bypass["Bypass: Proceed with 0 Favorites"]:::state
    
    T1_500_Bypass --> T1_550
    T1_550 --> Act_Rank["rank_assets_by_score.py"]:::action
    Act_Rank --> T1_600["600: Assets Ranked & Scored"]:::state
    
    T1_600 --> Act_Cleanup["delete_google_assets.py"]:::action
    Act_Cleanup --> T1_650["650: Storage Cleaned Up"]:::state
```

---

### Track 2: Memory Feature & Publishing (Weekly Moments Workflow)
This track operates independently and uses the ranked/scored assets from Track 1 to suggest, curate, and publish event-based memories.

```mermaid
flowchart TD
    classDef state fill:#1e1e2e,stroke:#cdd6f4,stroke-width:1px,color:#cdd6f4;
    classDef action fill:#313244,stroke:#a6adc8,stroke-width:1px,color:#cdd6f4;
    
    Start([Start Weekly Curation]) --> T2_M100["M100: Candidate Moment Identified\n(Suggested by rank score)"]:::state
    
    T2_M100 --> Act_Sync["create_apple_moments_albums.py\n(Creates ToBeCurated album)"]:::action
    Act_Sync --> T2_M200["M200: Sync Proposed to Apple Photos"]:::state
    
    T2_M200 --> Act_Manual["User curates photos in Apple Photos\n(Copies selection to Curated album)"]:::action
    Act_Manual --> T2_M300["M300: Manual Curation Completed"]:::state
    
    T2_M300 --> Act_Export["export_curated_album.py\n(Exports to LaCie folder)"]:::action
    Act_Export --> T2_M400["M400: Curated folder exported locally"]:::state
    
    T2_M400 --> Act_Publish["User manual upload to Shutterfly/YouTube\n(Recorded via pipeline planner)"]:::action
    Act_Publish --> T2_M500["M500: Curated Moment Published"]:::state
```

---

## 🧠 Curation Bypass & Retroactive Favorites Sync Design

For months where Google Photos AI does not automatically create a "Best of Month" album, the pipeline supports a direct bypass mode to prevent blocking the monthly progress:

### 1. Direct-Rank Bypass Mode
If a month has `0` favorites, you can choose `bypass` during the manual curation task check in `pipeline_planner.py`. This updates the status code to `500` (Google AI Curation Complete) and marks the batch as bypassed in the database (`is_bypassed = 1`, `bypass_timestamp = datetime('now')`).

### 2. Grace Period Delayed Storage Cleanup (650)
For bypassed batches, the pipeline enforces a **14-day grace period** during which the automatic cleanup step (`650` - which deletes the raw photos from Google Photos) is skipped. This keeps the photos on Google Photos longer, giving the backend AI more time to index and cluster them.

### 3. Automatic Retroactive Favorites Sync
During the startup bootstrap check of each planner session, the system queries the Google Photos API for favorites. If it detects any bypassed batches (`is_bypassed = 1`), it automatically retrieves the media items in the corresponding curating album and checks if any late-arriving favorites exist.
* To keep operations targeted and efficient, it will **only update the favorites status for assets that belong to created/curated moments** for that month.
* The normalized score (in `ranked_assets_view`) automatically recalculates to include the retroactive favorite weight, updating your published moments without requiring you to re-rank the whole batch.
