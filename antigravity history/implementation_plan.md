# Introduce Stage 620 – Apple Photos Curated Folder Verification

## Overview

After ranking (`600`), the pipeline needs to verify that a **Curated** album exists in Apple Photos under `Media Organizer on LaCie > Curated` for each Moment associated with the batch's assets. 

- **Moments are loosely coupled to month batches** — a single Moment (e.g. `2025-07 - TripEuropeIstanbul`) can span multiple batches. The check is per `MomentsAlbumName`, not per batch month.
- **Auto-creation**: The pipeline can create empty Curated albums via AppleScript. The user then manually selects and copies assets into those albums.
- **Verification**: Album existence = curation assumed complete. The organizer does not inspect album contents.

---

## Pipeline Flow

```
550 (favorites_pulled)
  → 600 (ranked)
  → 620 (curated_verified)   ← NEW  [manual gate]
  → 650 (cleaned)
```

### Two-Run Pattern for the 600 → 620 Transition

**Run 1** (batch at `600`):
1. Planner finds all distinct `MomentsAlbumName` values in `assets` for this batch
2. AppleScript checks `Media Organizer on LaCie > Curated` for each album name
3. For any missing albums → **auto-creates** the empty album via AppleScript
4. Logs: `✅ Created Curated album: [name]` / `ℹ️ Already exists: [name]`
5. Prompts user: `"Please populate the Curated albums with your selected assets in Apple Photos, then run the planner again to confirm. [Press Enter to continue]"`
6. **Does NOT advance** to `620` yet — exits after creating folders

**Run 2** (batch still at `600`):
1. Re-checks all `MomentsAlbumName` albums in `Media Organizer on LaCie > Curated`
2. All exist → prompts: `"All Curated albums verified for {month}. Confirm curation is complete? [y/N]"`
3. On `y` → advances batch to `620`

---

## User Review Required

> [!IMPORTANT]
> **The organizer auto-creates empty Curated albums** via AppleScript on the first run. The user is then responsible for populating those albums manually in Apple Photos before the next planner run.

> [!NOTE]
> Stage `620` uses `transition_type = 'manual'` — the planner always asks for human confirmation before advancing past it. This prevents the cleanup step (`650`) from running before curation is genuinely complete.

> [!WARNING]
> The `650 (cleaned)` status `preceding_code` must be changed from `600` to `620` in both `storage_manager_main.py` and the migration. Existing batches already at `600` will be picked up correctly by the new manual transition.

---

## Proposed Changes

### Database / Schema

#### [MODIFY] [storage_manager_main.py](file:///Users/nickolaycohen/dev/media-organizer-pipeline/scripts/storage_manager_main.py)

Add a new `INSERT OR REPLACE` for `620`:
```sql
INSERT INTO batch_status (code, preceding_code, short_label, full_description, transition_type, script_name, pipeline_stage)
VALUES ('620', '600', 'curated_verified',
        'Apple Photos Curated album created and verified for Moments in this batch',
        'manual', NULL, '3.5')
ON CONFLICT(code) DO UPDATE SET
    preceding_code = excluded.preceding_code,
    transition_type = excluded.transition_type;
```

Update `650` to chain from `620`:
```sql
... VALUES ('650', '620', 'cleaned', ...)
ON CONFLICT(code) DO UPDATE SET preceding_code = '620', ...
```

---

### Migration

#### [NEW] `migrations/041_add_620_curated_verified.py`

```python
def run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO batch_status
          (code, preceding_code, short_label, full_description, transition_type, pipeline_stage)
        VALUES ('620', '600', 'curated_verified',
                'Apple Photos Curated album created and verified for Moments in this batch',
                'manual', '3.5')
    """)
    # Re-chain cleanup to follow 620
    cursor.execute("""
        UPDATE batch_status SET preceding_code = '620'
        WHERE code = '650'
    """)
    conn.commit()
    print("✅ Added '620' (curated_verified) and re-chained '650' to follow it.")
```

---

### Planner Logic

#### [MODIFY] [pipeline_planner.py](file:///Users/nickolaycohen/dev/media-organizer-pipeline/scripts/pipeline_planner.py)

**New helper function** `check_and_create_curated_albums(month, cursor)`:
- Queries: `SELECT DISTINCT MomentsAlbumName FROM assets WHERE month = ? AND MomentsAlbumName IS NOT NULL AND MomentsAlbumName != ''`
- Runs AppleScript to get all album names currently under `Media Organizer on LaCie > Curated`
- Compares lists → returns `(missing: List[str], existing: List[str])`

**New helper function** `create_curated_album_in_apple_photos(album_name)`:
- Runs AppleScript to create an album under `Media Organizer on LaCie > Curated` if it doesn't exist
- Same pattern as existing `create_apple_moments_albums.py`

**In the `manual_candidates` evaluation loop**, when `selected_prev == '600'` (i.e. the `600 → 620` transition):
```
missing, existing = check_and_create_curated_albums(month, cursor)

if missing:
    → auto-create each missing album via AppleScript
    → log: "✅ Created Curated album: X"
    → log: "💡 Please populate these albums in Apple Photos, then re-run the planner."
    → exit (no status advance yet)
else:
    → log: "✅ All Curated albums verified for {month}: [list]"
    → prompt: "Curation complete for {month}? [y/N]"
    → on y: advance batch to 620
```

---

### Batch Status Table Doc

#### [MODIFY] [batch_status.md](file:///Users/nickolaycohen/dev/media-organizer-pipeline/docs/batch_status.md)

Add new row between 600 and 650:

| Code | Short Label        | Full Description                                                   | Stage | Script (if applicable)         |
|------|--------------------|--------------------------------------------------------------------|-------|-------------------------------|
| 620  | `curated_verified` | Curated albums created in Apple Photos; awaiting user population  | 3.5   | Manual (Apple Photos + pipeline auto-create) |

Update `650` row to note it follows `620`.

---

### README

#### [MODIFY] [README.md](file:///Users/nickolaycohen/dev/media-organizer-pipeline/docs/README.md)

1. Add `620 - Curated Verify` to the Execution Phase step list (between `600` and `650`)
2. Add row to Stage 3 table:

| 3.5 | Create/Verify Curated Albums | Auto-create Curated albums in Apple Photos for each Moment; user populates | Planner (AppleScript) + Manual | 🆕 New |

3. Add a `## Step 3.5 – Curated Album Verification` section explaining the two-run pattern

---

## Verification Plan

### Database Check
```bash
sqlite3 db/media_organizer.db \
  "SELECT code, preceding_code, short_label, transition_type FROM batch_status WHERE code IN ('600','620','650')"
```
Expected:
```
600|550|ranked|pipeline
620|600|curated_verified|manual
650|620|cleaned|pipeline
```

### Manual Verification
1. Set a test batch to status `600`
2. Run `python3 scripts/pipeline_planner.py`
   - Confirm it detects `600 → 620` as a manual candidate
   - Confirm it lists `MomentsAlbumName` values for the batch
   - Confirm missing albums are auto-created in Apple Photos `Curated`
   - Confirm planner exits without advancing status
3. Run planner again
   - Confirm all albums found in `Curated`
   - Confirm user is prompted to confirm curation
   - On `y` → batch advances to `620`
4. Run planner a third time
   - Confirm it now suggests `620 → 650` (cleanup) as next step
