# Google API Usage and Scopes

This document outlines every Google API call made by the media organizer pipeline, the script responsible, and the specific OAuth scopes required for each operation. This helps ensure consistency and prevent permission errors.

---

## 1. `google_photos.py` - Central API Module

This module contains the core functions that interact with Google's APIs. These functions now accept an authenticated `credentials` object and no longer handle authentication themselves.

| Function                   | API Call(s)                                                                                    | Required Scopes                                            |
| :------------------------- | :--------------------------------------------------------------------------------------------- | :--------------------------------------------------------- |
| `create_or_get_album`      | `GET /v1/albums` (List albums)<br>`POST /v1/albums` (Create album)                             | `...photoslibrary.edit.appcreateddata`                     |
| `upload_media`             | `POST /v1/uploads` (Get upload token)<br>`POST /v1/mediaItems:batchCreate` (Create media item) | `https://www.googleapis.com/auth/photoslibrary.appendonly` |
| `get_all_favorites`        | `POST /v1/mediaItems:search` (with `FAVORITES` and `includeArchivedMedia`)                     | `...photoslibrary.readonly.appcreateddata`                 |
| `get_google_storage_quota` | `GET /drive/v3/about?fields=storageQuota`                                                      | `https://www.googleapis.com/auth/drive.readonly`           |
| `list_albums`              | `GET /v1/albums` (List albums)                                                                 | `https://www.googleapis.com/auth/photoslibrary.readonly`   |

---

## 2. Script-Level API Calls

This section details which scripts call the central API functions and the scopes they request to perform their tasks.

### `list_google_photos_albums.py`

| Function Called | Scopes Requested by Script  | Purpose                                   |
| :-------------- | :-------------------------- | :---------------------------------------- |
| `list_albums()` | `...photoslibrary.readonly` | To list all albums in the user's library. |

### `pipeline_planner.py`

| Function Called        | Scopes Requested by Script | Purpose                                        |
| :--------------------- | :------------------------- | :--------------------------------------------- |
| `check_google_quota()` | `...drive.readonly`        | To check available storage before/during sync. |
| `get_all_favorites()`  | `...appcreateddata`        | Detect manual uploads by checking favorites.   |

### `pull_google_favorites.py` 500 -> 550

| Function Called         | Scopes Requested by Script   | Purpose                                         |
| :---------------------- | :--------------------------- | :---------------------------------------------- |
| `create_or_get_album()` | `...edit.appcreateddata`     | To find or create the monthly curation album.   |
| `get_all_favorites()`   | `...readonly.appcreateddata` | To get a global list of all starred items.      |
| `get_album_items()`     | `...readonly.appcreateddata` | To get all media items from the curation album. |

### `upload_to_google_photos.py` 210 -> 399,400

| Function Called         | Scopes Requested by Script    | Purpose                                          |
| :---------------------- | :---------------------------- | :----------------------------------------------- |
| `check_google_quota()`  | `...drive.readonly`           | To check available storage before uploading.     |
| `create_or_get_album()` | `...readonly.appcreateddata`  | To find or create the monthly album for uploads. |
| `upload_media()`        | `...photoslibrary.appendonly` | To upload individual media files to the album.   |

---

## 3. Scope Definitions (`constants.py`)

The project now utilizes granular, app-created scopes to comply with Google's updated security model.

- **`GOOGLE_PHOTOS_EDIT_ACCESS_SCOPES`**: `https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata`
- **`GOOGLE_PHOTOS_READONLY_SCOPES`**: `https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata`
- **`GOOGLE_PHOTOS_APPEND_ONLY_SCOPES`**: `https://www.googleapis.com/auth/photoslibrary.appendonly`
- **`GOOGLE_DRIVE_READ_ONLY_SCOPES`**: `https://www.googleapis.com/auth/drive.readonly`
