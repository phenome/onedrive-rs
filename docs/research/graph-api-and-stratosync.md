# Microsoft Graph API (OneDrive Personal) & `dmcbane/stratosync` Research Report

Date: 2026-11-23  
Target repo: `phenome/onedrive-rs`  
Scope: Primary-source verification of Microsoft Graph `/me/drive` constraints (Delta, Thumbnails, Content I/O, OAuth2) and architectural audit of upstream `dmcbane/stratosync` (`v0.13.0-beta.22`).

---

## 1. Microsoft Graph API for OneDrive Personal (`/me/drive`)

> **Important Personal-drive targeting note** (from local handoff): `/me/drives` on this account returns hidden internal `b!...` drives whose first entry fails with `ObjectHandle is Invalid`. Always address `/me/drive` (or the explicit drive ID resolved from `/me/drive`), never `/me/drives[0]`.

### 1.1 Delta API (`/me/drive/root/delta`)

#### Fields returned
From [`driveitem-delta.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/api/driveitem-delta.md), [`driveitem.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/driveitem.md), [`hashes.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/hashes.md), [`folder.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/folder.md), and [`filesysteminfo.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/filesysteminfo.md):

- **Identity & tree structure**:
  - `id`: stable `driveItem` identifier across renames/moves.
  - `name`: item name (`Option` in practice—`stratosync` observed rare nameless stub items on live OneDrive delta pages that must be skipped rather than failing deserialization).
  - `parentReference` (`itemReference`): `driveId`, `driveType`, `id`, `name`, `path` (e.g. `/drive/root:/Documents` or `/drives/{drive_id}/root:/Documents`).
    - **Critical delta caveat** (`driveitem-delta.md` Remarks): *"The `parentReference` property on items won't include a value for **path**. This occurs because renaming a folder doesn't result in any descendants of the folder being returned from **delta**. **When using delta you should always track items by id**."* (In practice `parentReference.path` is often present, but becomes stale on descendants after an ancestor rename—store `(id, parent_id, name)` in SQLite and derive paths from the parent tree!)
- **Type & metadata**:
  - `size` (`Int64`), `createdDateTime`, `lastModifiedDateTime`.
  - `fileSystemInfo`: client-settable `createdDateTime`, `lastModifiedDateTime` (and `lastAccessedDateTime` on `/recent` only).
  - `eTag` (metadata + content) and `cTag` (content-only; **omitted on folders** per `driveitem.md`).
  - `folder`: `{ childCount, view }` when item is a container.
  - `file`: `{ mimeType, hashes: { quickXorHash, sha1Hash, crc32Hash } }`.
    - `hashes.md`: **`quickXorHash` (base64) is the only hash guaranteed on both OneDrive Personal and OneDrive for Business** (`sha256Hash` is unsupported).
  - `deleted`: `{ state }` when deleted.
- **OneDrive Personal omission table** (`driveitem-delta.md` Remarks):
  - Create/Modify: `n/a` (none omitted by policy, aside from `cTag` on folders).
  - Delete: `ctag`, `size` are omitted.

#### Does `/delta` support `$expand=thumbnails`?
- **Spec vs. Personal reality**: `driveitem-delta.md` states *"This method supports the `$select`, `$expand`, and `$top` OData query parameters"*. However, Microsoft Q&A / developer reports document breaking changes and `400 Bad Request` errors when combining `$expand=thumbnails` with `/delta` on OneDrive Personal (e.g., `/me/drive/root/delta?expand=thumbnails`). Meanwhile, `driveitem-list-thumbnails.md` (Example 4) specifically documents `$expand=thumbnails` on `/children`, not `/delta`.
- **Recommendation**: Do **not** rely on `$expand=thumbnails` in `/delta`. Keep `/delta` lean (`$select` metadata only) so huge trees (`Devel/node_modules`) sync fast, and fetch thumbnails lazily via `GET /me/drive/items/{id}/children?$expand=thumbnails` when a folder is opened (or when a folder contains thumbnail-eligible media).

#### Subfolder-scoped delta (`/me/drive/items/{id}/delta`) on OneDrive Personal
- **Not supported / not documented in Graph v1.0**: `driveitem-delta.md` HTTP requests list **only** `.../root/delta` (`GET /me/drive/root/delta`, `GET /drives/{drive-id}/root/delta`, etc.).
- Cross-checked with [`rclone` `backend/onedrive/onedrive.go`](https://github.com/rclone/rclone/blob/master/backend/onedrive/onedrive.go) (lines 1755–1765): rclone attempts `GET /{id}/delta` and explicitly falls back to `GET /root/delta` on API error with comment: *"Some drives only support delta listings at the root of the drive, in which case list the whole drive and filter out the items outside dir."*
- Design `onedrive-rs` around `/me/drive/root/delta` only.

#### Scaling to 100k–200k+ items (`Devel/node_modules`), `$top`, 429 throttling, and 410 resync
- **Page size & `$top`**:
  - Default page size across OneDrive collections is **200 items** (`driveitem-list-children.md`).
  - `rclone` uses `$top=1000` (`list_chunk` default `1000` in `onedrive.go` l.291, l.1749).
  - `delta-query-overview.md` notes that `$top` item counts can still vary depending on item/change types and server limits; never assume exact page sizes and never mutate `@odata.nextLink` / `@odata.deltaLink` query parameters after the initial request.
- **Concrete paging math for 100k–200k items** (initial enumeration is sequential across `@odata.nextLink` pages):
  - At **200 items/page** (default): 100k–200k items = **500–1,000 sequential GETs**. At ~150–300 ms/RTT, that is **~75–300 s (1.25–5 min)**.
  - At **1,000 items/page** (`$top=1000`, if honored by Personal): 100k–200k items = **100–200 sequential GETs** (**~15–60 s**).
  - **Incremental polls** via saved `@odata.deltaLink`: typically **1 HTTP GET** returning only changed items.
- **Throttling (`429` / `503`)**:
  - Global Graph cap is **130,000 req / 10 s** per app across all tenants (`throttling-limits.md`). OneDrive Personal per-user numeric limits are not published (`throttling-files-and-lists.md` points to SharePoint Online guidance).
  - When throttled, Graph returns `HTTP 429 Too Many Requests` (or `503`) with a `Retry-After` header in seconds (`throttling.md`).
  - `scan-guidance.md` mandates: **pause all concurrent requests** to the service for the full `Retry-After` duration; continuing other concurrent requests while throttled extends the block.
- **Token expiry / Resync (`410 Gone`)**:
  - `driveitem-delta.md`: stale/invalidated delta tokens return `HTTP 410 Gone` with a `Location` header containing a fresh enumeration URL and an error code of `resyncChangesApplyDifferences` or `resyncChangesUploadDifferences`.
  - Operational note from `stratosync` (`delta.rs` l.559): OneDrive can also return `HTTP 404` with `error.code == "resyncRequired"` for expired tokens—treat both `410` and `404 resyncRequired` as resync triggers.

#### Lazy per-folder `/children` vs. Full initial `/delta`
- `driveitem-delta.md` explicitly warns: *"If you are trying to maintain a full local representation of the items in a folder or a drive, you must use `delta` for the initial enumeration. Other approaches, such as paging through the `children` collection of a folder, are not guaranteed to return every single item if any writes take place during the enumeration."*
- **Why full initial `/delta` wins for `onedrive-rs`**:
  1. Initial `/delta` (~15–60 s at `$top=1000`, or ~1.5–5 min at 200/page) runs **once** and can checkpoint `@odata.nextLink` page-by-page into SQLite so restarts resume cleanly.
  2. Once populated, every `readdir`, `lookup`, `getattr`, and stray `find ~` is **100% local sub-millisecond SQLite I/O** with zero network calls—completely preventing Stratosync's 2.5–14 s cold-dir `rclone lsjson` stalls.
  3. Lazy `/children` alone cannot initialize a delta cursor (`?token=latest` skips existing items, leaving an incomplete DB with no way to track changes in unvisited folders without mixing two consistency models).
  4. **Hybrid optimization if fast first-mount UX is desired**: start background `/me/drive/root/delta` immediately on first boot, and optionally serve unvisited directories via `/children` *until* the initial `/delta` completes (recording the final `@odata.deltaLink` once enumeration finishes).

---

### 1.2 Thumbnails API

#### Response format & `$expand=thumbnails`
From [`driveitem-list-thumbnails.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/api/driveitem-list-thumbnails.md), [`thumbnailset.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/thumbnailset.md), and [`thumbnail.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/thumbnail.md):

- **Batch per-folder fetch** (works on **OneDrive Personal**; explicitly **fails on OneDrive for Business / SharePoint** per `driveitem-list-thumbnails.md` Remarks):
  ```http
  GET /me/drive/items/{folder-id}/children?$expand=thumbnails
  ```
  Response items include:
  ```json
  {
    "id": "182331E8-2788-4932-B52A-A6550577043F",
    "name": "my photo.jpg",
    "thumbnails": [
      {
        "id": "0",
        "small":  { "width": 96,  "height": 64,  "url": "https://sn3302files..." },
        "medium": { "width": 176, "height": 117, "url": "https://sn3302files..." },
        "large":  { "width": 800, "height": 533, "url": "https://sn3302files..." }
      }
    ]
  }
  ```
- **Named sizes** (`driveitem-list-thumbnails.md` table; note `thumbnailset.md` lists older `48x48` / `1920x1920` descriptions):
  - `small` (96 px longest edge), `medium` (176 px longest), `large` (800 px longest) — original aspect ratio.
  - `smallSquare` (96×96), `mediumSquare` (176×176), `largeSquare` (800×800) — square crop.
- **Custom sizes & `$select` / `select`**:
  - On `/thumbnails`, `select` (or `$select`) requests named or custom sizes `c{W}x{H}` (bounded, aspect preserved) or `c{W}x{H}_crop`, e.g.:
    `GET /me/drive/items/{item-id}/thumbnails?select=c256x256` (or `c300x400_crop`).
  - Docs note a larger cached thumbnail may be returned if one already exists and scales cleanly; aspect ratio is preserved.
  - `query-parameters.md` documents nested `$select` inside `$expand` (`$expand=children($select=id,name)`), though `driveitem-list-thumbnails.md` Example 4 uses bare `?$expand=thumbnails` without nested `($select=...)`—test `$expand=thumbnails($select=large,medium)` or `$expand=thumbnails($select=c256x256)` live against `/me/drive` before relying on nested custom size selection, vs. picking `large` (800 px) / `medium` (176 px) from `?$expand=thumbnails` (or fetching `/thumbnails/0/c256x256/content`).
  - `originalOrientation=true` (Personal only) omits EXIF auto-rotation; **omit it** so OneDrive returns upright pre-rotated thumbnails.

#### Image format & freedesktop thumbnail cache implications
- **Graph output format**: `driveitem-list-thumbnails.md` Example 2 shows `.jpg` URLs (`https://onedrive.com/.../asdjlkasjdkasdjlk.jpg`), and OneDrive serves **JPEG** (or PNG when alpha/vector rendering applies) from `*.files.1drv.com` CDN URLs.
- **Freedesktop / Nautilus requirement** (per [`nautilus-cloud-thumbnails-2026-11-23.txt`](file:///home/henrique/devel/wiki/tasks/2026/11/onedrive-client-switch/nautilus-cloud-thumbnails-2026-11-23.txt)):
  - Nautilus 51.1 + `gnome-desktop` require **PNG** files in `~/.cache/thumbnails/{normal,large,x-large,xx-large}/<md5(file://URI)>.png` (`normal` ≤ 128 px, `large` ≤ 256 px) containing PNG `tEXt` chunks `Thumb::URI` (exact GIO `file://` URI) and `Thumb::MTime` (integer seconds matching `stat().st_mtime`).
  - **Implication**: `onedrive-rs` **must decode** downloaded Graph thumbnail bytes (JPEG/PNG) and **re-encode as PNG** (resizing `large` 800 px → ≤256 px for `large/` or ≤128 px for `normal/` if needed) with `Thumb::URI` and `Thumb::MTime` `tEXt` chunks.

#### Non-image files (PDF, video, Office docs, HEIC, RAW)
- Supported! `thumbnail.md` defines thumbnails for *"an image, video, document, or any item that has a bitmap representation."*
- `driveitem-list-thumbnails.md` Example 4 explicitly returns thumbnails for `presentation.pptx`.
- Microsoft's OneDrive/SharePoint preview docs confirm server thumbnail generation for **JPEG, PNG, HEIC/HEIF, camera RAW** (`NEF`, `NRW`, `ORF`, `PEF`, `RW2`, `MRW`, `MEF`, etc.), **PDF**, **Office documents** (`.docx`, `.xlsx`, `.pptx`), and **videos** (`.mp4`, `.mov`, `.avi`, `.mkv`, etc.), subject to size/codec limits (images < 100 MB / ~12000×8000 px; high-bitrate or exotic video codecs may lack thumbnails).
- Items with no bitmap representation (`.txt`, `.rs`, `.json`, archives) return `"thumbnails": []`. Remember from `nautilus-cloud-thumbnails-2026-11-23.txt`: if Nautilus opens a FUSE file whose mime type *has* a local thumbnailer (e.g., an oversized >100 MB image or unsupported RAW/video) and no valid cache/fail-cache entry exists, Nautilus will attempt local thumbnailing (`open()` → full download!) unless `thumbnail-limit` blocks it or a `fail/gnome-thumbnail-factory/<md5>.png` marker is written.

#### Thumbnail URL expiry & caching
- `driveitem-list-thumbnails.md`: *"Thumbnail URLs are cache-safe. The URL will change, if the item changes in a way that requires a new thumbnail to be generated."*
- No explicit TTL is documented for thumbnail `url` values (unlike `@microsoft.graph.downloadUrl`), though they are pre-authenticated `*.files.1drv.com` URLs; fetch them promptly upon folder listing rather than storing URLs long-term, and rely on local PNG cache + `Thumb::MTime` / `cTag` invalidation.

---

### 1.3 Content I/O & OAuth2 Auth

#### Ranged downloads & `@microsoft.graph.downloadUrl`
From [`driveitem-get-content.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/api/driveitem-get-content.md) and [`driveitem.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/resources/driveitem.md):
- **Ranged reads**: Supported via standard `Range: bytes=0-1023` header sent **directly to `@microsoft.graph.downloadUrl`** (or the `302 Found` `Location` from `GET /me/drive/items/{id}/content`), **without** an `Authorization: Bearer` header.
- **Responses**: `HTTP 206 Partial Content` with `Content-Range` (note: docs warn that if a range cannot be generated, the server may ignore `Range` and return `200 OK` with full contents—handle `200` defensively).
- **Expiry**: `driveitem.md` specifies `@microsoft.graph.downloadUrl` is valid for a short period (**1 hour**; `driveitem-get-content.md` warns *"might expire within minutes"*). Cache in memory per-item for only a few minutes and refresh automatically on `401`/`403`/`410`.

#### Uploads: Simple PUT vs. `createUploadSession`
- **Simple `PUT`** ([`driveitem-put-content.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/api/driveitem-put-content.md)):
  - `PUT /me/drive/items/{item-id}/content` or `PUT /me/drive/items/{parent-id}:/{filename}:/content`
  - Documented max size: **250 MB** (updated from the legacy 4 MiB limit), though `driveitem-createuploadsession.md` best practices still recommend resumable sessions for files **> 10 MiB**.
- **Upload Session** ([`driveitem-createuploadsession.md`](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/api-reference/v1.0/api/driveitem-createuploadsession.md)):
  - `POST /me/drive/items/{parentItemId}:/{fileName}:/createUploadSession` (new) or `POST /me/drive/items/{itemId}/createUploadSession` (update).
  - **Conflict detection**: Supports `If-Match: {etag or ctag}` and `If-None-Match` headers on `createUploadSession` (and on explicit commit `PUT` via `@microsoft.graph.sourceUrl`), returning `412 Precondition Failed` on mismatch. Request body `item["@microsoft.graph.conflictBehavior"]` supports `"fail"` (default), `"replace"`, `"rename"`; `item.fileSize` (OneDrive Personal only) pre-checks quota (`507 Insufficient Storage`).
  - **Chunk rules**:
    - Upload sequential fragments via `PUT` to `uploadUrl` **without** an `Authorization` header (sending `Authorization` causes `401`).
    - Max single request size: **< 60 MiB**; recommended **5–10 MiB**.
    - Every fragment (except the final one) **must be a multiple of 320 KiB (327,680 bytes)**; non-multiples fail on commit.
    - Each fragment extends `expirationDateTime`; `GET uploadUrl` returns `nextExpectedRanges` for resume; `DELETE uploadUrl` aborts.

#### OAuth2 for OneDrive Personal (Public Client PKCE Loopback)
From [`v2-oauth2-auth-code-flow.md`](https://github.com/MicrosoftDocs/entra-docs/blob/main/docs/identity-platform/v2-oauth2-auth-code-flow.md), [`reply-url.md`](https://github.com/MicrosoftDocs/entra-docs/blob/main/docs/identity-platform/reply-url.md), [`supported-accounts.md`](https://github.com/MicrosoftDocs/entra-docs/blob/main/docs/identity-platform/supported-accounts-validation.md), and [`refresh-tokens.md`](https://github.com/MicrosoftDocs/entra-docs/blob/main/docs/identity-platform/refresh-tokens.md):
- **Entra app registration**:
  - `signInAudience`: `"PersonalMicrosoftAccount"` (*Personal Microsoft accounts only*) or `"AzureADandPersonalMicrosoftAccount"` (*Any org + personal*).
  - Authority endpoint: `https://login.microsoftonline.com/consumers/oauth2/v2.0/{authorize,token}` (or `/common/...` if supporting both).
  - Platform: **Mobile and desktop applications** (public client, no `client_secret`). **Do not** register as `spa` (SPA enforces CORS `Origin` and caps refresh tokens at 24 hours).
- **Loopback redirect & PKCE**:
  - Register `http://localhost` (or `http://127.0.0.1` via manifest `replyUrlsWithType`). Per RFC 8252 and `reply-url.md`, **Microsoft Entra ignores the port when matching localhost redirect URIs**, allowing ephemeral port binding (`http://localhost:<port>`) at runtime. Query parameters in redirect URIs are **not** allowed for personal accounts.
  - Use `code_challenge` (`S256`) + `code_verifier` (PKCE) on `/authorize` and `/token`.
- **Minimal scopes**:
  - `Files.ReadWrite` + `offline_access` (delegated). `offline_access` is required for `/token` to issue a `refresh_token`. Avoids the broad Mail/Calendar/Contacts/Sites scopes requested by GNOME Online Accounts.
- **Refresh token lifetime & rolling behavior**:
  - Default lifetime for non-SPA desktop/native clients: **90 days** (`refresh-tokens.md`).
  - **Rolling**: Each `grant_type=refresh_token` request returns a fresh `refresh_token` (alongside a ~1h `access_token`). Entra does not immediately revoke the old refresh token, but clients **must** atomically persist the new `refresh_token` and discard the old one. Active background polling/use keeps the 90-day sliding window alive indefinitely unless explicitly revoked or password-changed.

---

## 2. Upstream `dmcbane/stratosync` Architecture Audit

Inspected shallow clone at commit `d53da4c` (`2026-10-04`, `Cargo.toml` version `0.13.0-beta.22`, license `MIT OR Apache-2.0`, author Dale McBane).

### 2.1 Crate Layout & Stack
4-crate workspace in `crates/`:
- `stratosync-core`: `Backend` trait, `RcloneBackend`, `WebDavBackend`, `delta.rs` (`GoogleDriveDelta`, `OneDriveDelta`), `rclone_config.rs`, `StateDb` (`rusqlite` `0.31` bundled, WAL mode), `BaseStore`, `hashes.rs`, `ipc.rs`, `merge.rs`.
- `stratosync-daemon` (`stratosyncd`): `fuser` `0.15` (`abi-7-12`) FUSE3 filesystem (`StratoFs`), `tokio` runtime, `CacheManager` (LRU eviction, pin-aware), `RemotePoller`, `UploadQueue`, `notify` `6.1.1` watcher, Prometheus `/metrics`, IPC socket.
- `stratosync-cli` (`stratosync`): `clap` CLI + `ratatui` TUI dashboard (`status`, `ls`, `pin`, `unpin`, `push`, `conflicts`, `versions`, `cache clear`, `daemon`).
- `stratosync-tray`: system tray indicator.

### 2.2 Coupling to `rclone`
Despite having an async `Backend` trait and a `reqwest`-based `OneDriveDelta` struct, **`stratosync` is deeply coupled to shelling out to `rclone` subprocesses**:
1. **`RcloneBackend` (`crates/stratosync-core/src/backend/mod.rs`)**: Every `Backend` method—`stat`, `list`, `list_recursive`, `download`, `download_with_progress`, `download_range` (`rclone cat --offset --count`), `upload`, `upload_with_progress`, `check_if_match` (`rclone lsjson`), `mkdir`, `delete`, `rmdir` (`rclone purge`), `rename` (`rclone moveto`), and `about`—spawns an `rclone` subprocess via `tokio::process::Command` and parses `--use-json-log` stderr.
2. **Even `OneDriveDelta` (`crates/stratosync-core/src/backend/delta.rs`) shells out to `rclone`**: OAuth tokens are read via `rclone config show <remote>` and refreshed via `rclone about <remote>: --json` (`rclone_config.rs`).
3. **`WebDavBackend` (`backend/webdav.rs`)**: Opt-in sidecar that still spawns `rclone serve webdav`.
4. **Domain leakage**: `hashes.rs` and `types/mod.rs` exist largely to reconcile `rclone`'s lowercase-hex `quickxor` / `stat` output against Graph's base64 `quickXorHash` and uppercase `sha1Hash`, and `RcloneBackend::upload` simulates `If-Match` via a TOCTOU `lsjson` pre-check rather than Graph's native `If-Match` header.

### 2.3 FUSE Layer (`fuser`), Cold `readdir`, and Prefetch Behavior
- **FUSE-to-Tokio bridge (`crates/stratosync-daemon/src/fuse/mod.rs`)**: Implements `fuser::Filesystem` synchronously; each callback parks on `self.rt.block_on(...)`. Concurrent `open()` calls on an unhydrated inode coalesce onto `DashMap<Inode, Vec<oneshot::Sender>>`. Uses `fuser::Notifier::inval_inode` after directory mutations and `host_statfs` (`statvfs`) so file managers don't reject copies with `ENOSPC`.
- **Why cold `readdir` & browsing lagged**:
  - `StratoFs::readdir` checks `if dir.dir_listed.is_none()` and calls `populate_directory()`, which executes `backend.list()` (`rclone lsjson --hash`) synchronously before returning entries, then spawns `spawn_prefetch_child_dirs` (4 concurrent `rclone lsjson` calls for subdirectories) and `spawn_prefetch_headers` (up to 16 concurrent `rclone cat --offset 0 --count ...` subprocesses for MIME sniffing!).
  - In `poller.rs`, delta mode's initial population (`token == None`) does **not** use `/delta`—it calls `poll_once()` (`rclone lsjson --recursive --hash`) and then `get_start_token()` (`?token=latest`). If initial `list_recursive` fails or hasn't finished, `dir_listed` stays `None` and every cold directory falls back to on-demand `rclone lsjson`.
  - No thumbnail support exists at all; `header_cache` (`rclone cat`) was added specifically as a workaround for Dolphin/Nautilus MIME sniffing.

### 2.4 Nautilus Extension
Located in `contrib/file-managers/nautilus/stratosync_nautilus.py` (83 lines, `nautilus-python` 3.0/4.0) + `contrib/file-managers/common/stratosync_fm_common.py` (202 lines):
- **Emblems (`Nautilus.InfoProvider`)**: Reads virtual read-only xattrs served by `StratoFs::getxattr`: `user.stratosync.status`, `user.stratosync.etag`, `user.stratosync.remote_path`. Maps status (`cached`, `dirty`, `uploading`, `hydrating`, `remote`, `conflict`, `stale`) to freedesktop emblem icon names (`emblem-default`, `emblem-synchronizing`, `emblem-downloads`, `emblem-web`, `emblem-important`, `emblem-generic`).
- **Context menu (`Nautilus.MenuProvider`)**: Checks `user.stratosync.status` xattr to filter Stratosync paths, then spawns detached CLI commands (`subprocess.Popen(["stratosync", "pin"|"unpin", path])`).
- **Gap**: No `pinned` xattr exists yet, so both Pin and Unpin always appear regardless of current pin state, and there is no "Free up space / purge" action distinct from `unpin`.

### 2.5 Fork vs. Clean Greenfield Recommendation
**Recommendation: Clean greenfield Rust implementation, using `dmcbane/stratosync` strictly as a reference.**

| Aspect | Forking `stratosync` | Greenfield `onedrive-rs` |
|---|---|---|
| **rclone baggage** | Must rip out `RcloneBackend`, `rclone_config`, `WebDavBackend`, `GoogleDriveDelta`, `hashes.rs` Graph/rclone normalization, and `rclone cat` `header_cache`. | Zero `rclone` dependency; native `reqwest` Graph client + OAuth2 PKCE + `If-Match` upload sessions. |
| **Data model** | Built around `remote_path` + `rclone lsjson`; `remote_item_id` was retrofitted in `v0.13.0-beta.1` after rename bugs (`2026-05-06` post-mortem). | Native `(id, parent_id, name)` Graph tree in SQLite from day one; initial `/delta` populates `file_index` directly. |
| **Thumbnails** | None; relies on `header_cache` (`rclone cat`). | First-class lazy `/children?$expand=thumbnails` → PNG + `tEXt` cache seeder integrated with FUSE `readdir`/Nautilus. |
| **Scope & complexity** | Carries TUI dashboard, tray app, Prometheus, 5 file-manager plugins, 3-way git merge conflict UI. | Focused scope: FUSE daemon + SQLite + Graph client + pin/purge/LRU + minimal `nautilus-python` extension. |
| **Reusable patterns** | `fuser` `block_on` + `oneshot` hydration coalescing, `fuser::Notifier::inval_inode`, `statfs`, `user.*` xattrs + `nautilus-python` 4.0 shell. | Borrow these exact patterns cleanly in ~1/5th the codebase size. |
