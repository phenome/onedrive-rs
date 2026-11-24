# OneDrive Client

A cloud-first OneDrive client for Linux desktops (targeting Nautilus and POSIX tools) that keeps files remote until opened, serves cloud thumbnails without downloading originals, and manages local cache retention through explicit pinning, purging, and automatic eviction.

## Language

**Cloud-only**:
A file whose metadata exists locally but whose content bytes are not stored in the local cache; opening it fetches bytes on demand.
_Avoid_: Placeholder, stub, remote-only

**Hydrated**:
A file whose content bytes are currently stored in the local cache and available without re-downloading.
_Avoid_: Downloaded, materialized, locally synced

**Pin**:
An explicit user directive to hydrate an item and protect its cached bytes from automatic eviction.
_Avoid_: Keep local, offline, lock, favorite

**Sticky Pin**:
A pin placed on a folder that recursively hydrates all current descendants and automatically hydrates future remote additions under that folder.
_Avoid_: Folder sync, recursive download

**Unpin Override**:
An explicit exception recorded on an item inside a sticky-pinned folder when that item is purged, keeping it cloud-only while its ancestor folder remains pinned.
_Avoid_: Exclude, ignore rule

**Purge**:
An explicit user action that removes an item's locally cached bytes, unpins it (or records an unpin override if under a sticky-pinned folder), and returns it to cloud-only. Refused while the item is dirty.
_Avoid_: Free up space, delete local, evict

**Eviction**:
Automatic removal of locally cached content bytes from clean, unpinned hydrated files when cache size or idle-age limits are exceeded.
_Avoid_: Purge, cleanup, garbage collection

**Dirty**:
An item with local modifications (create, edit, rename, delete) queued for upload and not yet confirmed on OneDrive.
_Avoid_: Unsynced, pending, modified

**Conflict Copy**:
A sibling file created alongside a dirty file when its local change collides with a newer remote version, preserving both versions without silent overwrite.
_Avoid_: Duplicate, merge conflict
