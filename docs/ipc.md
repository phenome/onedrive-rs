# OneDrive daemon IPC

Connect to `$XDG_RUNTIME_DIR/onedrive-rs/daemon.sock` with an `AF_UNIX` stream socket.
Send one UTF-8 JSON object followed by a newline. Read one JSON object through
its terminating newline. The server closes the connection after each reply.
Requests may be up to 64 KiB including the newline. Unknown operations and
fields, invalid paths, and malformed requests receive an error envelope.

The socket is mode `0600`. Both `XDG_RUNTIME_DIR` and its `onedrive-rs`
subdirectory must be owned by the effective user with mode `0700`. The server
rejects symlinked directories and holds an exclusive lock on the opened
socket directory until it shuts down. A restart reclaims a same-user socket at
the reserved path only when a connection is refused and the socket entry still
matches the inspected inode. A live listener, a symlink, or a regular file at
that path prevents binding. On a graceful stop, the server removes its own
socket from the opened directory.

## Requests and results

| Request | Successful `result` |
| --- | --- |
| `{"op":"status"}` | Object with `sync_state` (string), `unpinned_cache_bytes`, `pinned_cache_bytes`, `pending_upload_count` (nonnegative integers), `free_disk_floor_warning` and `paused` (booleans) |
| `{"op":"pin","path":"/folder/file"}` | `null` |
| `{"op":"unpin","path":"/folder/file"}` | `null` |
| `{"op":"purge","path":"/folder/file"}` | `null` |
| `{"op":"sync"}` | `null` |
| `{"op":"uploads"}` | Array of entries with `path` and `sync_state` strings, for example `[{"path":"/draft.txt","sync_state":"dirty"}]` |
| `{"op":"pause"}` | `null` |
| `{"op":"resume"}` | `null` |

A wire path starts at the OneDrive mount root. Use `/` for the root itself.
Paths are UTF-8 and exclude empty components, `.`, `..`, trailing slashes
(except for the root), and NUL characters.

A successful Pin or Purge replies with `{"ok":true,"result":null}`. An error
replies with `{"ok":false,"error":"Cannot purge a Dirty item"}`. Nautilus can
send Pin and Purge over this socket from its worker thread, then invalidate the
file info on success or surface the error message.

## CLI

The CLI takes absolute filesystem paths or paths relative to the working
directory. For example, `onedrive-rs pin "$XDG_RUNTIME_DIR/onedrive/docs/draft.txt"`
sends a Pin request for `/docs/draft.txt`. Set `mount_path` in the XDG configuration for a custom mount.
The `--mount-path /absolute/mount` option overrides that setting for the daemon
and path commands. Path validation checks components and mount containment
without accessing the item on disk. Paths containing `..` are rejected.

With `--json`, command results and errors are printed to stdout in the same
response envelope as IPC. Human-readable errors go to stderr. Exit status is
`0` on success, `1` on command failure and `2` on invalid arguments. Help and
version output are human-readable.

`auth login` opens the browser and stores the refresh credential in the desktop
Secret Service. The CLI reads the client ID from the XDG configuration or the
build-time default. See [Entra setup](entra-setup.md). `auth status` reports
whether a credential exists for that client ID. `auth logout` deletes that
credential and can be run again after sign-out. JSON auth results have the
shape `{"ok":true,"result":{"signed_in":false}}`.

Auth operations in the same desktop D-Bus session coordinate by client ID.
Logout waits for any credential rotation already in progress, then deletes
the stored credential. The daemon rechecks the credential before using cached
access. Every new access token is checked against the authenticated personal
drive before the daemon sends file operations, delta continuations, or uses
signed URLs. Requests dispatched before a credential change can finish.

The default mount is `$XDG_RUNTIME_DIR/onedrive`. The daemon creates the mount
directory if needed and requires it to be empty. After signing in to a different
account, restart the daemon to mount that account's drive. With the systemd unit,
run `systemctl --user restart onedrive-rs.service`. The current mount keeps its own
local data and queues writes offline until credentials for its drive are restored
or the daemon restarts. Switching back restores the original drive's catalog
and Dirty queue. When startup cannot verify credentials or reach Graph, the
daemon opens the last verified drive for offline use. A first start without a
verified drive mounts an empty filesystem. Sign in and restart to populate it.

The catalog and delta checkpoint live at
`$XDG_DATA_HOME/onedrive-rs/drives/<drive-key>/catalog.sqlite3`
(default data home `~/.local/share`). The key is the SHA-256 digest of the
verified drive ID. Dirty staging lives beside the catalog in
`catalog.sqlite3.staging`. Each catalog owns a separate sparse content
namespace under `$XDG_CACHE_HOME/onedrive-rs` (default `~/.cache/onedrive-rs`).

Delta sync runs on startup and then every `delta_poll_interval_secs`, measured
from the completion of the previous background pass. The default is 60 seconds.
`pause` prevents new background delta passes and suspends the upload worker.
An active delta pass can finish. `sync` can run while paused and drains pending
uploads after a successful delta pass. `resume` wakes both workers. SIGINT and
SIGTERM end requests in flight, unmount the filesystem, and remove the socket.
Committed delta pages, dirty staging, and cache files survive the restart.
The daemon can adopt an earlier unscoped catalog from
`$XDG_DATA_HOME/onedrive-rs/catalog.sqlite3` or
`$XDG_STATE_HOME/onedrive-rs/catalog.sqlite3` (default state home
`~/.local/state`). Adoption requires exactly one source whose root item ID
matches the authenticated drive root, an unused destination, and a same-filesystem
move. The daemon checkpoints SQLite and moves Dirty staging with the catalog.
When the root is unknown or the catalogs conflict, the daemon logs the paths
and leaves the catalog, WAL, and staging bytes in place for recovery.

Status reports cached content bytes, the cache filesystem's free disk floor,
and the count of queued uploads. A second sync request returns an error while
a delta pass is active. Pin and sync requests can run for up to five minutes.
The daemon reserves connection capacity for short requests while these
operations run.
