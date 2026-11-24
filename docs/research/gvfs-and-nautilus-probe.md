# GVfs, Nautilus 51.1, and GLib 2.90.1 Technical Probe

Date: 2026-11-23
Target workspace: `/home/henrique/devel/onedrive-rs`
Host environment (verified via `pacman -Q`): Arch Linux, `gvfs 1.62.0-3`, `nautilus 51.1-1`, `glib2 2.90.1-1`, `nautilus-python 4.2.0-1`, `gnome-desktop` 51.0 (`libgnome-desktop-4.so.2`).

Context incorporated (not re-derived):
- [nautilus-cloud-thumbnails-2026-11-23.txt](file:///home/henrique/devel/wiki/tasks/2026/11/onedrive-client-switch/nautilus-cloud-thumbnails-2026-11-23.txt)
- [handoff-rust-onedrive-client-2026-11-23.md](file:///tmp/handoff-rust-onedrive-client-2026-11-23.md)

---

## Executive Summary

1. **Out-of-tree Rust GVfs backend — Not recommended.** GVfs exposes **no** public headers, pkg-config files, GIR/VAPI, or stable C/ABI interface (`pacman -Ql gvfs` installs zero `.h`/`.pc` files). Linking `/usr/lib/gvfs/libgvfsdaemon.so` breaks on Arch rolling upgrades because `GVfsBackendClass` has zero padding and shifted vtable slots in `1.57.1`/`1.58.0` and `1.62.0`. Reimplementing GVfs's private D-Bus + Unix `socketpair` FD-passing wire protocol (`org.gtk.vfs.*` + `GVfsDaemonSocketProtocol`) in pure Rust avoids the C ABI break (the XML protocol itself did not change in `1.50.0..1.62.0`), but forces maintaining an undocumented internal protocol while still inheriting GVfs's lack of local cache/pin/purge and forcing non-GIO apps through `gvfsd-fuse`. A custom GIO `GVfs` module (`gio-vfs`) is a **process-wide singleton** that displaces `GDaemonVfs` (`trash://`, `sftp://`, `dav://`, etc.) and provides zero POSIX visibility. **Verdict: build a Rust FUSE daemon.**
2. **Nautilus 51.1 extensions (`libnautilus-extension-4`) — UI/metadata overlay only.** `NautilusInfoProvider`, `NautilusMenuProvider`, `NautilusColumnProvider`, and `NautilusPropertiesModelProvider` can add emblems, string attributes (for custom columns), Properties dialog rows, and context menu items. They **cannot** set `preview::icon`, inject custom thumbnails, or override `nautilus_file_should_show_thumbnail()`. `nautilus-python 4.2.0-1` is already installed on this machine and avoids manual GTypeModule FFI + root-owned `/usr/lib/nautilus/extensions-4/` installs required by a Rust `cdylib`.
3. **GLib 2.90.1 local thumbnail rules — Viable for FUSE, with one critical fail-cache pitfall.** GLib sets `thumbnail::path` and `thumbnail::is-valid` on local/FUSE files by checking `$XDG_CACHE_HOME/thumbnails/{xx-large,x-large,large,normal}/<md5(file://...)>.png` for `tEXt::Thumb::URI` and `tEXt::Thumb::MTime` (optional `Thumb::Size` is validated if present). **Crucial catch:** Nautilus 51.1's `NautilusImage` widget does **not** check GLib's `thumbnailing::failed` attribute; instead `gnome-desktop` 51.0 loads `~/.cache/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` with `gdk_pixbuf_new_from_file()`. A 0-byte fail marker fails to parse and causes Nautilus to **open and download the file anyway**; fail markers must be valid PNGs (e.g., 1×1 transparent RGBA PNG) with matching `tEXt::Thumb::URI` and `tEXt::Thumb::MTime`.

---

## 1. Out-of-Tree Rust GVfs Backend Feasibility

### 1.1 Installed Footprint and Lack of Public API
Inspected `pacman -Ql gvfs`, `/usr/lib/gvfs/`, and `/usr/share/gvfs/mounts/` on the host, plus upstream `gvfs` tag `1.62.0`:

- **No public dev surface:** `pacman -Ql gvfs | grep -E '\.(h|pc|gir|vapi)$'` returns nothing. Upstream `meson.build` / `common/meson.build` / `daemon/meson.build` call neither `install_headers()` nor `pkg.generate()`.
- **Private shared libraries:** `/usr/lib/gvfs/libgvfscommon.so` (243,656 B) and `/usr/lib/gvfs/libgvfsdaemon.so` (161,832 B) are installed in `gvfs_pkglibdir` (`/usr/lib/gvfs`) with no `SOVERSION` (`readelf -d` shows `SONAME: [libgvfsdaemon.so]`, `RUNPATH: [/usr/lib/gvfs]`).
- **Mount discovery (`daemon/mount.c` l.562–627):** `gvfsd` scans `/usr/share/gvfs/mounts/*.mount` (19 files installed on host; 12 from base `gvfs` such as `admin.mount`, `dav.mount`, `http.mount`, `sftp.mount`, `trash.mount`) or `$GVFS_MOUNTABLE_DIR`. KeyFile `[Mount]` entries (`Type`, `Exec`, `DBusName`, `AutoMount`, `Scheme`, `SchemeAliases`, `DefaultPort`, `HostnameIsInetAddress`, `MountPerClient`) tell `gvfsd` either to spawn `Exec --spawner <dbus_id> <obj_path>` (`mount.c` l.491–505) or invoke `org.gtk.vfs.Mountable.Mount` on `DBusName`. `gvfsd` does not watch `/usr/share/gvfs/mounts/` via inotify; it reloads mount configs only on `SIGUSR1` (`mount.c` l.1131–1175) or daemon restart (`gvfsd.hook`).

### 1.2 Integration Path A: Linking `libgvfsdaemon.so` (`GVfsBackend` Subclass)
Exported symbols (`nm -D --defined-only /usr/lib/gvfs/libgvfsdaemon.so`) expose `g_vfs_backend_*`, `g_vfs_daemon_*`, `g_vfs_channel_*`, and `g_vfs_job_*` C GObject classes. However:
- **Zero ABI stability:** `struct _GVfsBackendClass` (`gvfs/daemon/gvfsbackend.h` l.90–471) contains ~90 vtable function pointers and **zero** reserved padding slots.
- **Confirmed ABI breaks in `1.50.0..1.62.0`:**
  - Commit `85e7d6a7` (*daemon: Add support for edit mode*, first tagged in `1.57.1`) inserted `try_edit` and `edit` into the middle of `struct _GVfsBackendClass` (`gvfsbackend.h` l.216–223), shifting every subsequent vtable slot (`close_write`, `try_close_write`, `query_info`, `enumerate`, `delete`, etc.).
  - Commit `cc2a2251` (*daemon: Add auto-unmount support for idle backends*, `gvfs 1.62+`) added `g_vfs_backend_set_autounmount`, `activity_started`, and `activity_finished` and altered `GVfsBackendPrivate` / job tracking.
- **Arch rolling-upgrade consequence:** Any out-of-tree Rust binary linking `libgvfsdaemon.so` and subclassing `GVfsBackend` via bindgen would have to vendor exact `gvfs` internal headers for the installed version and would suffer silent vtable mis-dispatch or crashes whenever Arch bumps `gvfs`.

### 1.3 Integration Path B: Pure-Rust Private D-Bus + Socket Protocol Daemon
A standalone Rust executable launched via `/usr/share/gvfs/mounts/onedrive.mount` could speak GVfs's private wire protocol without linking `libgvfsdaemon.so`. To do so, it must implement (`gvfs/common/org.gtk.vfs.xml` and `gvfs/common/gvfsdaemonprotocol.{h,c}`):
1. **D-Bus lifecycle & mount interfaces:**
   - Call `org.gtk.vfs.Spawner.Spawned` back to `gvfsd` (`daemon/daemon-main.c` l.161–213) and `org.gtk.vfs.MountTracker.RegisterMount` / `UnregisterMount`.
   - Export `org.gtk.vfs.Daemon` (`GetConnection`, `Cancel`, `Mount`, `ListMonitorImplementations`), `org.gtk.vfs.Mountable` (`Mount`), `org.gtk.vfs.Mount` (24 methods: `Unmount`, `OpenForRead`, `OpenForWrite`, `OpenForWriteFlags`, `QueryInfo`, `QueryFilesystemInfo`, `Enumerate`, `OpenIconForRead`, etc.), and `org.gtk.vfs.Monitor` (`Subscribe`, `Unsubscribe`).
   - Drive client callbacks (`org.gtk.vfs.Enumerator.{GotInfo,Done}`, `org.gtk.vfs.MonitorClient.Changed`, `org.gtk.vfs.Progress.Progress`, `org.gtk.vfs.MountOperation.*`).
2. **Custom GVariant attribute & icon marshaling (`common/gvfsdaemonprotocol.c` l.30–212):**
   - `GFileInfo` is marshaled as `a(suv)` with custom `GIcon` serialization `(us)` version `3` via `g_icon_to_string()` (for `preview::icon`, GVfs relies on `GVfsIcon` deserialized by `libgvfsdbus.so`).
3. **Binary Unix-socket FD-passing stream protocol (`common/gvfsdaemonprotocol.h` l.26–76, `daemon/gvfschannel.c`, `daemon/gvfsreadchannel.c`):**
   - `OpenForRead`, `OpenForWrite`, and `OpenIconForRead` return a Unix FD (`org.gtk.GDBus.C.UnixFD`) from `socketpair(AF_UNIX, SOCK_STREAM, 0, ...)`.
   - Requests use 20-byte `GVfsDaemonSocketProtocolRequest` (`command` 0–7: `READ`, `WRITE`, `CLOSE`, `CANCEL`, `SEEK_SET`, `SEEK_END`, `QUERY_INFO`, `TRUNCATE`, `seq_nr`, `arg1`, `arg2`, `data_len`). Replies use 16-byte `GVfsDaemonSocketProtocolReply` (`type` 0–6).
   - `GVfsReadChannel` implements adaptive chunk sizing (`4 KiB` → `8` → `16` → `32` → `64` → `128 KiB`, capped at `256 KiB`) and pipelined readahead (`gvfsreadchannel.c` l.108–249).
- **Wire stability across `1.50.0..1.62.0`:** `git log 1.50.0..1.62.0 -- common/org.gtk.vfs.xml common/gvfsdaemonprotocol.h common/gvfsdaemonprotocol.c` shows **zero commits**. The wire format has been practically static, though it carries **zero** upstream stability contract and remains undocumented outside GVfs source.
- **Thumbnail caveat for GVfs backends:** `daemon/gvfsbackend.c: get_thumbnail_attributes()` (l.530–572) sets `thumbnail::path` and `thumbnailing::failed`, **never** `thumbnail::is-valid`. Thus, even a custom GVfs backend relies on `preview::icon` (`OpenIconForRead`) for Nautilus 51.1 under `local-only`, and re-generates thumbnails per session rather than picking up disk cache entries in `NautilusImage` (`nautilus-image.c` l.424–441).

### 1.4 Integration Path C: Custom GIO `GVfs` Extension Module (`gio-vfs`)
- `glib/gio/gvfs.c: g_vfs_get_default()` (l.349–367) calls `_g_io_module_get_default(G_VFS_EXTENSION_POINT_NAME, "GIO_USE_VFS", g_vfs_is_active)` (`glib/gio/giomodule.c` l.955–1072).
- **Single-winner dispatch:** `_g_io_module_get_default` instantiates **one process-wide singleton** — either the name in `GIO_USE_VFS`, or the highest-priority extension where `is_active()` returns `TRUE`.
- System `libgvfsdbus.so` (`/usr/lib/gio/modules/libgvfsdbus.so`) registers `"gvfs"` at priority `10` (`gvfs/client/gdaemonvfs.c` l.1512–1515); built-in `GLocalVfs` registers `"local"` at priority `0` (`glib/gio/glocalvfs.c` l.50–55).
- **Why this is non-viable:** Installing a custom `GVfs` module with priority `> 10` in `/usr/lib/gio/modules` or `GIO_EXTRA_MODULES` **replaces `GDaemonVfs` completely** for every GTK/GIO app. That breaks `trash://`, `sftp://`, `dav://`, `mtp://`, `admin://`, `recent://`, and `gvfs-metadata` unless the Rust module also proxies/wraps `GDaemonVfs` (itself private) — and non-GIO apps (CLI tools, editors, `rg`, `ffmpeg`) still cannot see files at all.

### 1.5 Engineering Verdict: Rust GVfs Backend vs. Rust FUSE Daemon
| Dimension | Out-of-tree Rust GVfs (C ABI link) | Out-of-tree Rust GVfs (raw D-Bus + socketpair) | Rust FUSE Daemon (`fuser` / `fuse3`) |
|---|---|---|---|
| **Rolling-upgrade safety on Arch** | **Broken** (unversioned `.so`, unpadded vtable shifts) | **Fragile** (undocumented private wire protocol, zero contract) | **High** (kernel FUSE ABI + `libfuse3` stable) |
| **Non-GIO / CLI / editor access** | Via `gvfsd-fuse` only (extra hop, no direct cache control) | Via `gvfsd-fuse` only | **Native POSIX** for all processes |
| **Persistent metadata DB & pin/purge/eviction** | None in GVfs; must build anyway | Must build anyway | **Natural fit** (SQLite/redb + block/file store) |
| **Nautilus 51 `local-only` thumbnails** | Official via `preview::icon` (`GVfsIcon`) | Official via `preview::icon` (`GVfsIcon`) | Works via freedesktop cache seeding (see §3), provided fail-cache & metadata rules are respected |
| **Maintenance / ownership cost** | Unacceptable | High (reimplementing `org.gtk.vfs.*` + `GVfsIcon` + `socketpair`) | Moderate and self-contained |

**Verdict:** Do **not** build a Rust GVfs backend or a GIO `gio-vfs` module. Build a **Rust FUSE daemon** with a persistent metadata DB (populated via Graph delta), ranged-read local cache, pin/purge/eviction, and freedesktop thumbnail-cache seeding (verified against §3).

---

## 2. Nautilus 51.1 Extension Capabilities (`libnautilus-extension-4`)

### 2.1 Public Interfaces Verified in `/usr/include/nautilus/`
`pkg-config --cflags --libs libnautilus-extension-4` links `-lnautilus-extension -lgio-2.0 -lgobject-2.0 -lglib-2.0`. The complete public API in `/usr/include/nautilus/nautilus-extension.h` and `/usr/include/nautilus/libnautilus-extension/*.h` exposes exactly four provider interfaces:

1. **`NautilusMenuProvider`** (`nautilus-menu-provider.h`, `nautilus-menu.h`):
   - `get_file_items(provider, GList *files)` and `get_background_items(provider, NautilusFileInfo *current_folder)` return lists of `NautilusMenuItem` (supports `name`, `label`, `tip`, icon name, `set_submenu()`, `activate` signal).
   - `nautilus_menu_provider_emit_items_updated_signal(provider)` forces menu refresh.
2. **`NautilusInfoProvider`** (`nautilus-info-provider.h`, `nautilus-directory-async.c` l.3894–3948):
   - `update_file_info(provider, NautilusFileInfo *file, GClosure *update_complete, NautilusOperationHandle **handle)` returns `NAUTILUS_OPERATION_COMPLETE`, `NAUTILUS_OPERATION_FAILED`, or `NAUTILUS_OPERATION_IN_PROGRESS` (async completion via `nautilus_info_provider_update_complete_invoke()`, plus `cancel_update()`).
3. **`NautilusColumnProvider`** (`nautilus-column-provider.h`, `nautilus-column.h`, `nautilus-column-utilities.c` l.175):
   - `get_columns(provider)` returns `NautilusColumn` (`name`, `attribute`, `label`, `description`) mapping string attributes to list-view columns.
4. **`NautilusPropertiesModelProvider`** (`nautilus-properties-model-provider.h`, `nautilus-properties-model.h`, `nautilus-properties-item.h`, `nautilus/src/nautilus-properties.c` l.3544–3572):
   - `get_models(provider, GList *files)` returns `NautilusPropertiesModel` (`title`, `GListModel` of `NautilusPropertiesItem(name, value)`) rendered in the Properties dialog.

### 2.2 Can an Extension Control Thumbnails or `preview::icon`?
**No.** Verified from `/usr/include/nautilus/libnautilus-extension/nautilus-file-info.h` (l.40–105) and `nautilus/src/nautilus-file.c` (`nautilus_file_info_iface_init`, l.9022–9048):

- The **only** mutators on `NautilusFileInfo` are:
  - `nautilus_file_info_add_emblem(file_info, emblem_name)` → prepends to `file->details->extension_emblems` (`nautilus-file.c` l.8829–8838), merged into `nautilus_file_get_emblem_icons()` (`l.4600–4638`).
  - `nautilus_file_info_add_string_attribute(file_info, attribute_name, value)` → inserts into `file->details->extension_attributes` (`nautilus-file.c` l.8848–8866), which is **only** consulted by `nautilus_file_get_string_attribute_q()` (`l.6578–6582`) for `NautilusColumn` display.
  - `nautilus_file_info_invalidate_extension_info(file_info)` → invalidates `NAUTILUS_ATTRIBUTE_EXTENSION_INFO` (`l.8869–8874`).
- There is **no** method to mutate `GFileInfo` attributes (`preview::icon`, `thumbnail::path`, `thumbnail::is-valid`), no method to attach a custom thumbnail `GdkTexture`/`GdkPixbuf`/`GIcon`, and no override for `nautilus_file_should_show_thumbnail()` (`nautilus-file.c` l.4520–4556). Note also that setting custom-icon metadata (`metadata::custom-icon`) would actually **suppress** thumbnails (`if (has_custom_icon (file)) return FALSE;` at `nautilus-file.c` l.4524).
- **What the extension CAN do for OneDrive:** display sync-state emblems (`emblem-synchronizing`, `emblem-default`, custom themed emblems), a "Cloud Status" column, Properties dialog metadata (cloud item ID, sync state, cache size), and context menu items ("Keep Local / Pin", "Free Up Space / Purge") talking to the daemon over D-Bus or a Unix socket.

### 2.3 Rust `cdylib` vs. `nautilus-python`
`nautilus-python` `4.2.0-1` is packaged in Arch `extra` and already installed (`/usr/lib/nautilus/extensions-4/libnautilus-python.so`, linked against `libnautilus-extension.so.4` and `libpython3.14.so.1.0`).

| Aspect | Rust `cdylib` (`libnautilus-extension-4`) | `nautilus-python` (`4.2.0-1`) |
|---|---|---|
| **Module entrypoints & install path** | Must export C ABI `nautilus_module_initialize(GTypeModule*)`, `nautilus_module_shutdown()`, `nautilus_module_list_types(const GType**, int*)` (`nautilus-module.c` l.79–97). Nautilus loads native `.so` files **only** from compile-time `NAUTILUS_EXTENSIONDIR` (`/usr/lib/nautilus/extensions-4/`, `nautilus-module.c` l.257), requiring root/packaging to install. | `libnautilus-python.so` is already in `/usr/lib/nautilus/extensions-4/` and loads user scripts from `~/.local/share/nautilus-python/extensions/` or `$XDG_DATA_DIRS/nautilus-python/extensions/` (`README` l.1–6). |
| **Bindings & boilerplate** | No maintained `nautilus-extension4` Rust crate; requires manual `GTypeModule` dynamic type registration + `Nautilus*ProviderInterface` vtable FFI. | Official GIR bindings via PyGObject (`from gi.repository import Nautilus, GObject`). |
| **Runtime overhead & IPC** | Single-language repo, zero Python runtime in Nautilus process, direct Rust IPC crate reuse. | Pulls CPython 3.14 into Nautilus process; thin ~100–200 LOC script talking to Rust daemon over D-Bus (`Gio.DBusProxy`) or Unix socket. |

**Recommendation:** Use `nautilus-python` for prototyping and thin UI glue (menu + emblems + D-Bus/socket IPC to the Rust daemon); optionally replace with a packaged Rust `cdylib` later only if avoiding CPython inside Nautilus is desired.

---

## 3. GLib 2.90.1 Local Thumbnail Validation & Nautilus 51.1 Interaction

Verified directly against `glib` `2.90.1` (`gio/glocalfileinfo.c`, `gio/thumbnail-verify.c`, `gio/glocalfile.c`, `gio/gunixmounts.c`, `glib/gconvert.c`), `/usr/lib/libgio-2.0.so.0`, and `gnome-desktop` `51.0` (`libgnome-desktop/gnome-desktop-thumbnail.c`).

### 3.1 URI Canonicalization & MD5 Key (`glocalfileinfo.c`, `gconvert.c`)
- `glocalfileinfo.c: get_thumbnail_attributes()` (l.1468–1474) computes:
  ```c
  uri = g_filename_to_uri (path, NULL, NULL);
  ```
  `g_filename_to_uri()` (`glib/gconvert.c` l.1736–1769) calls `g_escape_file_uri(NULL, filename, error)`, producing `file:///...` (empty hostname → three slashes) with the absolute path percent-encoded per RFC 2396 Section 3.3.
- `basename` is lowercase hex MD5 of the exact UTF-8/byte `uri` string + `".png"`.

### 3.2 Directory Search Order (`glocalfileinfo.c` l.1384–1528)
- **Generic attributes (`thumbnail::path`, `thumbnail::is-valid`, `thumbnailing::failed`):**
  - Iterates `ThumbnailSize` from `THUMBNAIL_SIZE_LAST - 1` (`4`) down to `0` (`glocalfileinfo.c` l.1489–1501):
    `xx-large` → `x-large` → `large` → `normal` under `$XDG_CACHE_HOME/thumbnails/<dir>/<basename>`.
  - Stops (`break`) on the **first** path where `g_file_test(filename, G_FILE_TEST_IS_REGULAR)` is `TRUE`.
  - **Important:** It does **not** continue searching smaller directories if the first match fails validation; it sets `thumbnail::path` to that first file and `thumbnail::is-valid` to `thumbnail_verify(filename, uri, stat_buf)`.
- **Size-specific attributes** (`thumbnail::path-{normal,large,x-large,xx-large}` and matching `is-valid-*` / `failed-*`, l.2186–2224): check only the single corresponding directory.

### 3.3 PNG `tEXt` Validation Rules (`gio/thumbnail-verify.c`)
`thumbnail_verify()` (`thumbnail-verify.c` l.225–254) mmaps the PNG, requires PNG magic `\x89PNG\r\n\x1a\n` (l.132), scans `tEXt` chunks (not `zTXt` or `iTXt`), and checks:

1. **`Thumb::URI` (required):** exact byte-for-byte match against `uri` (`thumbnail-verify.c` l.85–98).
2. **`Thumb::MTime` (required):** decimal ASCII integer seconds matching `_g_stat_mtime(file_stat_buf)` (`st_mtim.tv_sec`, `l.48–75, 100–106, 240`). Note: `"0"` matches `st_mtime == 0`. Fractional/ISO strings fail.
3. **`Thumb::Size` (optional, but validated if present):** `MATCHED_ALL` is `(MATCHED_URI | MATCHED_MTIME)` (`l.44–46`), so omitting `Thumb::Size` passes. **However**, if a `tEXt::Thumb::Size` chunk is present (and reached before scan exit), `check_integer_match(_g_stat_size(file_stat_buf), ...)` **must** match `st_size` in bytes or `thumbnail_verify()` returns `FALSE` (`l.108–116`).
4. **Chunk ordering optimization (`l.197–212`):** once `required_matches == MATCHED_ALL` (`URI` and `MTime` seen), hitting any non-`tEXt` chunk (such as `IDAT`) exits the loop early as valid. Place `Thumb::URI` and `Thumb::MTime` (and `Thumb::Size` if written) at the start of the PNG before `IDAT`.

### 3.4 Fail-Cache Markers: GLib vs. Nautilus 51.1 / `gnome-desktop` 51.0 (Critical!)
- **What GLib 2.90.1 does (`glocalfileinfo.c` l.1509–1523):**
  - Only if **no** regular file exists in `xx-large`, `x-large`, `large`, `normal`, GLib checks:
    `$XDG_CACHE_HOME/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` (`g_file_test(..., G_FILE_TEST_IS_REGULAR)`).
  - If present, GLib leaves `thumbnail::path` **unset** and sets:
    - `thumbnailing::failed = TRUE`
    - `thumbnail::is-valid = thumbnail_verify(filename, uri, stat_buf)`
- **What Nautilus 51.1 actually does (`nautilus-image.c` l.424–495 & `gnome-desktop-thumbnail.c` l.815–993, 1371–1379, 1673–1693):**
  - `NautilusImage` queries `thumbnail::is-valid`, `thumbnailing::failed`, and `thumbnail::path` (`nautilus-image.c` l.663–665), **but ignores `thumbnailing::failed` completely** in `file_info_ready_callback()`.
  - Because `thumbnail::path` is `NULL`, `NautilusImage` proceeds to `nautilus_can_thumbnail(uri, content_type, mtime)` (`nautilus-image.c` l.475) → `gnome_desktop_thumbnail_factory_can_thumbnail()` (`gnome-desktop-thumbnail.c` l.951–993) → `gnome_desktop_thumbnail_factory_has_valid_failed_thumbnail()` (`l.917`).
  - `lookup_failed_thumbnail_path()` checks `$XDG_CACHE_HOME/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` (`appname = "gnome-thumbnail-factory"`, `l.160, 824`) via `validate_thumbnail_path()` (`l.832–846`), which calls **`gdk_pixbuf_new_from_file(path, NULL)`** and `gnome_desktop_thumbnail_is_valid(pixbuf, uri, mtime)` (`l.1673–1693`).
  - **Consequence:**
    - If the fail marker is a **valid PNG image** (e.g. a 1×1 transparent RGBA PNG matching `make_failed_thumbnail()` at `gnome-desktop-thumbnail.c` l.1371–1379) with `tEXt::Thumb::URI == uri` and `tEXt::Thumb::MTime == mtime` (integer seconds), `has_valid_failed_thumbnail` returns `TRUE`, `nautilus_can_thumbnail` returns `FALSE`, `NautilusImage` falls back to the MIME icon (`l.478–487`), and **Nautilus does NOT open or read the source file**.
    - If the fail marker is an empty (0-byte) or non-PNG file, `gdk_pixbuf_new_from_file()` fails, `has_valid_failed_thumbnail` returns `FALSE`, and **Nautilus WILL invoke the thumbnailer and open/download the file**.
    - Note also: `glocalfileinfo.c` line 1340–1376 sniffs up to 16 KiB of a local file (`g_open` + `read`) if `g_content_type_guess(basename, NULL, 0, &result_uncertain)` sets `result_uncertain = TRUE` and `fast = FALSE` — files with standard extensions (`.png`, `.jpg`, `.pdf`) resolve with `result_uncertain = FALSE` without opening the file, whereas extensionless/ambiguous files trigger a 16 KiB sniff.

### 3.5 FUSE Subtypes in GLib 2.90.1 Remote-FS List
Verified in `glib/gio/glocalfile.c` (`is_remote_fs_type`, l.2904–2923), `glib/gio/gunixmounts.c` (`g_unix_is_system_fs_type`, l.329–378), and `strings /usr/lib/libgio-2.0.so.0`:
- **`is_remote_fs_type()`** (`glocalfile.c` l.2904–2923) returns `TRUE` **only** for:
  - `"nfs"`, `"nfs4"`, `"cifs"`, `"smb"`, `"smb2"`, `"fuse.sshfs"`.
- **`g_unix_is_system_fs_type()`** (`gunixmounts.c` l.348–350) includes `"fuse.gvfsd-fuse"`, `"fuse.portal"`, `"fusectl"` (used for hiding OS/API mounts, not `filesystem::remote`).
- **Implication for a custom Rust FUSE mount (`fuse.onedrive`):** GLib reports `filesystem::remote = FALSE`. Under Nautilus's `show-image-thumbnails = 'local-only'`, Nautilus treats the mount as local — meaning pre-seeded thumbnails in `~/.cache/thumbnails/` **will** be displayed, and any file lacking either a valid seeded thumbnail or a valid 1×1 PNG fail-cache marker **will** be opened by Nautilus for thumbnailing.
