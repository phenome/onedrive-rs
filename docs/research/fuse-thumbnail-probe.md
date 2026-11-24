# Empirical Probe: FUSE + Freedesktop Thumbnail-Cache Seeding (GLib 2.90.1 & GnomeDesktop 4.0)

**Date:** 2026-11-23  
**Purpose:** Headless empirical verification of whether a `fuse.onedrive-rs` FUSE mount is treated by GLib / GIO as a local filesystem and whether pre-seeded freedesktop thumbnails and fail markers in `~/.cache/thumbnails/` are accepted by GLib (`gio info`) and `GnomeDesktop.DesktopThumbnailFactory` without opening/reading the underlying FUSE files.

---

## 1. Local Environment

Commands and observed outputs:

```sh
rustc --version
# rustc 1.99.1 (58cf8f9c3 2026-10-13) (Arch Linux rust 1:1.99.1-1)

cargo --version
# cargo 1.99.1 (5f94df478 2026-08-27) (Arch Linux rust 1:1.99.1-1)

gcc --version | head -n1
# gcc (GCC) 16.2.1 20260810

pkg-config --modversion fuse3
# 3.18.3

fusermount3 --version
# fusermount3 version: 3.18.3

gio --version
# 2.90.1
```

Python GObject Introspection (`gi.repository`) checks:
- `GLib`: `2.90.1`
- `Gio`: available
- `GnomeDesktop`: `3.0` and `4.0` available (`4.0` used in probes)
- `Nautilus 4.0`: not available (`ValueError: Namespace Nautilus not available for version 4.0`)
- `PIL` (`Pillow`): available (used to generate valid PNGs with `tEXt` chunks)

`~/.cache/thumbnails/` did not exist on the machine prior to the probe.

---

## 2. Throwaway FUSE Probe Setup

Implemented a minimal low-level `libfuse3` (`FUSE_USE_VERSION 35`, `<fuse3/fuse_lowlevel.h>`) daemon in `/tmp/onedrive-rs-probe-src/probe_fs.c`, compiled with:

```sh
gcc -Wall -Wextra -O2 /tmp/onedrive-rs-probe-src/probe_fs.c $(pkg-config --cflags --libs fuse3) -o /tmp/onedrive-rs-probe-src/probe_fs
```

Mounted at `/tmp/onedrive-rs-probe-mnt`:

```sh
/tmp/onedrive-rs-probe-src/probe_fs -f -o subtype=onedrive-rs /tmp/onedrive-rs-probe-mnt
```

Observed `/proc/mounts` entry:

```text
onedrive-rs /tmp/onedrive-rs-probe-mnt fuse.onedrive-rs rw,nosuid,nodev,relatime,user_id=1000,group_id=1000 0 0
```

Virtual root entries (`st_size = 12582912` = 12 MiB, `st_mtime = 1700000000` + configurable offset, `attr_timeout = 0.0`, `entry_timeout = 0.0`):

```text
timeout 10 ls -la /tmp/onedrive-rs-probe-mnt
total 36864
drwxr-xr-x   2 henrique henrique        0 Nov 14  2023 .
drwxrwxrwt 221 root     root        17340 Nov 23 20:08 ..
-rw-r--r--   1 henrique henrique 12582912 Nov 14  2023 failed.png
-rw-r--r--   1 henrique henrique 12582912 Nov 14  2023 seeded.png
-rw-r--r--   1 henrique henrique 12582912 Nov 14  2023 unseeded.png
```

Synthetic cache entries written before probing:
- `seeded.png`: 256×256 PNG written to `~/.cache/thumbnails/large/fce32e6cb42af860a7ab7b0ecd2fb96c.png` (`md5("file:///tmp/onedrive-rs-probe-mnt/seeded.png")`) with `Thumb::URI` and `Thumb::MTime`.
- `failed.png`: 1×1 transparent PNG written to `~/.cache/thumbnails/fail/gnome-thumbnail-factory/<md5("file:///tmp/onedrive-rs-probe-mnt/failed.png")>.png` with `Thumb::URI` and `Thumb::MTime`.
- `unseeded.png`: no thumbnail and no fail marker.

Every FUSE callback (`lookup`, `getattr`, `readdir`, `open`, `read`) was logged with timestamps and per-inode `open`/`read` counts.

---

## 3. Probe Results

### (a) `filesystem::remote` on `fuse.onedrive-rs` vs `fuse.sshfs`

Command:

```sh
timeout 10 gio info -f -a filesystem::* /tmp/onedrive-rs-probe-mnt
```

Observed output on `fuse.onedrive-rs`:

```text
attributes:
  filesystem::type: fuse
  filesystem::readonly: FALSE
  filesystem::remote: FALSE
```

Bonus check (f) — same binary mounted with `-o subtype=sshfs` (`/proc/mounts`: `sshfs /tmp/onedrive-rs-probe-mnt fuse.sshfs ...`):

```text
attributes:
  filesystem::type: fuse
  filesystem::readonly: FALSE
  filesystem::remote: TRUE
```

**Conclusion:** GLib 2.90.1 reports `filesystem::remote: FALSE` for `fuse.onedrive-rs` (and `TRUE` for `fuse.sshfs`), so Nautilus under `show-image-thumbnails=local-only` treats `fuse.onedrive-rs` as a local filesystem.

---

### (b) `seeded.png` — Thumbnail Validation and FUSE `open`/`read` Syscall Counts

#### Initial full-wildcard `gio info` observation (`standard::*` surprise)

When running the combined query:

```sh
timeout 10 gio info -a filesystem::*,standard::*,time::*,thumbnail::*,thumbnailing::*,preview::* \
  /tmp/onedrive-rs-probe-mnt/seeded.png \
  /tmp/onedrive-rs-probe-mnt/failed.png \
  /tmp/onedrive-rs-probe-mnt/unseeded.png
```

Observed attributes for `seeded.png` (without `Thumb::Size`):

```text
  standard::content-type: image/png
  standard::fast-content-type: image/png
  standard::size: 12582912
  time::modified: 1700000000
  thumbnail::path: /home/henrique/.cache/thumbnails/large/fce32e6cb42af860a7ab7b0ecd2fb96c.png
  thumbnail::is-valid: TRUE
  thumbnail::path-large: /home/henrique/.cache/thumbnails/large/fce32e6cb42af860a7ab7b0ecd2fb96c.png
  thumbnail::is-valid-large: TRUE
```

However, the FUSE log showed **1 `open` and 1 `read(off=0, size=32768)` per file** across all 3 files:

```text
lookup parent=1 name=seeded.png -> ino=2 mtime=1700000000
getattr ino=2 name=seeded.png mtime=1700000000 size=12582912
lookup parent=1 name=.hidden -> ENOENT
lookup parent=1 name=seeded.png -> ino=2 mtime=1700000000
open ino=2 name=seeded.png flags=0x48000 count=1
getattr ino=2 name=seeded.png mtime=1700000000 size=12582912
read ino=2 name=seeded.png off=0 size=32768 sent=32768 count=1
```

#### Isolating which GIO attribute triggered `open()`/`read()`

Isolated mounts were run for each attribute subset and for `GnomeDesktop.DesktopThumbnailFactory`:

| Probe | Attributes / API | `seeded.png` open/read | `failed.png` open/read | `unseeded.png` open/read |
|---|---|---|---|---|
| `A_thumb_only` | `gio info -a 'time::*,thumbnail::*,thumbnailing::*,preview::*'` | `0 / 0` | `0 / 0` | `0 / 0` |
| `B_fast_content_type` | `gio info -a 'standard::type,standard::name,standard::size,standard::fast-content-type,time::modified,thumbnail::*,thumbnailing::*,preview::*'` | `0 / 0` | `0 / 0` | `0 / 0` |
| `C_standard_content_type` | `gio info -a 'standard::content-type'` | `1 / 1` (32 KiB) | `1 / 1` (32 KiB) | `1 / 1` (32 KiB) |
| `D_gnome_desktop_only` | `GnomeDesktop.DesktopThumbnailFactory` (`lookup`, `has_valid_failed_thumbnail`, `can_thumbnail`) | `0 / 0` | `0 / 0` | `0 / 0` |

**Conclusion:**
- Thumbnail lookup and validation (`thumbnail::*`, `time::*`, `standard::fast-content-type`, and `GnomeDesktop.DesktopThumbnailFactory`) perform **0 `open()` and 0 `read()` calls** on the FUSE mount.
- Requesting **`standard::content-type`** (included in `standard::*`) causes GLib's local file backend to `open()` and `read(off=0, size=32768)` every file for MIME magic-byte sniffing. (Design implication for `onedrive-rs`: when Nautilus or another local GIO client queries `standard::content-type` on a `fuse.*` mount with `filesystem::remote: FALSE`, the daemon will receive a 32 KiB header read at offset 0 even if the file is not opened by the user.)

---

### (c) `Thumb::MTime`, `Thumb::Size`, and Mtime Invalidation

Tested against `seeded.png` (`st_mtime = 1700000000`, `st_size = 12582912`):

| Cache PNG metadata (`tEXt`) | Mount `st_mtime` | `gio info` `thumbnail::is-valid` | `gio info` `thumbnail::path` | `DesktopThumbnailFactory.lookup(uri, mtime)` |
|---|---|---|---|---|
| `Thumb::URI` + `Thumb::MTime=1700000000` (no `Thumb::Size`) | `1700000000` | `TRUE` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` |
| `Thumb::URI` + `Thumb::MTime=1700000000` + `Thumb::Size=12582912` | `1700000000` | `TRUE` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` |
| `Thumb::URI` + `Thumb::MTime=1700000000` + `Thumb::Size=999` | `1700000000` | **`FALSE`** | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` |
| `Thumb::URI` + `Thumb::MTime=1700000000.0` (float string) | `1700000000` | **`FALSE`** | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` |
| `Thumb::URI` + `Thumb::MTime=1700000000` + `Thumb::Size=12582912` | **`1700000001`** (+1s) | **`FALSE`** | `.../fce32e6cb42af860a7ab7b0ecd2fb96c.png` | **`None`** |

**Conclusion:**
- GLib 2.90.1 requires `Thumb::MTime` to be an exact integer-second string matching `st_mtime` (`1700000000.0` fails GLib validation; a +1s `st_mtime` change invalidates both GLib `thumbnail::is-valid` and `DesktopThumbnailFactory.lookup`).
- `Thumb::Size` is optional in GLib 2.90.1 (omitting it passes), **but if present, GLib 2.90.1 validates it against `st_size`** (a mismatch sets `thumbnail::is-valid: FALSE`, even though `DesktopThumbnailFactory.lookup` does not reject on `Thumb::Size`).

---

### (d) `failed.png` — Fail-Marker Behavior

With `~/.cache/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` (`Thumb::URI` + `Thumb::MTime=1700000000`):

- **`gio info` attributes (matching `st_mtime = 1700000000`):**
  Note: GLib 2.90.1 exposes `thumbnail::failed*`, not `thumbnailing::failed*`:
  ```text
  thumbnail::failed: TRUE
  thumbnail::is-valid: TRUE
  thumbnail::failed-normal: TRUE
  thumbnail::is-valid-normal: TRUE
  thumbnail::failed-large: TRUE
  thumbnail::is-valid-large: TRUE
  thumbnail::failed-xlarge: TRUE
  thumbnail::is-valid-xlarge: TRUE
  thumbnail::failed-xxlarge: TRUE
  thumbnail::is-valid-xxlarge: TRUE
  ```
- **`GnomeDesktop.DesktopThumbnailFactory` (`mtime = 1700000000`):**
  - `lookup` -> `None`
  - `has_valid_failed_thumbnail` -> `True`
  - `can_thumbnail` -> `False`
- **FUSE `open`/`read` counts (without `standard::content-type`):** `0 / 0`.
- **After bumping `st_mtime` +1s to `1700000001`:**
  - `gio info`: `thumbnail::failed: TRUE`, `thumbnail::is-valid: FALSE` (and `-normal`/`-large`/`-xlarge`/`-xxlarge` all `is-valid-*: FALSE`).
  - `DesktopThumbnailFactory`: `has_valid_failed_thumbnail` -> `False`, `can_thumbnail` -> `True`.

---

### (e) `unseeded.png` — Unseeded File Behavior

With no thumbnail and no fail marker:
- **`gio info`:** no `thumbnail::*` attributes are emitted at all (`thumbnail::path` absent, `thumbnail::failed` absent).
- **`GnomeDesktop.DesktopThumbnailFactory` (`mtime = 1700000000`):**
  - `lookup` -> `None`
  - `has_valid_failed_thumbnail` -> `False`
  - `can_thumbnail('file:///tmp/onedrive-rs-probe-mnt/unseeded.png', 'image/png', 1700000000)` -> `True`

This confirms Nautilus / `GnomeDesktopThumbnailFactory` will attempt thumbnail generation (opening and reading the file) for any unseeded image lacking a matching fail marker.

---

## 4. Summary of Implications for `onedrive-rs`

1. **Freedesktop thumbnail seeding works on `fuse.onedrive-rs`:** GLib 2.90.1 reports `filesystem::remote: FALSE` and `thumbnail::is-valid: TRUE` for pre-seeded PNGs in `~/.cache/thumbnails/large/<md5(file://...)>.png`, and `GnomeDesktopThumbnailFactory` finds them with **0 `open()`/`read()` calls** during thumbnail lookup.
2. **Fail markers suppress generation cleanly:** Writing a 1×1 PNG with `Thumb::URI` and `Thumb::MTime` under `~/.cache/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` makes GLib report `thumbnail::failed: TRUE` + `thumbnail::is-valid: TRUE` and makes `DesktopThumbnailFactory.can_thumbnail` return `False`.
3. **Strict `Thumb::MTime` and `Thumb::Size` formatting:** Write `Thumb::MTime` as an exact integer string equal to `st_mtime` (no `.0`), and either omit `Thumb::Size` or set it to the exact `st_size` reported by FUSE `getattr`.
4. **Watch out for `standard::content-type` header reads:** Unlike `thumbnail::*` and `standard::fast-content-type` (which incur 0 `open`/`read` calls), querying `standard::content-type` on a local FUSE mount causes GLib to `open()` and `read(off=0, size=32768)` every file to sniff magic bytes. The FUSE daemon should handle small offset-0 ranged reads gracefully (or cache/serve headers) so directory browsing by GIO clients does not trigger full-file downloads.
