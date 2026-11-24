# Live GUI Probe: GNOME Nautilus 51.1 on a `fuse.onedrive-rs` Mount

**Date:** 2026-11-23  
**Purpose:** Empirically verify real GNOME Nautilus 51.1 GUI behavior on a throwaway `fuse.onedrive-rs` FUSE3 mount with pre-seeded freedesktop thumbnails and fail markers, recording exact FUSE `open`/`read` syscall traces, caller process names (`/proc/<pid>/comm`), and `~/.cache/thumbnails/` disk access.

---

## 1. Environment & Pre-Check

Observed package versions and Nautilus preferences (read before and after the probe; no `gsettings` were modified):

```sh
nautilus --version
# GNOME nautilus 51.1

pacman -Q nautilus glib2 gnome-desktop-4
# nautilus 51.1-1
# glib2 2.90.1-1
# gnome-desktop-4 1:51.0-1

gsettings get org.gnome.nautilus.preferences show-image-thumbnails
# 'local-only'

gsettings get org.gnome.nautilus.preferences default-folder-viewer
# 'list-view'

gsettings get org.gnome.nautilus.list-view default-zoom-level
# 'small'
```

Pre-existing state:
- `pgrep -a nautilus`: no running Nautilus instance before or after the test.
- `~/.cache/thumbnails/` did not exist prior to the probe.
- System image thumbnailer (`/usr/share/thumbnailers/glycin-image-rs.thumbnailer`): `/usr/bin/glycin-thumbnailer --input %u --output %o --size %s` (handles `image/png` and other raster formats, sandboxed via `bwrap`).

Because the user's real dconf configuration uses `'list-view'` at `'small'` zoom (which does not render thumbnails in Nautilus rows), we ran two live GUI phases against identical mounts and pre-seeded caches:
1. **Phase 1 (`p1_real_listview`)**: User's real `gsettings` (`default-folder-viewer='list-view'`, `list-view.default-zoom-level='small'`, `show-image-thumbnails='local-only'`).
2. **Phase 2 (`p2_mem_gridview`)**: `GSETTINGS_BACKEND=memory` (in-memory schema defaults from `org.gnome.nautilus.gschema.xml`: `default-folder-viewer='icon-view'`, `icon-view.default-zoom-level='medium'`, `show-image-thumbnails='local-only'`, `thumbnail-limit=50`) — leaves the user's dconf completely untouched while exercising Nautilus's thumbnail display and generation pipeline.

---

## 2. Throwaway FUSE Daemon & Pre-Seeded Cache Setup

A low-level `libfuse3` (`FUSE_USE_VERSION 35`, `<fuse3/fuse_lowlevel.h>`) daemon was compiled with `gcc -Wall -Wextra -O2` and mounted at `/tmp/onedrive-rs-nautilus-probe-mnt`:

```sh
/tmp/onedrive-rs-nautilus-probe-src/probe_fs -f -o subtype=onedrive-rs /tmp/onedrive-rs-nautilus-probe-mnt
```

Observed `/proc/mounts` entry:

```text
onedrive-rs /tmp/onedrive-rs-nautilus-probe-mnt fuse.onedrive-rs rw,nosuid,nodev,relatime,user_id=1000,group_id=1000 0 0
```

All 5 virtual files in `/` reported `st_mode = S_IFREG | 0644`, `st_size = 12582912` (12 MiB), `st_mtime = 1700000000`, `attr_timeout = 0.0`, `entry_timeout = 0.0`. Every FUSE callback (`lookup`, `getattr`, `readdir`, `open`, `read`, `release`, `statfs`) logged timestamp, `fuse_req_ctx(req)->pid`, and `/proc/<pid>/comm`.

| Virtual File | Served Content (`off=0..12582912`) | Pre-Seeded Cache Entry (`~/.cache/thumbnails/...`) | PNG `tEXt` Metadata |
|---|---|---|---|
| `seeded.png` | Valid 64×64 PNG (156 B) + zero pad | `large/2a51b8fcdd29f4dfd1c87371682f1451.png` (256×256, 903 B) | `Thumb::URI`, `Thumb::MTime=1700000000`, `Thumb::Size=12582912` |
| `seeded_both.png` | Valid 64×64 PNG (156 B) + zero pad | `large/9b60d6f9087e409ed1da1c74de3af549.png` (256×256, 908 B)<br>`x-large/9b60d6f9087e409ed1da1c74de3af549.png` (512×512, 2028 B) | `Thumb::URI`, `Thumb::MTime=1700000000`, `Thumb::Size=12582912` |
| `failed.png` | Valid 64×64 PNG (156 B) + zero pad | `fail/gnome-thumbnail-factory/c601b4cbd9c635c842733e5099eefd40.png` (1×1 RGBA, 181 B) | `Thumb::URI`, `Thumb::MTime=1700000000` |
| `unseeded.png` | Valid 64×64 PNG (156 B) + zero pad | *(none)* | — |
| `readme.txt` | `"hello world\n"` + ASCII space pad | *(none)* | — |

In both phases, Nautilus was launched on Wayland with:

```sh
WAYLAND_DISPLAY=wayland-1 XDG_RUNTIME_DIR=/run/user/1000 XDG_SESSION_TYPE=wayland \
  nautilus --new-window /tmp/onedrive-rs-nautilus-probe-mnt
```

Window creation on Hyprland was confirmed in both phases via `hyprctl clients` (`class: org.gnome.Nautilus`, `title: onedrive-rs-nautilus-probe-mnt`, `size: 960,1054`), held open for 7 seconds, and exited cleanly via `nautilus -q`. Meanwhile `inotifywait -m -r -e open,access,create,modify,close_write,moved_to ~/.cache/thumbnails` and `stat -c '%n mtime=%Y atime=%X size=%s'` recorded thumbnail cache activity.

---

## 3. Results

### 3.1 Phase 1: Real User Settings (`list-view`, `small` zoom, `show-image-thumbnails='local-only'`)

Total FUSE callbacks logged: `lookup=29`, `getattr=27`, `statfs=7`, `readdir=3`, `open=4`, `read=4`, `release=4`.

| File | `open()` Count | `read()` Count | Offsets & Sizes | Bytes Read | Caller Process (`comm`) | Thumbnail Cache Activity |
|---|---|---|---|---|---|---|
| `seeded.png` | 1 (`flags=0x48000`) | 1 | `off=0, size=32768` | 32,768 | `pool-2` (Nautilus GIO worker) | None (0 `inotifywait` file events) |
| `seeded_both.png` | 1 (`flags=0x48000`) | 1 | `off=0, size=32768` | 32,768 | `pool-2` (Nautilus GIO worker) | None |
| `failed.png` | 1 (`flags=0x48000`) | 1 | `off=0, size=32768` | 32,768 | `pool-2` (Nautilus GIO worker) | None |
| `unseeded.png` | 1 (`flags=0x48000`) | 1 | `off=0, size=32768` | 32,768 | `pool-2` (Nautilus GIO worker) | None (not thumbnailed) |
| `readme.txt` | **0** | **0** | — | **0** | — | None |

**Observation:**
- `pool-2` (GLib/GIO worker thread inside `nautilus`) opened each `.png` file once with `flags=0x48000` (`O_RDONLY | O_LARGEFILE | O_NOATIME`) and read 32 KiB at offset 0 for `standard::content-type` MIME magic sniffing during directory enumeration.
- `readme.txt` incurred **0 `open()` and 0 `read()` calls** (resolved without magic-byte sniffing).
- At `list-view` + `small` zoom, Nautilus did not instantiate thumbnail widgets or spawn `glycin-thumbnailer`, so no files in `~/.cache/thumbnails/` were opened and `unseeded.png` was not thumbnailed.

---

### 3.2 Phase 2: Grid View (`GSETTINGS_BACKEND=memory`: `icon-view`, `medium` zoom, `show-image-thumbnails='local-only'`)

Total FUSE callbacks logged: `lookup=47`, `getattr=234`, `statfs=7`, `readdir=3`, `open=9`, `read=104`, `release=9`.

| File | `open()` Count | `read()` Count | Exact Offsets & Sizes | Total Bytes Read | Caller Processes (`comm`) | `~/.cache/thumbnails/` Activity |
|---|---|---|---|---|---|---|
| `seeded.png` | 2 (`0x48000`) | 2 | 2 × (`off=0, size=32768`) | 65,536 | `pool-3` (×2, Nautilus) | `large/2a51b8...png` **opened & read** (`atime`: `1795475619 -> 1795475732`, `mtime` unchanged) |
| `seeded_both.png` | 2 (`0x48000`) | 2 | 2 × (`off=0, size=32768`) | 65,536 | `pool-3` (×1), `pool-4` (×1, Nautilus) | **`x-large/9b60d6...png` opened & read** (`atime`: `1795475619 -> 1795475732`); `large/9b60d6...png` **not opened** (`atime` unchanged) |
| `failed.png` | 2 (`0x48000`) | 2 | 2 × (`off=0, size=32768`) | 65,536 | `pool-3` (×1), `pool-5` (×1, Nautilus) | `fail/gnome-thumbnail-factory/c601b4...png` **opened & read** (`atime`: `1795475619 -> 1795475732`, `mtime` unchanged) |
| `unseeded.png` | 3 (`0x48000` ×2, `0x8000` ×1) | 98 | 2 × (`off=0, size=32768`) +<br>96 × (`size=131072`, `off=0, 131072, ..., 12451840`) | 12,648,448<br>(64 KiB + full 12 MiB) | `pool-3` (×1), `pool-2` (×1, Nautilus)<br>`bwrap` (`lookup`/`getattr` only)<br>`glycin-thumbnai` (`glycin-thumbnailer`: 1 `open`, 96 `read`s) | **Generated new thumbnail:** `large/f349682d2bf7b09f913f04cbe72718cd.png` (307 B, 64×64 RGB PNG) |
| `readme.txt` | **0** | **0** | — | **0** | — | None |

#### Selected `inotifywait` Trace (`p2_mem_gridview`)

```text
20:15:32 OPEN /home/henrique/.cache/thumbnails/fail/gnome-thumbnail-factory/c601b4cbd9c635c842733e5099eefd40.png
20:15:32 OPEN /home/henrique/.cache/thumbnails/large/2a51b8fcdd29f4dfd1c87371682f1451.png
20:15:32 OPEN /home/henrique/.cache/thumbnails/x-large/9b60d6f9087e409ed1da1c74de3af549.png
20:15:32 ACCESS /home/henrique/.cache/thumbnails/x-large/9b60d6f9087e409ed1da1c74de3af549.png
20:15:32 ACCESS /home/henrique/.cache/thumbnails/large/2a51b8fcdd29f4dfd1c87371682f1451.png
20:15:32 CREATE /home/henrique/.cache/thumbnails/large/f349682d2bf7b09f913f04cbe72718cd.png.O68DZ3
...
20:15:32 MOVED_TO /home/henrique/.cache/thumbnails/large/f349682d2bf7b09f913f04cbe72718cd.png
```

#### Generated `unseeded.png` Thumbnail Inspection

Inspecting `~/.cache/thumbnails/large/f349682d2bf7b09f913f04cbe72718cd.png` via `PIL.Image`:
- Dimensions: `64×64`, mode `RGB`, size `307` bytes
- PNG `tEXt` metadata:
  - `Software`: `GNOME::ThumbnailFactory`
  - `Thumb::MTime`: `1700000000`
  - `Thumb::URI`: `file:///tmp/onedrive-rs-nautilus-probe-mnt/unseeded.png`

#### Exact Sequence Observed on `unseeded.png` vs Seeded/Failed Files

1. **`20:15:32.805–.806` (Directory load):** Nautilus worker `pool-3` ran `readdir` and queried file info (including `standard::content-type`), triggering the **1st** `open(0x48000)` + `read(off=0, size=32768)` on all 4 `.png` files (0 on `readme.txt`).
2. **`20:15:32.810` (`NautilusImage` widget attribute query):** Nautilus workers (`pool-5`, `pool-3`, `pool-4`, `pool-2`) issued a **2nd** async `g_file_query_info_async` for each cell widget (`nautilus-image.c` requests `standard::content-type` along with `thumbnail::is-valid`, `thumbnailing::failed`, `thumbnail::path`), triggering a **2nd** `open(0x48000)` + `read(off=0, size=32768)` on all 4 `.png` files (still 0 on `readme.txt`).
3. **Cache / fail-marker decision:**
   - For `seeded.png`, Nautilus read `large/2a51b8...png` directly from disk and stopped.
   - For `seeded_both.png`, Nautilus read `x-large/9b60d6...png` directly from disk (`large/` was ignored) and stopped.
   - For `failed.png`, Nautilus/GnomeDesktop read `fail/gnome-thumbnail-factory/c601b4...png` and suppressed thumbnail generation.
   - For `unseeded.png`, no valid thumbnail or fail marker existed, so at `20:15:32.831` Nautilus launched `bwrap` → `glycin-thumbnailer`, which opened `unseeded.png` (`flags=0x8000`, `O_RDONLY | O_LARGEFILE`) at `20:15:32.835` and read all `12,582,912` bytes sequentially in 96 × 128 KiB chunks in ~7 ms before writing `large/f349682d2bf7b09f913f04cbe72718cd.png`.

---

## 4. Conclusions & Implications for `onedrive-rs`

1. **Pre-seeded freedesktop thumbnails and fail markers work end-to-end in live Nautilus 51.1 (`show-image-thumbnails='local-only'`):**
   - Nautilus treats `fuse.onedrive-rs` as a local filesystem, validates and reads pre-seeded `large/` (`256×256`) and `x-large/` (`512×512`) PNGs directly from `~/.cache/thumbnails/`, prefers `x-large/` when both sizes exist, and honors `fail/gnome-thumbnail-factory/<md5>.png` fail markers.
   - Neither `seeded.png`, `seeded_both.png`, nor `failed.png` triggered `glycin-thumbnailer` or any full-file read, and none of the pre-seeded cache files were overwritten.
2. **Every image file still receives up to two 32 KiB header reads at `off=0` on folder open:**
   - Both Nautilus's directory loader and `NautilusImage` (`nautilus-image.c`) query `standard::content-type`, causing GLib's `pool-*` threads to `open()` and `read(off=0, size=32768)` every `.png` twice in grid view (once in list view at small zoom), while `.txt` (`readme.txt`) incurs 0 reads.
   - **Design requirement for `onedrive-rs`:** A single 32 KiB ranged read at `off=0` on a file must **not** trigger a full-file download (otherwise opening a folder of 955 seeded PNGs would still download all 955 files). Serving ranged reads (or caching the first 32–64 KiB header / using HTTP `Range: bytes=0-32767`) keeps directory browsing fast even with MIME sniffing enabled.
3. **Unseeded images under `local-only` trigger full-file reads via `glycin-thumbnailer`:**
   - As `unseeded.png` demonstrated, any image lacking both a valid thumbnail and a valid fail marker will be read in full (`96 × 128 KiB = 12 MiB`) by `glycin-thumbnailer` inside `bwrap`. Pre-seeding either a server-fetched thumbnail or a `fail/gnome-thumbnail-factory/` marker (with matching `Thumb::URI` and `Thumb::MTime`) before Nautilus lists the directory prevents unwanted full downloads.

---

## 5. Cleanup Verification

After completing both phases:
- Unmounted `/tmp/onedrive-rs-nautilus-probe-mnt` via `fusermount3 -u`; `findmnt /tmp/onedrive-rs-nautilus-probe-mnt` exited `1`.
- Deleted the 5 probe thumbnail files (`2a51b8...png`, both `9b60d6...png` files, `c601b4...png`, and `f349682d...png`); `find ~/.cache/thumbnails -type f` returned empty.
- Removed all `/tmp/onedrive-rs-nautilus-probe-*` files and directories.
- Verified `pgrep -a nautilus` returned empty and all `gsettings` keys (`show-image-thumbnails='local-only'`, `default-folder-viewer='list-view'`, `list-view.default-zoom-level='small'`) remained unmodified.
