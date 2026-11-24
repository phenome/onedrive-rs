---
status: accepted
date: 2026-11-23
---

# Rust FUSE3 daemon with freedesktop thumbnail-cache seeding

We will build `onedrive-rs` as a greenfield Rust FUSE3 daemon (`fuser`) backed by a local SQLite metadata tree (`/me/drive/root/delta`), a sparse ranged-read/pin/evict cache, a freedesktop thumbnail-cache seeder, and a thin Nautilus extension—rather than an out-of-tree GVfs backend or a fork of `dmcbane/stratosync`.

## Considered Options

- **Out-of-tree Rust GVfs backend** — rejected. `gvfs 1.62.0-3` installs no public headers or pkg-config files, and `libgvfsdaemon.so` exposes a private, unversioned C ABI (`_GVfsBackendClass` has zero padding and broke in 1.57.1 and post-1.62), making it fragile on Arch rolling upgrades. Reimplementing the private D-Bus + Unix socketpair FD-passing protocol still provides no local cache/pin/async-write model and degrades non-GIO POSIX apps to `gvfsd-fuse`. A custom `gio-vfs` module displaces `GDaemonVfs` process-wide (breaking `trash://`, `sftp://`, `dav://`) and provides zero POSIX visibility. (Evidence: [`docs/research/gvfs-and-nautilus-probe.md`](../research/gvfs-and-nautilus-probe.md).)
- **Fork `dmcbane/stratosync`** — rejected (used as reference only). Upstream `v0.13.0-beta.22` shells out to `rclone` subprocesses for reads (`rclone cat`), cold listings (`rclone lsjson`), and OAuth token refresh, carries legacy `remote_path` baggage, and has no thumbnail support. (Evidence: [`docs/research/graph-api-and-stratosync.md`](../research/graph-api-and-stratosync.md).)
- **Contribute pin/purge to `gvfs-onedrive` upstream (C)** — rejected. Not Rust, depends on GNOME Online Accounts' broad scopes, exposes only 96 px server thumbnails, and lacks POSIX/write-back cache semantics.

## Consequences

- **Thumbnails work under Nautilus `local-only` with zero upstream patches.** Live Nautilus 51.1 probing confirmed that `fuse.onedrive-rs` reports `filesystem::remote=FALSE` and that Nautilus reads pre-seeded PNGs in `~/.cache/thumbnails/{large,x-large}/` and 1×1 transparent RGBA PNG fail markers in `~/.cache/thumbnails/fail/gnome-thumbnail-factory/` without invoking `glycin-thumbnailer` or downloading full originals. (Evidence: [`docs/research/fuse-thumbnail-probe.md`](../research/fuse-thumbnail-probe.md), [`docs/research/nautilus-live-probe.md`](../research/nautilus-live-probe.md).)
- **Nautilus `standard::content-type` sniffing requires small ranged reads.** Live probing showed Nautilus GLib workers issue `read(off=0, size=32768)` on `.png` entries during folder load. The FUSE daemon must serve small offset-0 ranged reads directly without triggering full-file hydration.
- **Full metadata lives locally in SQLite.** Tracking items by `(id, parent_id, name)` from `/me/drive/root/delta` makes `readdir`, `lookup`, and stray walks sub-millisecond local I/O.
