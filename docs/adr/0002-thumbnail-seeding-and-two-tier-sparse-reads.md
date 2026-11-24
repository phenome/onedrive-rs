---
status: accepted
date: 2026-11-23
---

# First-visit thumbnail seeding (`x-large` + fail markers) and two-tier sparse reads

To render server-side Microsoft Graph thumbnails in Nautilus 51 under `show-image-thumbnails=local-only` without triggering full-file downloads, `onedrive-rs` seeds `~/.cache/thumbnails/` on the first `readdir` of any folder containing thumbnailable media and serves FUSE reads from a two-tier sparse block cache (64 KiB header chunk, 2 MiB streaming chunks).

## Context & Evidence

Live Nautilus 51.1 tracing ([`docs/research/nautilus-live-probe.md`](../research/nautilus-live-probe.md)) and headless GLib 2.90.1 probing ([`docs/research/fuse-thumbnail-probe.md`](../research/fuse-thumbnail-probe.md)) established four constraints:
1. Nautilus prefers `x-large/` (512 px) over `large/` (256 px) when present and validates PNG `tEXt::Thumb::URI` and integer-second `tEXt::Thumb::MTime` (`Thumb::Size` optional, but must match `st_size` if written).
2. Any thumbnailable file lacking both a valid thumbnail and a valid fail marker is handed to `bwrap` + `glycin-thumbnailer` within ~25 ms, which reads the entire file (e.g., 12 MiB per 4K PNG).
3. Nautilus 51.1 validates fail markers via `gdk_pixbuf_new_from_file()`, so fail markers in `~/.cache/thumbnails/fail/gnome-thumbnail-factory/<md5>.png` must be valid PNGs (1×1 transparent RGBA PNG with `Thumb::URI` and `Thumb::MTime`), never 0-byte files.
4. Even when thumbnails are seeded, Nautilus GLib worker threads (`pool-*`) query `standard::content-type` on `.png` entries, issuing `open()` + `read(off=0, size=32768)` (twice in grid view, once in list view).

## Decision

- **First-visit `readdir` thumbnail seeding:** Only when a folder contains thumbnailable items (images, PDFs, videos, Office docs) and has unseeded entries, `readdir` fetches `GET /me/drive/items/{id}/children?$expand=thumbnails`, downloads Graph's `large` (800 px) thumbnails concurrently over HTTP/2, re-encodes them as ≤512 px PNGs in `~/.cache/thumbnails/x-large/<md5>.png`, and writes 1×1 transparent RGBA PNG fail markers for thumbnailable items that Graph cannot thumbnail—before returning `readdir` so `glycin-thumbnailer` cannot race ahead. Non-media folders (e.g., `node_modules`) incur zero thumbnail requests; subsequent opens are instant from disk.
- **Two-tier sparse ranged-read cache:** Reads starting at offset 0 with length `≤ 64 KiB` fetch only `Range: bytes=0-65535` from `@microsoft.graph.downloadUrl` and cache that 64 KiB block locally (serving genuine bytes so repeat MIME sniffs hit disk in <1 ms, capping a 955-PNG folder at ~60 MB instead of 11 GB). Reads beyond 64 KiB fetch 2 MiB chunks for fast sequential hydration.

## Consequences

- Opening a large unvisited photo folder (e.g., 955 PNGs) takes a few seconds once for concurrent thumbnail seeding, then opens instantaneously on every subsequent visit.
- Never synthesize fake bytes on `read()` to fool MIME sniffing; POSIX callers always see authentic file bytes.
