# onedrive-rs

A cloud-first Microsoft OneDrive (Personal) client in Rust for Linux desktops, targeting GNOME Nautilus and POSIX tools.

- **Cloud-first FUSE3 mount** outside `$HOME` (`/run/user/$UID/onedrive`) backed by a local SQLite metadata tree (`/me/drive/root/delta`).
- **Server-side thumbnails** seeded into `~/.cache/thumbnails/` (`x-large` 512 px PNGs + valid fail markers) with two-tier sparse ranged reads (`64 KiB` header sniff → `2 MiB` streaming chunks) so browsing never triggers full-file downloads.
- **Pin, Sticky Pin, Purge, and Eviction** with conservative defaults (`1 GiB` unpinned cache, `7 days` idle age, `5 GiB` free-disk floor).
- **Async write-back** with a persistent upload queue and conflict-copy preservation on `If-Match` / `eTag` mismatch.
- **Shared local IPC** serving the CLI, a thin `nautilus-python` extension (emblems + Pin/Purge menu), and (Phase 2) a native Rust `gpui` management app.

## Design Docs

- [GLOSSARY.md](GLOSSARY.md) — canonical domain language
- [ADR-0001: Rust FUSE3 daemon with freedesktop thumbnail-cache seeding](docs/adr/0001-rust-fuse-daemon-with-thumbnail-seeding.md)
- [ADR-0002: First-visit thumbnail seeding and two-tier sparse reads](docs/adr/0002-thumbnail-seeding-and-two-tier-sparse-reads.md)
- [docs/research/](docs/research/) — primary-source and empirical probe reports
