# Nautilus 51 acceptance fixture

Run the preview probe from the workspace. The host needs FUSE3, Nautilus 51,
nautilus-python, PyGObject, D-Bus, Xvfb and xvfb-run, Bubblewrap, ImageMagick,
and libXtst.

```sh
cargo test --locked -p onedrive-core --test live_fuse live_nautilus_preview -- --ignored --nocapture
```

The test mounts two synthetic 12 MiB PNGs through the real FUSE adapter. The
fake cloud supplies preview bytes for one item. Nautilus must render the
preview's distinctive color on screen, recognize the other item's valid fail
marker, set the Cloud-only emblems, and offer Pin when the positive tile is
right-clicked. Fake cloud telemetry must show only offset-zero header reads.

The fixture runs Nautilus on a private D-Bus session and Xvfb display, with
temporary HOME and XDG directories and the memory settings backend. Bubblewrap
hides globally installed Python extensions in the Nautilus process. A temporary
copy of the OneDrive extension records actual Nautilus callbacks, emblem calls,
and socket replies. The reproducible observations use synthetic OneDrive
metadata and bytes throughout.

## Mounted daemon and retention

Run the combined Nautilus and daemon test from the workspace.

```sh
cargo test --locked -p onedrive-rs --test nautilus_fixture live_nautilus_retention -- --ignored --nocapture
```

This runs the real daemon against the fake cloud and checks its read telemetry
before Pin. It opens the native context menu and activates Pin and Purge through
keyboard input. The fixture checks both socket replies, FUSE xattrs, the
emblem rendered in the same window, and pinned byte counts. It then runs CLI
Sticky Pin, Unpin, and Purge along with POSIX read hydration. After each change,
it checks the rendered emblem and context actions on the same visible tile
without navigating or reopening the folder.
The runner signals only its daemon worker, checks that the socket and mount are
gone, and removes the temporary files. It reports a retained mount path if
fallback unmount also fails. For a screenshot when assertions fail, set
`ONEDRIVE_DESKTOP_SCREENSHOT` to a path outside the temporary test directory.
