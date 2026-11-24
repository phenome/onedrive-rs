# Nautilus 51 acceptance fixture

Run the preview probe from the workspace. The host needs FUSE3, Nautilus 51,
nautilus-python, PyGObject, D-Bus, Xvfb and xvfb-run, Bubblewrap, ImageMagick,
and libXtst.

```sh
cargo test -p onedrive-core --test live_fuse live_nautilus_preview -- --ignored --nocapture
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

Once #8's daemon and #5/#6 are integrated, stage the daemon fixture as a test in
the binary crate. Then pass Cargo's compiled test executable to the runner.

```sh
cp desktop/nautilus/tests/daemon.rs crates/onedrive-rs/tests/nautilus_fixture.rs
worker=$(
    cargo test --locked -p onedrive-rs --test nautilus_fixture --no-run --message-format=json |
    python -c 'import json,sys; print(next(o["executable"] for o in map(json.loads, sys.stdin) if o.get("target", {}).get("name") == "nautilus_fixture" and o.get("executable")))'
)
timeout -k 3s 45s bash desktop/nautilus/tests/retention.sh "$worker"
```

This runs the real daemon against the fake cloud and checks its read telemetry
before Pin. A right-click invokes the native MenuProvider. The fixture
activates the returned Nautilus menu item and asserts the socket reply,
refreshed status and emblem, and pinned byte count for both Pin and Purge.
The runner signals only its daemon worker, checks that the socket and mount are
gone, and removes the temporary files. It reports a retained mount path if
fallback unmount also fails. For a screenshot when assertions fail, set
`ONEDRIVE_DESKTOP_SCREENSHOT` to a path outside the temporary test directory.
