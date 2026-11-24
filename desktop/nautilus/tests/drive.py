"""Drive the private Nautilus window and assert rendered and extension state."""

import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import time

import gi

from gi.repository import Gio

ROOT = Path(os.environ["ONEDRIVE_DESKTOP_TEST"])
MOUNT = Path(os.environ["ONEDRIVE_RS_MOUNT"])
VIEW = MOUNT / "media" if sys.argv[1] == "retention" else MOUNT
POSITIVE = "café; #%.png"
NEGATIVE = "failed.png"
PREVIEW_COLOR = bytes((0, 100, 200))


def events():
    path = ROOT / "desktop-events"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def wait_for(label, probe):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError(f"{label} did not appear. Events: {events()}")


def preview_location():
    width, height = 1024, 768  # Xvfb dimensions in desktop.sh
    pixels = subprocess.run(
        ["magick", "import", "-window", "root", "-depth", "8", "rgb:-"],
        check=True, capture_output=True, timeout=3,
    ).stdout
    assert len(pixels) == width * height * 3
    rows = []
    for y in range(height):
        row = pixels[y * width * 3:(y + 1) * width * 3]
        columns = [x for x in range(width) if row[x * 3:x * 3 + 3] == PREVIEW_COLOR]
        if len(columns) >= 30:
            rows.append((y, columns))
    if len(rows) >= 15:
        y, columns = rows[len(rows) // 2]
        assert columns[-1] - columns[0] + 1 == len(columns), "Multiple preview-colored tiles"
        return columns[len(columns) // 2], y
    return None


def screen_crop(x, y, width, height):
    return subprocess.run(
        ["magick", "import", "-window", "root", "-crop",
         f"{width}x{height}+{x}+{y}", "-depth", "8", "rgb:-"],
        check=True, capture_output=True, timeout=3,
    ).stdout


def thumbnail_size(name, subdirectory):
    uri = Gio.File.new_for_path(str(VIEW / name)).get_uri()
    filename = hashlib.md5(uri.encode()).hexdigest() + ".png"
    data = (Path(os.environ["XDG_CACHE_HOME"]) / "thumbnails" / subdirectory / filename).read_bytes()
    assert data[:16] == b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR"
    return struct.unpack(">II", data[16:24])


def thumbnail_attributes(name):
    file = Gio.File.new_for_path(str(VIEW / name))
    return file.query_info("thumbnail::*", Gio.FileQueryInfoFlags.NONE, None)


def input_event(button=None, point=None, keys=()):
    x11 = c.CDLL("libX11.so.6")
    xtest = c.CDLL("libXtst.so.6")
    x11.XOpenDisplay.restype = c.c_void_p
    display = c.c_void_p(x11.XOpenDisplay(None))
    assert display
    try:
        if button is None:
            for symbol in keys:
                keycode = x11.XKeysymToKeycode(display, c.c_ulong(symbol))
                assert xtest.XTestFakeKeyEvent(display, keycode, 1, 0)
                assert xtest.XTestFakeKeyEvent(display, keycode, 0, 0)
        else:
            assert xtest.XTestFakeMotionEvent(display, -1, *point, 0)
            assert xtest.XTestFakeButtonEvent(display, button, 1, 0)
            assert xtest.XTestFakeButtonEvent(display, button, 0, 0)
        x11.XSync(display, 0)
    finally:
        x11.XCloseDisplay(display)


def daemon_request(request):
    path = Path(os.environ["XDG_RUNTIME_DIR"]) / "onedrive-rs/daemon.sock"
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(2)
        connection.connect(str(path))
        connection.sendall(json.dumps(request).encode() + b"\n")
        with connection.makefile("rb") as replies:
            return json.loads(replies.readline(65536))


def daemon_status():
    response = daemon_request({"op": "status"})
    assert response["ok"] is True, response
    return response["result"]


def emblem_pixels(location):
    x, y = location
    # At the fixture's default grid zoom, the emblem sits at the top right.
    return screen_crop(x + 46, y - 53, 24, 24)


def perform_action(location, operation, status, emblem):
    previous = len(events())
    x, y = location
    # The menu overlaps the lower-right part of the visible thumbnail.
    def menu_patch():
        return screen_crop(x + 5, y + 4, 225, 220)

    tile = menu_patch()
    input_event(3, location)
    wait_for(f"visible {operation} menu", lambda: menu_patch() != tile)
    assert ["menu", POSITIVE, [[f"OneDrive::{operation}", True]]] in events()
    # Nautilus 51 places the single extension item just above Properties.
    for symbol in (0xFF52, 0xFF52, 0xFF0D):  # Up, Up, Return
        # Let GTK finish opening the popover and move focus between key events.
        time.sleep(0.15)
        input_event(keys=(symbol,))

    def observed():
        current = events()[previous:]
        return (
            ["status", POSITIVE, status] in current
            and ["emblem", POSITIVE, emblem] in current
        )

    reply = wait_for(
        f"{operation} socket reply",
        lambda: next(
            (event for event in events()[previous:]
             if event[:3] == ["reply", operation, f"/media/{POSITIVE}"]),
            None,
        ),
    )
    assert reply[3] is None, reply
    wait_for(f"{operation} status and emblem refresh", observed)
    state, pin = status.split(":")
    assert os.getxattr(VIEW / POSITIVE, "user.onedrive.state") == state.encode()
    assert os.getxattr(VIEW / POSITIVE, "user.onedrive.pin") == pin.encode()


def verify_external_changes(location, cloud_only):
    def check_refresh(change, status, emblem, menu, previous_pixels):
        previous = len(events())
        change()
        wait_for(f"{status} on the existing tile", lambda: (
            ["status", POSITIVE, status] in events()[previous:]
            and ["emblem", POSITIVE, emblem] in events()[previous:]
        ))
        wait_for(f"{status} rendered", lambda: emblem_pixels(location) != previous_pixels)
        rendered = emblem_pixels(location)
        x, y = location
        def menu_patch():
            return screen_crop(x + 5, y + 4, 225, 220)

        closed = menu_patch()
        input_event(3, location)
        wait_for(f"visible {status} menu", lambda: menu_patch() != closed)
        wait_for(f"{status} context actions", lambda: (
            ["menu", POSITIVE, [[f"OneDrive::{action}", True] for action in menu]]
            in events()[previous:]
        ))
        input_event(keys=(0xFF1B,))  # Escape closes the popover without leaving the folder.
        wait_for("context menu closed", lambda: menu_patch() == closed)
        return rendered

    def cli(operation, path):
        subprocess.run(
            [os.environ["ONEDRIVE_DESKTOP_CLI"], operation, os.fspath(path)],
            check=True, capture_output=True, timeout=5,
        )

    pinned = check_refresh(lambda: cli("pin", VIEW),
                           "Hydrated:pinned", "starred-symbolic", ["purge"], cloud_only)
    hydrated = check_refresh(lambda: cli("unpin", VIEW),
                             "Hydrated:none", "object-select-symbolic", ["pin", "purge"], pinned)
    purged = check_refresh(lambda: cli("purge", VIEW / POSITIVE),
                           "Cloud-only:none", "folder-remote-symbolic", ["pin"], hydrated)
    assert purged == cloud_only
    check_refresh(lambda: subprocess.run(
        ["cat", os.fspath(VIEW / POSITIVE)], stdout=subprocess.DEVNULL, check=True, timeout=5,
    ), "Hydrated:none", "object-select-symbolic", ["pin", "purge"], purged)
    print("CLI Sticky Pin, Unpin, Purge and read hydration refreshed the same tile and context actions.")


def verify(mode):
    nautilus = [
        "bwrap", "--die-with-parent", "--dev-bind", "/", "/",
        "--tmpfs", "/usr/share/nautilus-python/extensions", "--", "nautilus",
    ]
    with (ROOT / "nautilus.log").open("w") as log:
        # The version switch also needs a display with Nautilus 51.
        version = subprocess.run(
            [*nautilus, "--version"], check=True, capture_output=True, text=True, timeout=4,
        )
        print(version.stdout.strip())
        assert version.stdout.strip().startswith("GNOME nautilus 51."), version.stdout
        if mode == "retention":
            wait_for("fixture sync", lambda: daemon_status()["sync_state"] == "idle")
            wrong_mount = daemon_request({
                "op": "desktop_changes", "mount": str(VIEW), "epoch": None, "since": None,
            })
            assert wrong_mount["ok"] is False, wrong_mount
        desktop = subprocess.Popen([*nautilus, "--new-window", str(VIEW)], stdout=log, stderr=log, start_new_session=True)
        try:
            for name in (POSITIVE, NEGATIVE):
                wait_for(name, lambda: ["status", name, "Cloud-only:none"] in events())
                assert ["emblem", name, "folder-remote-symbolic"] in events()
            assert ["thumbnail-policy", "local-only"] in events()
            assert thumbnail_size(POSITIVE, "x-large") == (512, 256)
            assert thumbnail_size(NEGATIVE, "fail/gnome-thumbnail-factory") == (1, 1)
            seeded = thumbnail_attributes(POSITIVE)
            missing = thumbnail_attributes(NEGATIVE)
            assert seeded.get_attribute_boolean("thumbnail::is-valid")
            assert missing.get_attribute_boolean("thumbnail::is-valid")
            assert missing.get_attribute_boolean("thumbnail::failed")
            assert not any(
                missing.has_attribute(key) for key in missing.list_attributes("thumbnail")
                if "path" in key
            )
            location = wait_for("visible server thumbnail", preview_location)
            print("Nautilus rendered the seeded preview and accepted the missing preview marker.")
            if mode == "retention":
                assert daemon_status()["pinned_cache_bytes"] == 0
                reads = [json.loads(line) for line in (ROOT / "reads.jsonl").read_text().splitlines()]
                assert sorted(reads) == sorted(
                    [[name, 0, 65536] for name in (POSITIVE, NEGATIVE)]
                ), reads
                print(f"Nautilus source reads before Pin: {reads}")
                unselected = emblem_pixels(location)
                input_event(1, location)
                cloud_only = wait_for(
                    "selected tile", lambda: (pixels := emblem_pixels(location)) != unselected and pixels,
                )
                perform_action(location, "pin", "Hydrated:pinned", "starred-symbolic")
                assert daemon_status()["pinned_cache_bytes"] == 12 * 1024 * 1024
                wait_for("visible pinned emblem", lambda: emblem_pixels(location) != cloud_only)
                perform_action(location, "purge", "Cloud-only:none", "folder-remote-symbolic")
                assert daemon_status()["pinned_cache_bytes"] == 0
                wait_for("visible Cloud-only emblem", lambda: emblem_pixels(location) == cloud_only)
                print("Nautilus Pin and Purge updated the daemon cache and refreshed emblems.")
                verify_external_changes(location, cloud_only)
            else:
                input_event(1, location)
                input_event(3, location)
                wait_for(
                    "Nautilus context menu",
                    lambda: ["menu", POSITIVE, [["OneDrive::pin", True]]] in events(),
                )
                print("Nautilus offered Pin for the Cloud-only item.")
            assert desktop.poll() is None
        except Exception:
            if os.environ.get("ONEDRIVE_DESKTOP_SCREENSHOT"):
                subprocess.run(
                    ["magick", "import", "-window", "root", os.environ["ONEDRIVE_DESKTOP_SCREENSHOT"]],
                    timeout=3,
                )
            print((ROOT / "nautilus.log").read_text(), file=sys.stderr)
            raise
        finally:
            if desktop.poll() is None:
                os.killpg(desktop.pid, signal.SIGTERM)
                try:
                    desktop.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(desktop.pid, signal.SIGKILL)
                    desktop.wait()
            diagnostics = (ROOT / "nautilus.log").read_text()
            assert "Traceback (most recent call last)" not in diagnostics, diagnostics


# The outer timeout must give Nautilus its normal cleanup on failure.
def interrupted(signum, frame):
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, interrupted)
verify(sys.argv[1])
