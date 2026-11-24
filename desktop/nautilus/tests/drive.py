"""Drive the private Nautilus window and assert rendered and extension state."""

import ctypes as c
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import gi

from gi.repository import Gio

ROOT = Path(os.environ["ONEDRIVE_DESKTOP_TEST"])
MOUNT = Path(os.environ["ONEDRIVE_RS_MOUNT"])
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


def thumbnail_attributes(name):
    file = Gio.File.new_for_path(str(MOUNT / name))
    return file.query_info("thumbnail::*", Gio.FileQueryInfoFlags.NONE, None)


def input_event(button=None, point=None):
    x11 = c.CDLL("libX11.so.6")
    xtest = c.CDLL("libXtst.so.6")
    x11.XOpenDisplay.restype = c.c_void_p
    display = c.c_void_p(x11.XOpenDisplay(None))
    assert display
    try:
        if button is None:
            keycode = x11.XKeysymToKeycode(display, c.c_ulong(0xFF1B))  # Escape
            assert xtest.XTestFakeKeyEvent(display, keycode, 1, 0)
            assert xtest.XTestFakeKeyEvent(display, keycode, 0, 0)
        else:
            assert xtest.XTestFakeMotionEvent(display, -1, *point, 0)
            assert xtest.XTestFakeButtonEvent(display, button, 1, 0)
            assert xtest.XTestFakeButtonEvent(display, button, 0, 0)
        x11.XSync(display, 0)
    finally:
        x11.XCloseDisplay(display)


def daemon_status():
    path = Path(os.environ["XDG_RUNTIME_DIR"]) / "onedrive-rs/daemon.sock"
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(2)
        connection.connect(str(path))
        connection.sendall(b'{"op":"status"}\n')
        with connection.makefile("rb") as replies:
            response = json.loads(replies.readline(65536))
    assert response["ok"] is True, response
    return response["result"]


def perform_action(location, operation, status, emblem):
    trigger = ROOT / "desktop-action"
    previous = len(events())
    pending = trigger.with_suffix(".tmp")
    pending.write_text(json.dumps([POSITIVE, operation]))
    pending.rename(trigger)
    input_event(1, location)
    input_event(3, location)

    def observed():
        current = events()[previous:]
        return (
            ["status", POSITIVE, status] in current
            and ["emblem", POSITIVE, emblem] in current
            and any(
                event[:2] == ["menu", POSITIVE]
                and [f"OneDrive::{operation}", True] in event[2]
                for event in current
            )
        )

    reply = wait_for(
        f"{operation} socket reply",
        lambda: next(
            (event for event in events()[previous:]
             if event[:3] == ["reply", operation, f"/{POSITIVE}"]),
            None,
        ),
    )
    assert reply[3] is None, reply
    wait_for(f"{operation} status and emblem refresh", observed)
    assert not trigger.exists()
    input_event()


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
        assert version.stdout.strip().startswith("GNOME nautilus 51."), version.stdout
        if mode == "retention":
            wait_for("fixture sync", lambda: daemon_status()["sync_state"] == "idle")
        desktop = subprocess.Popen([*nautilus, "--new-window", str(MOUNT)], stdout=log, stderr=log, start_new_session=True)
        try:
            for name in (POSITIVE, NEGATIVE):
                wait_for(name, lambda: ["status", name, "Cloud-only:none"] in events())
                assert ["emblem", name, "folder-remote-symbolic"] in events()
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
                perform_action(location, "pin", "Hydrated:pinned", "starred-symbolic")
                assert daemon_status()["pinned_cache_bytes"] == 12 * 1024 * 1024
                perform_action(location, "purge", "Cloud-only:none", "folder-remote-symbolic")
                assert daemon_status()["pinned_cache_bytes"] == 0
                print("Nautilus Pin and Purge updated the daemon cache and refreshed emblems.")
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
