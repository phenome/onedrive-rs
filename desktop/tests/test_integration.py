from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import onedrive_nautilus as extension
from gi.repository import Gio, GLib, Nautilus


class FileInfo:
    """Nautilus file metadata seam used by the production provider methods."""

    def __init__(self, location):
        self.location = location
        self.attributes = {}
        self.emblems = []
        self.invalidations = 0

    def get_uri_scheme(self):
        return self.location.get_uri_scheme()

    def get_location(self):
        return self.location

    def add_string_attribute(self, name, value):
        self.attributes[name] = value

    def get_string_attribute(self, name):
        return self.attributes.get(name)

    def add_emblem(self, name):
        self.emblems.append(name)

    def invalidate_extension_info(self):
        self.invalidations += 1
        self.attributes.clear()
        self.emblems.clear()


class ExtensionTests(unittest.TestCase):
    def setUp(self):
        temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(temp)
        self.mount = self.root / "mount"
        self.mount.mkdir()
        self.provider = extension.OneDriveExtension()
        self.provider.mount = self.mount
        self.provider.socket_path = self.root / "daemon.sock"
        self.path = self.mount / "folder" / "café #1.txt"
        self.path.parent.mkdir()
        self.path.touch()
        self.info = FileInfo(Gio.File.new_for_uri(self.path.as_uri()))
        self.complete = self.enterContext(
            patch.object(Nautilus, "info_provider_update_complete_invoke")
        )

    def set_status(self, state, pin):
        os.setxattr(self.path, "user.onedrive.state", state.encode())
        os.setxattr(self.path, "user.onedrive.pin", pin.encode())

    def drain_until(self, condition):
        deadline = time.monotonic() + 3
        context = GLib.MainContext.default()
        while not condition() and time.monotonic() < deadline:
            context.iteration(False)
            time.sleep(0.001)
        self.assertTrue(condition(), "Nautilus callback was not delivered")

    def update(self):
        self.complete.reset_mock()
        handle, closure = object(), object()
        self.assertEqual(
            self.provider.update_file_info_full(self.provider, handle, closure, self.info),
            Nautilus.OperationResult.IN_PROGRESS,
        )
        self.drain_until(lambda: self.complete.called)
        self.complete.assert_called_once_with(
            closure, self.provider, handle, Nautilus.OperationResult.COMPLETE
        )

    def test_emblems_and_menus_for_each_domain_state(self):
        cases = [
            ("Cloud-only", "none", ["folder-remote-symbolic"], {"Pin": True}),
            ("Hydrated", "none", ["object-select-symbolic"], {"Pin": True, "Purge": True}),
            ("Hydrated", "pinned", ["starred-symbolic"], {"Purge": True}),
            # An inherited or explicit Sticky Pin uses the same effective xattr.
            ("Cloud-only", "pinned", ["starred-symbolic"], {"Purge": True}),
            ("Cloud-only", "unpin_override", ["folder-remote-symbolic"], {"Pin": True}),
            ("Dirty", "none", ["software-update-available-symbolic"], {"Pin": True, "Purge": False}),
            ("Conflict", "pinned", ["dialog-warning-symbolic", "starred-symbolic"], {"Purge": False}),
        ]
        for state, pin, emblems, actions in cases:
            with self.subTest(state=state, pin=pin):
                self.info.invalidate_extension_info()
                self.set_status(state, pin)
                self.update()
                self.assertEqual(self.info.emblems, emblems)
                items = self.provider.get_file_items([self.info])
                self.assertEqual({item.props.label: item.props.sensitive for item in items}, actions)

    def test_outside_nonlocal_and_symlink_escape_skip_xattrs(self):
        sibling = self.root / "mount-other"
        sibling.mkdir()
        outside = sibling / "note.txt"
        outside.touch()
        link = self.mount / "link"
        link.symlink_to(sibling, target_is_directory=True)
        for location in (Gio.File.new_for_path(str(outside)), Gio.File.new_for_uri("sftp://host/a")):
            info = FileInfo(location)
            with patch.object(extension, "read_status") as read_status:
                self.assertEqual(
                    self.provider.update_file_info_full(self.provider, object(), object(), info),
                    Nautilus.OperationResult.COMPLETE,
                )
                self.assertEqual(self.provider.get_file_items([info]), [])
                read_status.assert_not_called()
        self.info = FileInfo(Gio.File.new_for_path(str(link / outside.name)))
        with patch.object(Gio.File, "new_for_path", wraps=Gio.File.new_for_path) as query_path:
            self.update()
            queried = [Path(args[0]) for args, _ in query_path.call_args_list]
            self.assertIn(link, queried)
            self.assertNotIn(link / outside.name, queried)
        self.assertEqual(self.info.emblems, [])
        self.assertEqual(self.provider.get_file_items([self.info]), [])

    def test_metadata_is_async_and_cancel_skips_callback(self):
        self.set_status("Hydrated", "none")
        handle = object()
        self.assertEqual(
            self.provider.update_file_info_full(self.provider, handle, object(), self.info),
            Nautilus.OperationResult.IN_PROGRESS,
        )
        self.assertFalse(self.complete.called)
        cancellable = self.provider.pending[handle]
        self.provider.cancel_update(self.provider, handle)
        self.assertTrue(cancellable.is_cancelled())
        # Wait for a subsequent real query so GLib has a chance to deliver the
        # canceled query's callback, then check that only the new one completed.
        self.update()
        self.assertFalse(self.provider.pending)

    def test_missing_or_unknown_xattrs_hide_menu(self):
        self.update()
        self.assertEqual(self.provider.get_file_items([self.info]), [])
        self.set_status("Unknown", "none")
        self.update()
        self.assertEqual(self.info.emblems, [])
        self.assertEqual(self.provider.get_file_items([self.info]), [])

    def serve(self, listener, response):
        with listener.accept()[0] as conn, conn.makefile("rb") as stream:
            conn.settimeout(3)
            line = stream.readline()
            self.assertTrue(line.endswith(b"\n"))
            conn.sendall(response)
            return json.loads(line)

    def test_menu_activation_talks_to_socket_and_refreshes_emblem(self):
        self.set_status("Cloud-only", "none")
        self.update()
        with ThreadPoolExecutor() as server, socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(self.provider.socket_path))
            listener.listen()
            listener.settimeout(3)
            for operation, next_status in (
                ("pin", ("Hydrated", "pinned")),
                ("purge", ("Cloud-only", "none")),
            ):
                with self.subTest(operation=operation):
                    def respond():
                        with listener.accept()[0] as conn, conn.makefile("rb") as stream:
                            conn.settimeout(3)
                            line = stream.readline()
                            self.assertTrue(line.endswith(b"\n"))
                            self.set_status(*next_status)
                            conn.sendall(b'{"ok":true,"result":null}\n')
                            return json.loads(line)

                    received = server.submit(respond)
                    previous = self.info.invalidations
                    [item] = self.provider.get_file_items([self.info])
                    item.emit("activate")
                    self.drain_until(lambda: self.info.invalidations > previous)
                    self.assertEqual(
                        received.result(timeout=3),
                        {"op": operation, "path": "/folder/café #1.txt"},
                    )
                    self.update()
                    expected = "starred-symbolic" if operation == "pin" else "folder-remote-symbolic"
                    self.assertEqual(self.info.emblems, [expected])

    def test_purge_failure_shows_daemon_error_and_invalidates(self):
        self.set_status("Hydrated", "none")
        self.update()
        failure = b'{"ok":false,"error":"a descendant has a pending upload"}\n'
        with ThreadPoolExecutor() as server, socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(self.provider.socket_path))
            listener.listen()
            listener.settimeout(3)
            received = server.submit(self.serve, listener, failure)
            alert = Mock()
            with patch.object(extension.Gtk, "AlertDialog", alert):
                [_, purge] = self.provider.get_file_items([self.info])
                purge.emit("activate")
                self.drain_until(lambda: alert.called)
                alert.assert_called_once_with(
                    message="OneDrive action failed", detail="a descendant has a pending upload"
                )
                self.assertEqual(self.info.invalidations, 1)
            self.assertEqual(received.result(timeout=3)["op"], "purge")


class BookmarkTests(unittest.TestCase):
    def test_bookmark_helper_preserves_existing_entries_and_is_idempotent(self):
        helper = Path(__file__).resolve().parents[1] / "onedrive-rs-bookmark"
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config"
            runtime = Path(directory) / "runtime with space"
            bookmarks = config / "gtk-3.0" / "bookmarks"
            bookmarks.parent.mkdir(parents=True)
            other = b"file:///tmp/other My other folder"
            bookmarks.write_bytes(other)
            env = dict(os.environ, XDG_CONFIG_HOME=str(config), XDG_RUNTIME_DIR=str(runtime))
            with ThreadPoolExecutor() as pool:
                list(pool.map(lambda _: subprocess.run([helper], env=env, check=True), range(8)))
            uri = (runtime / "onedrive").as_uri().encode()
            self.assertEqual(bookmarks.read_bytes(), other + b"\n" + uri + b" OneDrive\n")
            custom = other + b"\n" + uri + b" My OneDrive\n"
            bookmarks.write_bytes(custom)
            subprocess.run([helper], env=env, check=True)
            self.assertEqual(bookmarks.read_bytes(), custom)
