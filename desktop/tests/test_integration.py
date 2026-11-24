from concurrent.futures import ThreadPoolExecutor
import gc
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
from gi.repository import Gio, GLib, GObject, Nautilus


class FileInfo(GObject.Object, Nautilus.FileInfo):
    """Nautilus file metadata seam used by the production provider methods."""

    def __init__(self, location):
        super().__init__()
        self.location = location
        self.attributes = {}
        self.emblems = []
        self.invalidations = 0

    def do_get_uri_scheme(self):
        return self.location.get_uri_scheme()

    def do_get_location(self):
        return self.location

    def do_add_string_attribute(self, name, value):
        self.attributes[name] = value

    def do_get_string_attribute(self, name):
        return self.attributes.get(name)

    def do_add_emblem(self, name):
        self.emblems.append(name)

    def do_invalidate_extension_info(self):
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

    def set_status(self, state, pin, path=None):
        path = self.path if path is None else path
        os.setxattr(path, "user.onedrive.state", state.encode())
        os.setxattr(path, "user.onedrive.pin", pin.encode())

    def drain_until(self, condition):
        deadline = time.monotonic() + 3
        context = GLib.MainContext.default()
        while not condition() and time.monotonic() < deadline:
            context.iteration(False)
            time.sleep(0.001)
        self.assertTrue(condition(), "Nautilus callback was not delivered")

    def update(self, info=None):
        info = self.info if info is None else info
        self.complete.reset_mock()
        handle, closure = object(), object()
        self.assertEqual(
            self.provider.update_file_info_full(self.provider, handle, closure, info),
            Nautilus.OperationResult.IN_PROGRESS,
        )
        self.drain_until(lambda: self.complete.called)
        self.complete.assert_called_once_with(
            closure, self.provider, handle, Nautilus.OperationResult.COMPLETE
        )

    def record_queries(self):
        paths = []
        query_info_async = Gio.File.query_info_async

        def record(file, *args):
            paths.append(Path(file.get_path()))
            return query_info_async(file, *args)

        return patch.object(Gio.File, "query_info_async", record), paths

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
        record, queried = self.record_queries()
        with record:
            self.update()
        self.assertEqual(queried, [self.mount, link])
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

    def serve(self, listener, response, statuses=()):
        with listener.accept()[0] as conn, conn.makefile("rb") as stream:
            conn.settimeout(3)
            line = stream.readline()
            self.assertTrue(line.endswith(b"\n"))
            for status in statuses:
                self.set_status(*status)
            conn.sendall(response)
            return json.loads(line)

    def test_menu_activation_talks_to_socket_and_refreshes_emblem(self):
        self.set_status("Cloud-only", "none")
        record, queried = self.record_queries()
        with record:
            self.update()
        self.assertEqual(queried, [self.mount, self.path.parent, self.path])
        with ThreadPoolExecutor() as server, socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(self.provider.socket_path))
            listener.listen()
            listener.settimeout(3)
            for operation, next_status in (
                ("pin", ("Hydrated", "pinned")),
                ("purge", ("Cloud-only", "none")),
            ):
                with self.subTest(operation=operation):
                    received = server.submit(
                        self.serve, listener, b'{"ok":true,"result":null}\n', [next_status]
                    )
                    previous = self.info.invalidations
                    [item] = self.provider.get_file_items([self.info])
                    record, queried = self.record_queries()
                    with record:
                        item.emit("activate")
                        self.drain_until(lambda: self.info.invalidations > previous)
                    self.assertEqual(queried, [self.mount, self.path.parent, self.path])
                    self.assertEqual(
                        received.result(timeout=3),
                        {"op": operation, "path": "/folder/café #1.txt"},
                    )
                    self.update()
                    expected = "starred-symbolic" if operation == "pin" else "folder-remote-symbolic"
                    self.assertEqual(self.info.emblems, [expected])

    def test_sticky_actions_refresh_observed_subtree_and_ancestors(self):
        ancestor = self.path.parent
        selected = ancestor / "selected"
        selected.mkdir()
        left = selected / "left"
        right = selected / "right"
        left.mkdir()
        right.mkdir()
        first = left / "first.txt"
        second = right / "second.txt"
        first.touch()
        second.touch()
        affected_paths = [self.mount, ancestor, selected, left, right, first, second]
        sibling = self.path
        similar_name = ancestor / "selected-other"
        similar_name.touch()
        observed = {}
        for path in [*affected_paths, sibling, similar_name]:
            self.set_status("Cloud-only", "none", path)
            # Separate Nautilus FileInfo objects for the same path model two windows.
            observed[path] = [FileInfo(Gio.File.new_for_path(str(path))) for _ in range(2)]
            for info in observed[path]:
                self.update(info)

        with ThreadPoolExecutor() as server, socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(self.provider.socket_path))
            listener.listen()
            listener.settimeout(3)
            for count, operation in enumerate(("pin", "purge"), start=1):
                pinned = operation == "pin"
                state = "Hydrated" if pinned else "Cloud-only"
                statuses = [
                    (state, "pinned" if pinned and path.is_relative_to(selected) else "none", path)
                    for path in affected_paths
                ]
                received = server.submit(self.serve, listener, b'{"ok":true,"result":null}\n', statuses)
                [item] = self.provider.get_file_items([observed[selected][0]])
                self.assertEqual(item.props.label, operation.capitalize())
                record, queried = self.record_queries()
                with record:
                    item.emit("activate")
                    self.drain_until(lambda: not self.provider.active_paths)
                self.assertEqual(queried, [self.mount, ancestor, selected])
                self.assertEqual(received.result(timeout=3), {"op": operation, "path": "/folder/selected"})
                emblems = {
                    ("Cloud-only", "none"): "folder-remote-symbolic",
                    ("Hydrated", "none"): "object-select-symbolic",
                    ("Hydrated", "pinned"): "starred-symbolic",
                }
                expected = {path: emblems[state, pin] for state, pin, path in statuses}
                for path, pair in observed.items():
                    for info in pair:
                        with self.subTest(operation=operation, path=path):
                            if path in expected:
                                self.assertEqual(info.invalidations, count)
                                self.update(info)
                                self.assertEqual(info.emblems, [expected[path]])
                            else:
                                self.assertEqual(info.invalidations, 0)
                                self.assertEqual(info.emblems, ["folder-remote-symbolic"])

    def test_mount_root_status_and_whole_drive_actions(self):
        root_info = FileInfo(Gio.File.new_for_path(str(self.mount)))
        self.set_status("Cloud-only", "none", self.mount)
        self.set_status("Cloud-only", "none")
        self.update()
        record, queried = self.record_queries()
        with record:
            self.update(root_info)
        self.assertEqual(queried, [self.mount])
        self.assertEqual(root_info.emblems, ["folder-remote-symbolic"])
        with ThreadPoolExecutor() as server, socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(self.provider.socket_path))
            listener.listen()
            listener.settimeout(3)
            for count, (operation, status, emblem) in enumerate((
                ("pin", ("Hydrated", "pinned"), "starred-symbolic"),
                ("purge", ("Cloud-only", "none"), "folder-remote-symbolic"),
            ), start=1):
                received = server.submit(
                    self.serve, listener, b'{"ok":true,"result":null}\n',
                    [(*status, self.mount), (*status, self.path)],
                )
                [item] = self.provider.get_file_items([root_info])
                self.assertEqual(item.props.label, operation.capitalize())
                record, queried = self.record_queries()
                with record:
                    item.emit("activate")
                    self.drain_until(lambda: not self.provider.active_paths)
                self.assertEqual(queried, [self.mount])
                self.assertEqual(received.result(timeout=3), {"op": operation, "path": "/"})
                for info in (root_info, self.info):
                    self.assertEqual(info.invalidations, count)
                    self.update(info)
                    self.assertEqual(info.emblems, [emblem])

    def test_observation_does_not_keep_file_info_alive(self):
        info = FileInfo(self.info.location)
        key = hash(info)
        file_ref = info.weak_ref()
        self.set_status("Hydrated", "none")
        self.update(info)
        self.assertIn(key, self.provider.observed_files)
        del self.info
        observation = self.provider.observation
        del info
        gc.collect()
        self.assertIsNone(file_ref())
        self.assertNotIn(key, self.provider.observed_files)
        self.assertTrue(observation.is_cancelled())
        self.assertEqual(self.provider.observation_timer, 0)

    def test_external_changes_recheck_only_affected_items_and_refresh_once(self):
        unrelated = self.mount / "other.txt"
        unrelated.touch()
        unrelated_info = FileInfo(Gio.File.new_for_path(str(unrelated)))
        self.set_status("Cloud-only", "none", unrelated)
        self.set_status("Cloud-only", "none")
        self.update(unrelated_info)
        self.update()
        ancestor = self.path.parent.stat().st_ino
        self.set_status("Hydrated", "pinned")
        record, queried = self.record_queries()
        with record:
            for revision in (1, 2):
                self.provider.accept_changes({
                    "epoch": "daemon", "revision": revision, "inodes": [[ancestor, True]],
                })
                self.drain_until(lambda: not self.provider.checking)
            self.provider.accept_changes({"epoch": "daemon", "revision": 2, "inodes": []})
        self.assertEqual(queried, [self.mount, self.path.parent, self.path] * 2)
        self.assertEqual(self.info.invalidations, 1)
        self.assertEqual(unrelated_info.invalidations, 0)
        self.update()
        self.assertEqual(self.info.emblems, ["starred-symbolic"])
        self.assertEqual([item.props.label for item in self.provider.get_file_items([self.info])],
                         ["Purge"])

    def test_activation_stops_at_replaced_symlink_component(self):
        self.set_status("Cloud-only", "none")
        self.update()
        [pin] = self.provider.get_file_items([self.info])
        outside = self.root / "outside"
        self.path.parent.rename(outside)
        self.path.parent.symlink_to(outside, target_is_directory=True)
        record, queried = self.record_queries()
        with record, patch.object(extension, "request") as send, patch.object(extension.Gtk, "AlertDialog") as alert:
            pin.emit("activate")
            self.drain_until(lambda: alert.called)
            send.assert_not_called()
        self.assertEqual(queried, [self.mount, self.path.parent])
        self.assertFalse(self.provider.active_paths)

    def test_purge_rechecks_dirty_state_before_sending(self):
        self.set_status("Hydrated", "none")
        self.update()
        [_, purge] = self.provider.get_file_items([self.info])
        self.set_status("Dirty", "none")
        with patch.object(extension, "request") as send, patch.object(extension.Gtk, "AlertDialog") as alert:
            purge.emit("activate")
            self.drain_until(lambda: alert.called)
            self.assertIn("await upload", alert.call_args.kwargs["detail"])
            send.assert_not_called()
            self.assertFalse(self.provider.active_paths)

    def test_action_on_non_utf8_name_reports_error_on_every_attempt(self):
        self.path = self.mount / os.fsdecode(b"\xff")
        self.path.touch()
        self.info = FileInfo(Gio.File.new_for_uri(self.path.as_uri()))
        self.set_status("Cloud-only", "none")
        with patch.object(extension.Gtk, "AlertDialog") as alert:
            for attempt in (1, 2):
                self.update()
                [pin] = self.provider.get_file_items([self.info])
                pin.emit("activate")
                self.drain_until(lambda: alert.call_count == attempt)
                self.assertIn("UTF-8", alert.call_args.kwargs["detail"])
                self.assertFalse(self.provider.active_paths)

    def test_purge_failure_shows_daemon_error_and_invalidates(self):
        descendant = self.info
        self.set_status("Dirty", "none")
        self.update(descendant)
        self.path = self.path.parent
        self.info = FileInfo(Gio.File.new_for_path(str(self.path)))
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
                self.assertEqual(descendant.invalidations, 0)
                self.assertFalse(self.provider.active_paths)
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
