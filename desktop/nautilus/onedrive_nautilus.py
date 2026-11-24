"""Nautilus adapter for the OneDrive mount and its local daemon socket."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
from weakref import ref

import gi

gi.require_version("Nautilus", "4.1")
gi.require_version("Gtk", "4.0")
from gi.repository import Gio, GLib, GObject, Gtk, Nautilus

STATUS_ATTRIBUTE = "onedrive-rs::status"
STATE_EMBLEMS = {
    "Cloud-only": "folder-remote-symbolic",
    "Hydrated": "object-select-symbolic",
    "Dirty": "software-update-available-symbolic",
    "Conflict": "dialog-warning-symbolic",
}
PIN_EMBLEM = "starred-symbolic"
PIN_VALUES = {"none", "pinned", "unpin_override"}
MAX_RESPONSE_BYTES = 64 * 1024
FILE_IDENTITY = "standard::type,unix::inode"
OBSERVATION_INTERVAL_MS = 750
MAX_STATUS_CHECKS = 8
STATE_XATTR = "xattr::onedrive.state"
PIN_XATTR = "xattr::onedrive.pin"
PRIORITY = GLib.PRIORITY_DEFAULT


@dataclass(frozen=True)
class Status:
    state: str
    pin: str

    @classmethod
    def parse(cls, value):
        if value:
            state, _, pin = value.partition(":")
            if state in STATE_EMBLEMS and pin in PIN_VALUES:
                return cls(state, pin)
        return None

    def emblems(self):
        if self.pin == "pinned" and self.state in {"Cloud-only", "Hydrated"}:
            return (PIN_EMBLEM,)
        if self.pin == "pinned":
            return (STATE_EMBLEMS[self.state], PIN_EMBLEM)
        return (STATE_EMBLEMS[self.state],)

    def actions(self):
        actions = {}
        if self.pin != "pinned":
            actions["pin"] = (True, "Hydrate and pin this OneDrive item")
        if self.state == "Dirty":
            actions["purge"] = (False, "Purge is unavailable while changes await upload")
        elif self.state == "Conflict":
            actions["purge"] = (False, "Purge is unavailable while there is a conflict")
        elif self.state == "Hydrated" or self.pin == "pinned":
            actions["purge"] = (True, "Purge cached content and clear this item's pin")
        return actions


def mount_candidate(file_info, mount):
    if mount is None or file_info.get_uri_scheme() != "file":
        return None
    location = file_info.get_location().get_path()
    if location is None:
        return None
    path = Path(location)
    # This cheap check avoids any filesystem lookup for unrelated Nautilus items.
    if path.is_relative_to(mount) and ".." not in path.parts:
        return path
    return None


def read_status(path, mount, cancellable, complete):
    """Check each path component without following links, then read item xattrs."""
    relative = path.relative_to(mount)
    components = iter(relative.parts)
    inodes = []

    def query(component):
        parent = component != path
        attributes = FILE_IDENTITY if parent else f"{FILE_IDENTITY},{STATE_XATTR},{PIN_XATTR}"
        Gio.File.new_for_path(os.fspath(component)).query_info_async(
            attributes, Gio.FileQueryInfoFlags.NOFOLLOW_SYMLINKS, PRIORITY,
            cancellable, finish_query, component,
        )

    def finish_query(file, result, component):
        try:
            info = file.query_info_finish(result)
        except GLib.Error:
            complete(None)
            return
        inodes.append(info.get_attribute_uint64("unix::inode"))
        if component != path:
            if info.get_file_type() != Gio.FileType.DIRECTORY:
                complete(None)
                return
            query(component / next(components))
            return
        if info.get_file_type() not in {Gio.FileType.REGULAR, Gio.FileType.DIRECTORY}:
            complete(None)
            return
        state = info.get_attribute_string(STATE_XATTR)
        pin = info.get_attribute_string(PIN_XATTR)
        status = Status.parse(f"{state}:{pin}")
        complete(("/" + "/".join(relative.parts), status, tuple(inodes)) if status else None)

    query(mount)


def request(socket_path, operation, mount, path, complete):
    # A folder Pin can wait for recursive hydration.
    exchange(socket_path, {"op": operation, "mount": os.fspath(mount), "path": path}, 300, None,
             lambda _result, error: complete(error))


def exchange(socket_path, message, timeout, cancellable, complete):
    client = Gio.SocketClient(enable_proxy=False, timeout=timeout)
    address = Gio.UnixSocketAddress.new(os.fspath(socket_path))
    try:
        payload = json.dumps(message, ensure_ascii=False).encode() + b"\n"
    except UnicodeEncodeError:
        complete(None, "The OneDrive path must be valid UTF-8")
        return
    response_bytes = bytearray()

    def connected(client, result):
        try:
            connection = client.connect_finish(result)
        except GLib.Error as error:
            complete(None, str(error))
            return
        connection.get_output_stream().write_all_async(
            payload, PRIORITY, cancellable, sent, connection
        )

    def sent(stream, result, connection):
        try:
            stream.write_all_finish(result)
        except GLib.Error as error:
            finished(connection, None, str(error))
            return
        read(connection)

    def read(connection):
        connection.get_input_stream().read_bytes_async(
            min(4096, MAX_RESPONSE_BYTES + 1 - len(response_bytes)),
            PRIORITY, cancellable, received, connection,
        )

    def received(stream, result, connection):
        try:
            chunk = stream.read_bytes_finish(result).get_data()
            response_bytes.extend(chunk)
            line, separator, _ = response_bytes.partition(b"\n")
            if not chunk or len(line) >= MAX_RESPONSE_BYTES:
                raise ValueError("Incomplete or oversized daemon response")
            if not separator:
                read(connection)
                return
            value, error = response_result(line)
        except (GLib.Error, ValueError) as failure:
            value, error = None, str(failure)
        finished(connection, value, error)

    def finished(connection, value, error):
        connection.close_async(PRIORITY, None, None)
        complete(value, error)

    client.connect_async(address, cancellable, connected)


def response_result(line):
    response = json.loads(line)
    if isinstance(response, dict):
        if response.get("ok") is True and "result" in response:
            return response["result"], None
        if response.get("ok") is False and isinstance(response.get("error"), str):
            return None, response["error"]
    raise ValueError("Invalid daemon response")


@dataclass
class ObservedFile:
    file_ref: object
    status: Status | None
    inodes: tuple[int, ...]


class OneDriveExtension(GObject.Object, Nautilus.InfoProvider, Nautilus.MenuProvider):
    def __init__(self):
        super().__init__()
        runtime = Path(GLib.get_user_runtime_dir())
        mount = Path(os.environ.get("ONEDRIVE_RS_MOUNT", runtime / "onedrive"))
        self.mount = None
        if mount.is_absolute() and mount != Path("/") and ".." not in mount.parts:
            self.mount = mount
        self.socket_path = runtime / "onedrive-rs" / "daemon.sock"
        self.pending = {}
        self.active_paths = set()
        self.observed_files = {}
        self.observation = None
        self.observation_timer = 0
        self.cursor = (None, None)
        self.status_checks = {}
        self.checking = set()

    def observe(self, file_info, status, inodes):
        key = hash(file_info)
        previous = self.observed_files.get(key)
        if previous is not None:
            previous.status = status
            previous.inodes = inodes
            return
        provider_ref = ref(self)

        def forget():
            provider = provider_ref()
            if provider is not None:
                provider.observed_files.pop(key, None)
                provider.status_checks.pop(key, None)
                if not provider.observed_files:
                    provider.stop_observing()

        self.observed_files[key] = ObservedFile(file_info.weak_ref(forget), status, inodes)
        if self.observation is None:
            self.observation = Gio.Cancellable()
            self.schedule_observation()

    def stop_observing(self):
        if self.observation_timer:
            GLib.source_remove(self.observation_timer)
            self.observation_timer = 0
        self.observation.cancel()
        self.observation = None
        self.cursor = (None, None)
        self.status_checks.clear()
        self.checking.clear()

    def schedule_observation(self, delay=OBSERVATION_INTERVAL_MS):
        if self.observation is None:
            return
        provider_ref = ref(self)

        def poll():
            provider = provider_ref()
            if provider is not None:
                provider.observation_timer = 0
                provider.poll_changes()
            return GLib.SOURCE_REMOVE

        self.observation_timer = GLib.timeout_add(delay, poll)

    def poll_changes(self):
        provider_ref = ref(self)
        session = self.observation
        epoch, since = self.cursor

        def received(result, error):
            provider = provider_ref()
            if provider is None or provider.observation is not session:
                return
            if error is None and isinstance(result, dict):
                provider.accept_changes(result)
                provider.schedule_observation()
            else:
                provider.schedule_observation(delay=3000)

        exchange(self.socket_path, {
            "op": "desktop_changes", "mount": os.fspath(self.mount),
            "epoch": epoch, "since": since,
        }, 2, session, received)

    def accept_changes(self, result):
        epoch, revision, changes = result.get("epoch"), result.get("revision"), result.get("inodes")
        if not isinstance(epoch, str) or type(revision) is not int:
            return
        if changes is not None and (
            not isinstance(changes, list) or any(
                not isinstance(change, list) or len(change) != 2
                or type(change[0]) is not int or type(change[1]) is not bool
                for change in changes
            )
        ):
            return
        cursor = (epoch, revision)
        if cursor == self.cursor:
            return
        self.cursor = cursor
        direct = set()
        subtrees = set()
        if changes is not None:
            for inode, subtree in changes:
                direct.add(inode)
                if subtree:
                    subtrees.add(inode)
        for key, observed in self.observed_files.items():
            if (changes is None or observed.inodes[-1] in direct
                    or subtrees.intersection(observed.inodes)):
                self.status_checks[key] = None
        self.check_status_changes()

    def check_status_changes(self):
        while self.status_checks and len(self.checking) < MAX_STATUS_CHECKS:
            key = next((key for key in self.status_checks if key not in self.checking), None)
            if key is None:
                return
            del self.status_checks[key]
            observed = self.observed_files.get(key)
            file_info = observed.file_ref() if observed is not None else None
            path = mount_candidate(file_info, self.mount) if file_info is not None else None
            if path is not None:
                self.check_status_change(key, path)

    def check_status_change(self, key, path):
        self.checking.add(key)
        checked = self.observed_files[key]
        provider_ref = ref(self)
        session = self.observation

        def complete(result):
            provider = provider_ref()
            if provider is None or provider.observation is not session:
                return
            provider.checking.discard(key)
            observed = provider.observed_files.get(key)
            file_info = observed.file_ref() if observed is not None else None
            if observed is checked and file_info is not None and (
                mount_candidate(file_info, provider.mount) == path
            ):
                status = result[1] if result else None
                if result:
                    observed.inodes = result[2]
                if status != observed.status:
                    observed.status = status
                    file_info.invalidate_extension_info()
            provider.check_status_changes()

        read_status(path, self.mount, session, complete)

    def invalidate_affected(self, path, selected):
        affected = {selected}
        for observation in tuple(self.observed_files.values()):
            file_info = observation.file_ref()
            if file_info is None:
                continue
            observed = mount_candidate(file_info, self.mount)
            # Descendants can inherit retention changes. Ancestors can show
            # aggregate state changes even when they were outside the selection.
            if observed is not None and (
                observed.is_relative_to(path) or path.is_relative_to(observed)
            ):
                affected.add(file_info)
        for file_info in affected:
            file_info.invalidate_extension_info()

    def update_file_info_full(self, provider, handle, closure, file_info):
        path = mount_candidate(file_info, self.mount)
        if path is None:
            return Nautilus.OperationResult.COMPLETE
        cursor = self.cursor
        cancellable = Gio.Cancellable()
        self.pending[handle] = cancellable

        def complete(result):
            self.finish_update(provider, handle, closure, file_info, cancellable, result)
            if result and cursor != self.cursor and hash(file_info) in self.observed_files:
                self.status_checks[hash(file_info)] = None
                self.check_status_changes()

        read_status(path, self.mount, cancellable, complete)
        return Nautilus.OperationResult.IN_PROGRESS

    def cancel_update(self, provider, handle):
        cancellable = self.pending.pop(handle, None)
        if cancellable:
            cancellable.cancel()

    def finish_update(self, provider, handle, closure, file_info, cancellable, result):
        if self.pending.get(handle) is not cancellable:
            return
        del self.pending[handle]
        if result:
            _, status, inodes = result
            self.observe(file_info, status, inodes)
            file_info.add_string_attribute(STATUS_ATTRIBUTE, f"{status.state}:{status.pin}")
            for emblem in status.emblems():
                file_info.add_emblem(emblem)
        Nautilus.info_provider_update_complete_invoke(
            closure, provider, handle, Nautilus.OperationResult.COMPLETE
        )

    def get_file_items(self, files):
        if len(files) != 1:
            return []
        file_info = files[0]
        path = mount_candidate(file_info, self.mount)
        if path is None:
            return []
        status = Status.parse(file_info.get_string_attribute(STATUS_ATTRIBUTE))
        if status is None:
            return []

        items = []
        for operation, (sensitive, tip) in status.actions().items():
            item = Nautilus.MenuItem(
                name=f"OneDrive::{operation}",
                label=operation.capitalize(),
                tip=tip,
                sensitive=sensitive,
            )
            item.connect("activate", self.activate, operation, path, file_info)
            items.append(item)
        return items

    def activate(self, item, operation, path, file_info):
        if path in self.active_paths:
            return
        self.active_paths.add(path)

        def finish(error):
            self.active_paths.remove(path)
            if error is None:
                self.invalidate_affected(path, file_info)
                return
            file_info.invalidate_extension_info()
            application = Gtk.Application.get_default()
            parent = application.get_active_window() if application else None
            dialog = Gtk.AlertDialog(message="OneDrive action failed", detail=error)
            dialog.show(parent)

        def checked(result):
            if result is None:
                finish("This item is no longer available in the OneDrive mount")
                return
            daemon_path, status, _ = result
            allowed, tip = status.actions().get(operation, (False, "The item's status has changed"))
            if not allowed:
                finish(tip)
                return
            request(self.socket_path, operation, self.mount, daemon_path, finish)

        read_status(path, self.mount, None, checked)
