"""Nautilus adapter for the OneDrive mount and its local daemon socket."""

from dataclasses import dataclass
import json
import os
from pathlib import Path

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
FILE_TYPE = "standard::type"
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
    if path != mount and path.is_relative_to(mount) and ".." not in path.parts:
        return path
    return None


def read_status(path, mount, cancellable, complete):
    """Check each path component without following links, then read item xattrs."""
    relative = path.relative_to(mount)
    components = iter([*reversed(path.parents[:-1]), path])

    def query(component):
        parent = component != path
        attributes = FILE_TYPE if parent else f"{FILE_TYPE},{STATE_XATTR},{PIN_XATTR}"
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
        if component != path:
            if info.get_file_type() != Gio.FileType.DIRECTORY:
                complete(None)
                return
            query(next(components))
            return
        if info.get_file_type() not in {Gio.FileType.REGULAR, Gio.FileType.DIRECTORY}:
            complete(None)
            return
        state = info.get_attribute_string(STATE_XATTR)
        pin = info.get_attribute_string(PIN_XATTR)
        status = Status.parse(f"{state}:{pin}")
        complete(("/" + relative.as_posix(), status) if status else None)

    query(next(components))


def request(socket_path, operation, path, complete):
    # A folder Pin can wait for recursive hydration.
    client = Gio.SocketClient(enable_proxy=False, timeout=300)
    address = Gio.UnixSocketAddress.new(os.fspath(socket_path))
    payload = json.dumps({"op": operation, "path": path}, ensure_ascii=False).encode() + b"\n"
    response_bytes = bytearray()

    def connected(client, result):
        try:
            connection = client.connect_finish(result)
        except GLib.Error as error:
            complete(str(error))
            return
        connection.get_output_stream().write_all_async(
            payload, PRIORITY, None, sent, connection
        )

    def sent(stream, result, connection):
        try:
            stream.write_all_finish(result)
        except GLib.Error as error:
            finished(connection, str(error))
            return
        read(connection)

    def read(connection):
        connection.get_input_stream().read_bytes_async(
            min(4096, MAX_RESPONSE_BYTES + 1 - len(response_bytes)),
            PRIORITY, None, received, connection,
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
            error = response_error(line)
        except (GLib.Error, ValueError) as failure:
            error = str(failure)
        finished(connection, error)

    def finished(connection, error):
        connection.close_async(PRIORITY, None, None)
        complete(error)

    client.connect_async(address, None, connected)


def response_error(line):
    response = json.loads(line)
    if isinstance(response, dict):
        if response.get("ok") is True and "result" in response:
            return None
        if response.get("ok") is False and isinstance(response.get("error"), str):
            return response["error"]
    raise ValueError("Invalid daemon response")


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

    def update_file_info_full(self, provider, handle, closure, file_info):
        path = mount_candidate(file_info, self.mount)
        if path is None:
            return Nautilus.OperationResult.COMPLETE
        cancellable = Gio.Cancellable()
        self.pending[handle] = cancellable

        def complete(result):
            self.finish_update(provider, handle, closure, file_info, cancellable, result)

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
            _, status = result
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
            file_info.invalidate_extension_info()
            if error is not None:
                application = Gtk.Application.get_default()
                parent = application.get_active_window() if application else None
                dialog = Gtk.AlertDialog(message="OneDrive action failed", detail=error)
                dialog.show(parent)

        def checked(result):
            if result is None:
                finish("This item is no longer available in the OneDrive mount")
                return
            daemon_path, status = result
            allowed, tip = status.actions().get(operation, (False, "The item's status has changed"))
            if not allowed:
                finish(tip)
                return
            request(self.socket_path, operation, daemon_path, finish)

        read_status(path, self.mount, None, checked)
