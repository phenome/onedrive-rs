

# Loaded only as part of the temporary copy of the real extension.
test_root = Path(os.environ["ONEDRIVE_DESKTOP_TEST"])


def record(*fields):
    with (test_root / "desktop-events").open("a") as stream:
        print(json.dumps(fields), file=stream)


native_add_emblem = Nautilus.FileInfo.add_emblem
native_finish_update = OneDriveExtension.finish_update
native_get_file_items = OneDriveExtension.get_file_items
native_request = request


def observe_emblem(file_info, emblem):
    native_add_emblem(file_info, emblem)
    record("emblem", file_info.get_name(), emblem)


def observe_finish(self, provider, handle, closure, file_info, cancellable, result):
    native_finish_update(self, provider, handle, closure, file_info, cancellable, result)
    record("status", file_info.get_name(), file_info.get_string_attribute(STATUS_ATTRIBUTE))


def observe_menu(self, files):
    items = native_get_file_items(self, files)
    if len(files) == 1:
        name = files[0].get_name()
        record("menu", name, [(item.props.name, item.props.sensitive) for item in items])
        trigger = test_root / "desktop-action"
        if trigger.exists():
            expected_name, operation = json.loads(trigger.read_text())
            if name == expected_name:
                for item in items:
                    if item.props.name == f"OneDrive::{operation}" and item.props.sensitive:
                        trigger.unlink()
                        # Exercise the real MenuItem signal and its socket callback.
                        GLib.idle_add(lambda: item.emit("activate") or GLib.SOURCE_REMOVE)
                        break
    return items


def observe_request(socket_path, operation, path, complete):
    def observed_reply(error):
        record("reply", operation, path, error)
        complete(error)

    native_request(socket_path, operation, path, observed_reply)


request = observe_request
Nautilus.FileInfo.add_emblem = observe_emblem
OneDriveExtension.finish_update = observe_finish
OneDriveExtension.get_file_items = observe_menu
