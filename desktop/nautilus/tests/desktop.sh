#!/usr/bin/env bash
# Run against a synthetic FUSE mount with an isolated thumbnail cache.
set -euo pipefail

test_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$1
mount=$2
mode=${3:-preview}
[[ $root == /tmp/* && $mount == "$root/"* && $mode =~ ^(preview|retention)$ ]]
[[ ${XDG_CACHE_HOME:?} == "$root/"* ]]
mkdir -p "$root"/{home,config,data/nautilus-python/extensions}
extension="$root/data/nautilus-python/extensions/onedrive_nautilus.py"
cat "$test_dir/../onedrive_nautilus.py" "$test_dir/observe.py" > "$extension"

cat >"$root/desktop-bus.conf" <<'XML'
<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-Bus Bus Configuration 1.0//EN" "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig>
  <type>session</type>
  <listen>unix:tmpdir=/tmp</listen>
  <policy context="default">
    <allow send_destination="*"/>
    <allow receive_sender="*"/>
    <allow own="*"/>
  </policy>
</busconfig>
XML

export HOME="$root/home" XDG_CONFIG_HOME="$root/config" XDG_DATA_HOME="$root/data"
export XDG_STATE_HOME="$root/state" XDG_DATA_DIRS=/usr/share
export XDG_CONFIG_DIRS="$root/config" XDG_SESSION_TYPE=x11 GTK_A11Y=none
export GSETTINGS_SCHEMA_DIR=/usr/share/glib-2.0/schemas GSETTINGS_BACKEND=memory
export GIO_USE_VFS=local GDK_BACKEND=x11 GSK_RENDERER=cairo
export MESA_SHADER_CACHE_DISABLE=true
export ONEDRIVE_RS_MOUNT="$mount" ONEDRIVE_DESKTOP_TEST="$root"
if [[ $mode == retention ]]; then
    desktop_deadline=40s
    [[ ${XDG_RUNTIME_DIR:?} == "$root/"* && -S $XDG_RUNTIME_DIR/onedrive-rs/daemon.sock ]]
else
    desktop_deadline=25s
    export XDG_RUNTIME_DIR="$root/desktop-runtime"
    mkdir -m 700 "$XDG_RUNTIME_DIR"
fi
unset DBUS_SESSION_BUS_ADDRESS DBUS_STARTER_ADDRESS DBUS_STARTER_BUS_TYPE WAYLAND_DISPLAY
ulimit -c 0
timeout -k 2s "$desktop_deadline" dbus-run-session --config-file="$root/desktop-bus.conf" -- \
    xvfb-run -a -s '-screen 0 1024x768x24' python "$test_dir/drive.py" "$mode"
