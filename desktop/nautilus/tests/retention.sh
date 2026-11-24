#!/usr/bin/env bash
# Pass the compiled daemon fixture test binary and CLI binary.
set -euo pipefail

test_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
worker=$(realpath -- "$1")
export ONEDRIVE_DESKTOP_CLI=$(realpath -- "$2")
root=$(mktemp -d -p /tmp onedrive-nautilus-XXXXXX)
export ONEDRIVE_DESKTOP_TEST="$root"
export HOME="$root/home" XDG_CONFIG_HOME="$root/config" XDG_CACHE_HOME="$root/cache"
export XDG_DATA_HOME="$root/data" XDG_STATE_HOME="$root/state" XDG_RUNTIME_DIR="$root/runtime"
export GIO_USE_VFS=local
unset DBUS_SESSION_BUS_ADDRESS DBUS_STARTER_ADDRESS DBUS_STARTER_BUS_TYPE WAYLAND_DISPLAY DISPLAY
mount="$XDG_RUNTIME_DIR/onedrive"
socket="$XDG_RUNTIME_DIR/onedrive-rs/daemon.sock"

mounted() {
    awk -v path="$mount" '$5 == path { found=1 } END { exit !found }' /proc/self/mountinfo
}

cleanup() {
    local status=$?
    trap - EXIT
    if [[ -n ${pid:-} ]]; then
        kill -TERM "$pid" 2>/dev/null || :
        for _ in {1..50}; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid"
            status=1
        fi
        wait "$pid" || status=1
        [[ ! -e $socket ]] || status=1
    fi
    if mounted; then
        timeout -k 2s 5s fusermount3 -u "$mount" || :
        status=1
    fi
    if mounted; then
        printf 'Fixture mount still active at %s\n' "$mount" >&2
        exit 1
    fi
    if ((status != 0)) && [[ -f $root/daemon.log ]]; then
        cat "$root/daemon.log" >&2
    fi
    timeout -k 2s 5s rm -rf -- "$root" || status=1
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -m 700 "$HOME" "$XDG_RUNTIME_DIR"
magick -size 800x400 xc:'#0064c8' PNG24:"$root/fixture.png"
"$worker" --exact desktop_fixture_worker --nocapture >"$root/daemon.log" 2>&1 &
pid=$!
for _ in {1..50}; do
    if mounted && [[ -S $socket ]]; then break; fi
    kill -0 "$pid"
    sleep 0.1
done
mounted
bash "$test_dir/desktop.sh" "$root" "$mount" retention
