#!/bin/sh
set -eu
gnome-keyring-daemon --foreground --unlock --components=secrets \
    --control-directory="$XDG_RUNTIME_DIR/keyring" <<'PASSWORD' &
synthetic-keyring-password
PASSWORD
keyring=$!
trap 'kill "$keyring" 2>/dev/null || true; wait "$keyring" 2>/dev/null || true' EXIT
# Wait for the foreground daemon itself before sending Secret Service requests.
attempt=0
until busctl --user --no-pager list --acquired | grep -q '^org.freedesktop.secrets '; do
    attempt=$((attempt + 1))
    if [ "$attempt" -eq 100 ]; then
        exit 1
    fi
    sleep .05
done
"$@"
