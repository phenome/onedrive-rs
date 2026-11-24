use std::fs::{self, Permissions};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use onedrive_graph::{SecretServiceStore, TokenStore};
use serde_json::{Value, json};

#[test]
#[ignore = "requires FUSE3 and an isolated D-Bus session with GNOME Keyring"]
fn cli_auth_in_an_isolated_secret_service_session() {
    let root = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let root = root.path();
    let config = root.join("config/onedrive-rs");
    fs::create_dir_all(&config).unwrap();
    fs::write(
        config.join("config.toml"),
        "client_id = 'synthetic-cli-client'\n",
    )
    .unwrap();
    let bin = root.join("bin");
    fs::create_dir(&bin).unwrap();
    let opener = bin.join("xdg-open");
    fs::write(&opener, include_bytes!("fixtures/xdg-open")).unwrap();
    fs::set_permissions(opener, Permissions::from_mode(0o700)).unwrap();

    let output = Command::new("timeout")
        .args(["20", "dbus-run-session", "--", "sh"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/isolated-keyring.sh"))
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "auth_session_fixture", "--nocapture"])
        .env("ONEDRIVE_TEST_AUTH_FIXTURE", root)
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_DATA_HOME", root.join("data"))
        .env("XDG_RUNTIME_DIR", root)
        .env_remove("GNOME_KEYRING_CONTROL")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn auth_session_fixture() {
    let Some(root) = std::env::var_os("ONEDRIVE_TEST_AUTH_FIXTURE") else {
        return;
    };
    let root = Path::new(&root);
    let path = std::env::join_paths(
        [root.join("bin")]
            .into_iter()
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();
    let cli = |operation: &str| {
        Command::new(env!("CARGO_BIN_EXE_onedrive-rs"))
            .args(["--json", "auth", operation])
            .env("PATH", &path)
            .output()
            .unwrap()
    };

    assert_signed_in(cli("status"), false);
    assert_signed_out_mount();
    let login = cli("login");
    assert_eq!(login.status.code(), Some(1), "{login:?}");
    assert_eq!(
        reply(&login),
        json!({"ok":false,"error":"OAuth authorization was denied"})
    );
    assert_signed_in(cli("status"), false);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let selected = SecretServiceStore::new("synthetic-cli-client");
    let other = SecretServiceStore::new("synthetic-other-client");
    runtime.block_on(async {
        selected
            .replace_refresh_token("synthetic-selected-refresh")
            .await
            .unwrap();
        other
            .replace_refresh_token("synthetic-other-refresh")
            .await
            .unwrap();
    });
    assert_signed_in(cli("status"), true);

    // Hold the same transaction a daemon uses during refresh, then publish a
    // rotation. Logout must wait and delete that rotated credential too.
    let transaction = runtime
        .block_on(selected.begin_transaction())
        .unwrap()
        .unwrap();
    let mut monitor = Command::new("stdbuf")
        .args([
            "-oL",
            "dbus-monitor",
            "--session",
            "type='method_call',interface='org.freedesktop.DBus',member='RequestName'",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut logout = Command::new(env!("CARGO_BIN_EXE_onedrive-rs"))
        .args(["--json", "auth", "logout"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    // Only logout is starting a credential transaction on this private bus.
    // Its second RequestName proves it had to retry while we held the lock.
    let requests = BufReader::new(monitor.stdout.take().unwrap())
        .lines()
        .map(Result::unwrap)
        .filter(|line| {
            line.trim_start()
                .starts_with("string \"org.onedrive_rs.Auth.Client_")
        })
        .take(2)
        .count();
    assert_eq!(requests, 2);
    monitor.kill().unwrap();
    monitor.wait().unwrap();
    assert!(logout.try_wait().unwrap().is_none());
    runtime.block_on(async {
        transaction
            .replace_refresh_token("synthetic-rotated-refresh")
            .await
            .unwrap();
        drop(transaction);
    });
    assert_signed_in(logout.wait_with_output().unwrap(), false);
    assert_signed_in(cli("status"), false);
    assert_signed_in(cli("logout"), false);
    runtime.block_on(async {
        assert!(selected.load_refresh_token().await.unwrap().is_none());
        assert_eq!(
            other.load_refresh_token().await.unwrap().as_deref(),
            Some("synthetic-other-refresh")
        );
    });
}

fn assert_signed_out_mount() {
    let output = Command::new("bash")
        .args([
            "-euc",
            r#"
mount="$XDG_RUNTIME_DIR/onedrive"
timeout --preserve-status 10 "$1" daemon &
daemon_pid=$!
cleanup() {
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
    timeout 2 fusermount3 -u "$mount" 2>/dev/null || true
}
trap cleanup EXIT
for attempt in {1..80}; do
    if timeout 1 mountpoint -q "$mount"; then break; fi
    sleep .05
done
timeout 1 mountpoint -q "$mount"
entries=$(timeout 2 ls -A "$mount")
test -z "$entries"
timeout 2 "$1" --json status
kill -TERM "$daemon_pid"
wait "$daemon_pid"
awk -v mount="$mount" '$5 == mount { still_mounted = 1 } END { exit still_mounted }' /proc/self/mountinfo
"#,
            "fixture",
            env!("CARGO_BIN_EXE_onedrive-rs"),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_signed_in(output: Output, signed_in: bool) {
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        reply(&output),
        json!({"ok":true,"result":{"signed_in":signed_in}})
    );
}

fn reply(output: &Output) -> Value {
    assert!(output.stderr.is_empty(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}
