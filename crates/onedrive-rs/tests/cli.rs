use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use onedrive_rs::ipc::{IpcServer, Request, socket_path};
use serde_json::{Value, json};

fn cli(runtime: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_onedrive-rs"))
        .current_dir(runtime)
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_CONFIG_HOME", runtime.join("config"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn status_renders_the_same_daemon_data_as_human_text_and_json() {
    let runtime = private_runtime();
    let server = IpcServer::bind(&socket_path(runtime.path())).unwrap();
    let status = json!({
        "sync_state": "idle",
        "unpinned_cache_bytes": 12,
        "pinned_cache_bytes": 45,
        "free_disk_floor_warning": true,
        "pending_upload_count": 1,
        "paused": false
    });
    let [human, json_output] = with_server(
        server,
        |request| {
            assert_eq!(request, Request::Status {});
            Ok(status.clone())
        },
        || {
            [
                cli(runtime.path(), &["status"]),
                cli(runtime.path(), &["status", "--json"]),
            ]
        },
    );
    assert!(human.status.success(), "{:?}", human);
    let text = String::from_utf8(human.stdout).unwrap();
    for expected in [
        "idle",
        "12 bytes",
        "45 bytes",
        "Free disk is below the configured floor",
        "Pending uploads",
    ] {
        assert!(text.contains(expected), "{text:?}");
    }
    assert!(json_output.status.success(), "{:?}", json_output);
    assert_eq!(
        serde_json::from_slice::<Value>(&json_output.stdout).unwrap(),
        json!({"ok": true, "result": status})
    );
}

fn with_server<T: Send>(
    server: IpcServer,
    mut dispatch: impl FnMut(Request) -> Result<Value, String>,
    run: impl FnOnce() -> T + Send,
) -> T {
    std::thread::scope(|scope| {
        let child = scope.spawn(run);
        server.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !child.is_finished() && std::time::Instant::now() < deadline {
            match server.serve_one(&mut dispatch) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => panic!("{error}"),
            }
        }
        drop(server);
        child.join().unwrap()
    })
}

#[test]
fn control_commands_dispatch_to_the_socket_and_preserve_errors() {
    let runtime = private_runtime();
    let server = IpcServer::bind(&socket_path(runtime.path())).unwrap();
    let uploads = json!([
        {"path": "/draft.txt", "sync_state": "dirty"},
        {"path": "/conflict.txt", "sync_state": "conflict"}
    ]);
    let commands = [
        (
            vec!["pin", "onedrive/photos/a b.png"],
            json!({"op":"pin", "path":"/photos/a b.png"}),
            Value::Null,
            "Pin completed",
        ),
        (
            vec!["unpin", "onedrive"],
            json!({"op":"unpin", "path":"/"}),
            Value::Null,
            "Unpin completed",
        ),
        (
            vec!["purge", "onedrive/clean.txt"],
            json!({"op":"purge", "path":"/clean.txt"}),
            Value::Null,
            "Purge completed",
        ),
        (
            vec!["sync"],
            json!({"op":"sync"}),
            Value::Null,
            "Sync completed",
        ),
        (
            vec!["uploads"],
            json!({"op":"uploads"}),
            uploads,
            "/conflict.txt",
        ),
        (vec!["pause"], json!({"op":"pause"}), Value::Null, "Paused"),
        (
            vec!["resume"],
            json!({"op":"resume"}),
            Value::Null,
            "Resumed",
        ),
    ];
    let mut expected = commands
        .iter()
        .flat_map(|(_, request, result, _)| [(request, result), (request, result)]);
    let mut purges = 0;
    let mount = runtime.path().join("custom");
    let dirty = mount.join("dirty.txt");
    with_server(
        server,
        |request| {
            let request = serde_json::to_value(request).unwrap();
            if request == json!({"op":"purge", "path":"/dirty.txt"}) {
                purges += 1;
                return Err("Cannot purge a Dirty item".into());
            }
            let (expected_request, result) = expected.next().expect("unexpected request");
            assert_eq!(request, *expected_request);
            Ok(result.clone())
        },
        || {
            for (args, _, result, human_text) in &commands {
                let human = cli(runtime.path(), args);
                assert!(human.status.success(), "{human:?}");
                assert!(
                    String::from_utf8(human.stdout)
                        .unwrap()
                        .contains(human_text)
                );
                let mut json_args = args.clone();
                json_args.push("--json");
                let output = cli(runtime.path(), &json_args);
                assert!(output.status.success(), "{output:?}");
                assert_eq!(
                    serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                    json!({"ok":true,"result":result})
                );
            }

            let args = [
                "purge",
                dirty.to_str().unwrap(),
                "--mount-path",
                mount.to_str().unwrap(),
            ];
            let human = cli(runtime.path(), &args);
            assert!(!human.status.success());
            assert_eq!(human.stderr, b"Cannot purge a Dirty item\n");
            let output = cli(runtime.path(), &[&args[..], &["--json"]].concat());
            assert!(!output.status.success());
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                json!({"ok":false,"error":"Cannot purge a Dirty item"})
            );
        },
    );
    assert!(expected.next().is_none());
    assert_eq!(purges, 2);
}

#[test]
fn rejects_paths_outside_the_mount_and_parent_components_before_connecting() {
    let runtime = private_runtime();
    for (path, expected) in [
        ("onedrive-sibling/file", "outside the OneDrive mount"),
        ("onedrive/../file", "parent components"),
    ] {
        let output = cli(runtime.path(), &["--json", "pin", path]);
        assert!(!output.status.success());
        let error = serde_json::from_slice::<Value>(&output.stdout).unwrap();
        assert_eq!(error["ok"], false);
        assert!(
            error["error"].as_str().unwrap().contains(expected),
            "{error}"
        );
    }
}

#[test]
fn daemon_reports_invalid_mount_configuration_in_both_formats() {
    let runtime = private_runtime();
    let config = runtime.path().join("config/onedrive-rs");
    fs::create_dir_all(&config).unwrap();
    fs::write(config.join("config.toml"), "client_id = 'test-client'\n").unwrap();
    let args = ["daemon", "--mount-path", "relative"];
    let message = "The OneDrive mount path must be absolute and cannot contain ..";

    let human = cli(runtime.path(), &args);
    assert_eq!(human.status.code(), Some(1));
    assert_eq!(String::from_utf8(human.stderr).unwrap().trim_end(), message);
    let output = cli(runtime.path(), &[&["--json"], args.as_slice()].concat());
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"ok":false,"error":message})
    );
    assert!(!socket_path(runtime.path()).exists());
}

#[test]
fn argument_and_transport_failures_produce_structured_errors() {
    let runtime = private_runtime();
    for (args, exit_code, message) in [
        (&["pin", "--json"][..], 2, "<PATH>"),
        (&["auth", "invalid", "--json"], 2, "unrecognized subcommand"),
        (
            &["status", "--json"],
            1,
            "Cannot communicate with the OneDrive daemon",
        ),
    ] {
        let output = cli(runtime.path(), args);
        assert_eq!(output.status.code(), Some(exit_code), "{output:?}");
        assert!(output.stderr.is_empty());
        let response = serde_json::from_slice::<Value>(&output.stdout).unwrap();
        assert_eq!(response["ok"], false);
        assert!(
            response["error"].as_str().unwrap().contains(message),
            "{response}"
        );
    }
}

fn private_runtime() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
