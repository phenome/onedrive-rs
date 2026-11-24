use std::path::Path;
use std::process::{Command, Output};

use onedrive_rs::ipc::{IpcServer, Request, socket_path};
use serde_json::{Value, json};

fn cli(runtime: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_onedrive-rs"))
        .current_dir(runtime)
        .env("XDG_RUNTIME_DIR", runtime)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn status_renders_the_same_daemon_data_as_human_text_and_json() {
    let runtime = tempfile::tempdir().unwrap();
    let server = IpcServer::bind(&socket_path(runtime.path())).unwrap();
    let status = json!({
        "sync_state": "idle",
        "unpinned_cache_bytes": 12,
        "pinned_cache_bytes": 45,
        "free_disk_floor_warning": true,
        "pending_upload_count": 1,
        "paused": false
    });
    std::thread::scope(|scope| {
        let child = scope.spawn(|| {
            [
                cli(runtime.path(), &["status"]),
                cli(runtime.path(), &["status", "--json"]),
            ]
        });
        server.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut served = 0;
        while !child.is_finished() && std::time::Instant::now() < deadline && served < 2 {
            match server.serve_one(|request| {
                assert_eq!(request, Request::Status);
                Ok(status.clone())
            }) {
                Ok(()) => served += 1,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => panic!("{error}"),
            }
        }
        drop(server);
        let [human, json_output] = child.join().unwrap();
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
    });
}
