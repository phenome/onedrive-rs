use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use onedrive_rs::ipc::{IpcClient, IpcServer, Request, Response, socket_path};
use serde_json::{Value, json};

#[test]
fn status_round_trips_over_a_private_socket_removed_on_drop() {
    let runtime = tempfile::tempdir().unwrap();
    let path = socket_path(runtime.path());
    let server = IpcServer::bind(&path).unwrap();
    assert_eq!(
        fs::symlink_metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(IpcServer::bind(&path).is_err());

    let status = json!({
        "sync_state": "idle",
        "unpinned_cache_bytes": 12,
        "pinned_cache_bytes": 45,
        "free_disk_floor_warning": false,
        "pending_upload_count": 1,
        "paused": false
    });
    std::thread::scope(|scope| {
        let responder = scope.spawn(|| {
            let dispatch = |request| {
                assert_eq!(request, Request::Status);
                Ok(status.clone())
            };
            server.serve_one(dispatch).unwrap();
            server.serve_one(dispatch).unwrap();
        });

        let mut peer = UnixStream::connect(&path).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        peer.write_all(b"{\"op\":\"status\"}\n").unwrap();
        let mut line = String::new();
        let mut reader = BufReader::new(peer);
        reader.read_line(&mut line).unwrap();
        assert!(line.ends_with('\n'));
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap(),
            json!({"ok": true, "result": status})
        );

        assert_eq!(
            IpcClient::new(&path).request(&Request::Status).unwrap(),
            Response::Success(status.clone())
        );
        responder.join().unwrap();
    });

    drop(server);
    assert!(!path.exists());
    drop(IpcServer::bind(&path).unwrap());
}
