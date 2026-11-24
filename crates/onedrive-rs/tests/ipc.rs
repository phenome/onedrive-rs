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
                assert_eq!(request, Request::Status {});
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
            IpcClient::new(&path).request(&Request::Status {}).unwrap(),
            Response::Success(status.clone())
        );
        responder.join().unwrap();
    });

    drop(server);
    assert!(!path.exists());
    drop(IpcServer::bind(&path).unwrap());
}

#[test]
fn wire_requests_require_newline_and_canonical_mount_paths() {
    use std::net::Shutdown;

    let runtime = tempfile::tempdir().unwrap();
    let path = socket_path(runtime.path());
    let server = IpcServer::bind(&path).unwrap();

    let exchange = |request: &[u8]| {
        let mut stream = UnixStream::connect(&path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream.write_all(request).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();

        server
            .serve_one(|request| {
                assert_eq!(
                    serde_json::to_value(request).unwrap(),
                    json!({"op": "pin", "path": "/folder/été photo.png"})
                );
                Ok(Value::Null)
            })
            .unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        serde_json::from_str::<Value>(&line).unwrap()
    };

    let reply = exchange("{\"op\":\"pin\",\"path\":\"/folder/été photo.png\"}\n".as_bytes());
    assert_eq!(reply, json!({"ok": true, "result": null}));

    for invalid in [
        &b"{\"op\":\"pin\",\"path\":\"folder/file\"}\n"[..],
        b"{\"op\":\"purge\",\"path\":\"/folder/../file\"}\n",
        b"{\"op\":\"unpin\",\"path\":\"/folder//file\"}\n",
        b"{\"op\":\"pin\",\"path\":\"/file/\"}\n",
        b"{\"op\":\"pin\",\"path\":\"/./file\"}\n",
        b"{\"op\":\"pin\",\"path\":\"/file\\u0000\"}\n",
        b"{\"op\":\"pin\"}\n",
        b"{\"op\":\"status\",\"path\":\"/file\"}\n",
        b"{\"op\":\"rename\"}\n",
        b"{\"op\":\"status\"}",
    ] {
        let reply = exchange(invalid);
        assert_eq!(reply["ok"], false);
        assert!(reply["error"].as_str().unwrap().contains("invalid request"));
    }
}

#[test]
fn binding_and_cleanup_preserve_existing_files() {
    let runtime = tempfile::tempdir().unwrap();
    let path = socket_path(runtime.path());
    let parent = path.parent().unwrap();

    fs::create_dir(parent).unwrap();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        IpcServer::bind(&path).err().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert!(!path.exists());

    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&path, "existing").unwrap();
    assert!(IpcServer::bind(&path).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), "existing");

    fs::remove_file(&path).unwrap();
    let server = IpcServer::bind(&path).unwrap();
    fs::remove_file(&path).unwrap();
    fs::write(&path, "replacement").unwrap();
    drop(server);
    assert_eq!(fs::read_to_string(&path).unwrap(), "replacement");
}
