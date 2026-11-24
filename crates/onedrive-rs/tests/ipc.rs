use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use onedrive_rs::ipc::{IpcServer, Request, socket_path};
use serde_json::{Value, json};

#[test]
fn status_round_trips_over_a_private_socket_removed_on_drop() {
    let runtime = private_runtime();
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
    let mut peer = UnixStream::connect(&path).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    peer.write_all(b"{\"op\":\"status\"}\n").unwrap();

    server
        .serve_one(|request| {
            assert_eq!(request, Request::Status {});
            Ok(status.clone())
        })
        .unwrap();

    let mut line = String::new();
    let mut reader = BufReader::new(peer);
    reader.read_line(&mut line).unwrap();
    assert!(line.ends_with('\n'));
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap(),
        json!({"ok": true, "result": status})
    );
    line.clear();
    assert_eq!(reader.read_line(&mut line).unwrap(), 0);

    drop(server);
    assert!(!path.exists());
    drop(IpcServer::bind(&path).unwrap());
}

#[test]
fn wire_requests_require_newline_and_canonical_mount_paths() {
    use std::net::Shutdown;

    let runtime = private_runtime();
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
    let runtime = private_runtime();
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

fn private_runtime() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}

#[test]
fn subprocess_socket_lifecycle() {
    use std::process::{Command, Stdio};

    let runtime = private_runtime();
    let path = socket_path(runtime.path());
    let spawn_server = || {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "socket_process_fixture", "--nocapture"])
            .env("ONEDRIVE_TEST_SOCKET", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let read_outcome = |child: &mut std::process::Child| {
        let mut line = String::new();
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let outcome = loop {
            assert_ne!(reader.read_line(&mut line).unwrap(), 0);
            if line.contains("socket fixture ready") {
                break true;
            }
            if line.contains("socket fixture busy") {
                break false;
            }
            line.clear();
        };
        child.stdout = Some(reader.into_inner());
        outcome
    };
    let mut first = spawn_server();
    assert!(read_outcome(&mut first));
    assert!(IpcServer::bind(&path).is_err());
    assert!(UnixStream::connect(&path).is_ok());
    first.kill().unwrap();
    first.wait().unwrap();
    assert!(path.exists());

    let mut restart = spawn_server();
    let mut competitor = spawn_server();
    assert_ne!(read_outcome(&mut restart), read_outcome(&mut competitor));
    assert!(UnixStream::connect(&path).is_ok());
    for child in [&mut restart, &mut competitor] {
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
    }
    assert!(!path.exists());
}

#[test]
fn socket_process_fixture() {
    let Some(path) = std::env::var_os("ONEDRIVE_TEST_SOCKET") else {
        return;
    };
    match IpcServer::bind(path.as_ref()) {
        Ok(server) => {
            println!("socket fixture ready");
            std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).unwrap();
            drop(server);
        }
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            println!("socket fixture busy");
        }
        Err(error) => panic!("{error}"),
    }
}

#[test]
fn binding_and_cleanup_use_the_opened_directory() {
    use std::os::unix::fs::symlink;

    let runtime = private_runtime();
    let path = socket_path(runtime.path());
    let parent = path.parent().unwrap();
    let moved = runtime.path().join("moved");
    let server = IpcServer::bind(&path).unwrap();
    fs::rename(parent, &moved).unwrap();
    symlink(&moved, parent).unwrap();
    assert!(IpcServer::bind(&path).is_err());
    fs::remove_file(parent).unwrap();
    fs::create_dir(parent).unwrap();
    fs::write(&path, "replacement").unwrap();

    drop(server);
    assert_eq!(fs::read_to_string(&path).unwrap(), "replacement");
    assert!(!moved.join("daemon.sock").exists());
    fs::remove_file(&path).unwrap();
    symlink(moved.join("daemon.sock"), &path).unwrap();
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(IpcServer::bind(&path).is_err());
    assert!(fs::symlink_metadata(&path).unwrap().is_symlink());
}

#[test]
fn restart_preserves_a_live_listener_without_the_directory_lock() {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::net::UnixListener;

    let runtime = private_runtime();
    let path = socket_path(runtime.path());
    fs::create_dir(path.parent().unwrap()).unwrap();
    fs::set_permissions(path.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
    let listener = UnixListener::bind(&path).unwrap();
    let inode = fs::symlink_metadata(&path).unwrap().ino();
    assert_eq!(
        IpcServer::bind(&path).err().unwrap().kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), inode);
    drop(listener);
    drop(IpcServer::bind(&path).unwrap());
    assert!(!path.exists());
}
