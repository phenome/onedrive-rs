use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use onedrive_rs::ipc::{IpcServer, Request, serve_connection, socket_path};
use serde_json::{Value, json};

#[tokio::test]
async fn status_round_trips_over_a_private_socket_removed_on_drop() {
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

    let listener = server.async_listener().unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    serve_connection(stream, async |request| {
        assert_eq!(request, Request::Status {});
        Ok(status.clone())
    })
    .await
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

    drop(listener);
    drop(server);
    assert!(!path.exists());
    drop(IpcServer::bind(&path).unwrap());
}

#[tokio::test]
async fn wire_requests_require_newline_and_canonical_mount_paths() {
    use std::net::Shutdown;

    let runtime = private_runtime();
    let path = socket_path(runtime.path());
    let server = IpcServer::bind(&path).unwrap();

    let listener = server.async_listener().unwrap();
    let exchange = async |request: &[u8]| {
        let mut stream = UnixStream::connect(&path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream.write_all(request).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();

        let (peer, _) = listener.accept().await.unwrap();
        serve_connection(peer, async |request| {
            assert_eq!(
                serde_json::to_value(request).unwrap(),
                json!({"op": "pin", "mount": "/custom mount", "path": "/folder/été photo.png"})
            );
            Ok(Value::Null)
        })
        .await
        .unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        serde_json::from_str::<Value>(&line).unwrap()
    };

    let reply = exchange(
        "{\"op\":\"pin\",\"mount\":\"/custom mount\",\"path\":\"/folder/été photo.png\"}\n"
            .as_bytes(),
    )
    .await;
    assert_eq!(reply, json!({"ok": true, "result": null}));

    for op in ["pin", "unpin", "purge"] {
        let reply = exchange(format!("{{\"op\":\"{op}\",\"path\":\"/\"}}\n").as_bytes()).await;
        assert_eq!(reply["ok"], false);
        assert!(
            reply["error"]
                .as_str()
                .unwrap()
                .contains("missing field `mount`")
        );
    }

    for invalid in [
        &b"{\"op\":\"pin\",\"mount\":\"/custom mount\",\"path\":\"folder/file\"}\n"[..],
        b"{\"op\":\"purge\",\"mount\":\"/custom mount\",\"path\":\"/folder/../file\"}\n",
        b"{\"op\":\"unpin\",\"mount\":\"/custom mount\",\"path\":\"/folder//file\"}\n",
        b"{\"op\":\"pin\",\"mount\":\"/custom mount\",\"path\":\"/file/\"}\n",
        b"{\"op\":\"pin\",\"mount\":\"/custom mount\",\"path\":\"/./file\"}\n",
        b"{\"op\":\"pin\",\"mount\":\"/custom mount\",\"path\":\"/file\\u0000\"}\n",
        b"{\"op\":\"pin\"}\n",
        b"{\"op\":\"status\",\"path\":\"/file\"}\n",
        b"{\"op\":\"rename\"}\n",
        b"{\"op\":\"status\"}",
    ] {
        let reply = exchange(invalid).await;
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
        .tempdir_in("/tmp")
        .unwrap()
}

#[test]
fn subprocess_socket_lifecycle() {
    use std::process::{Command, Stdio};

    use rustix::process::{Pid, Signal, kill_process};

    #[derive(Debug)]
    enum Outcome {
        Ready(Pid),
        Busy,
    }

    let runtime = private_runtime();
    let path = socket_path(runtime.path());
    let spawn_server = || {
        Command::new("timeout")
            .args(["-k", "1s", "5s"])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", "socket_process_fixture", "--nocapture"])
            .env("ONEDRIVE_TEST_SOCKET", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let read_outcome = |child: &mut std::process::Child| {
        let mut line = String::new();
        let mut reader = BufReader::new(child.stdout.as_mut().unwrap());
        while reader.read_line(&mut line).ok()? != 0 {
            if let Some(pid) = line.trim().strip_prefix("socket fixture ready ") {
                return Pid::from_raw(pid.parse().ok()?).map(Outcome::Ready);
            }
            if line.trim() == "socket fixture busy" {
                return Some(Outcome::Busy);
            }
            line.clear();
        }
        None
    };

    let mut first = spawn_server();
    let outcome = read_outcome(&mut first);
    let connected = UnixStream::connect(&path).is_ok();
    // Kill the fixture without running its cleanup. Its timeout wrapper still reaps it.
    let killed = if let Some(Outcome::Ready(pid)) = outcome {
        kill_process(pid, Signal::KILL).is_ok()
    } else {
        false
    };
    drop(first.stdin.take());
    let status = first.wait().unwrap();
    assert!(
        killed && connected,
        "fixture failed to start: {outcome:?} ({status})"
    );
    assert!(path.exists());

    let mut restart = spawn_server();
    let mut competitor = spawn_server();
    let outcomes = [read_outcome(&mut restart), read_outcome(&mut competitor)];
    let connected = UnixStream::connect(&path).is_ok();
    drop(restart.stdin.take());
    drop(competitor.stdin.take());
    let statuses = [restart.wait().unwrap(), competitor.wait().unwrap()];
    assert!(
        matches!(
            outcomes,
            [Some(Outcome::Ready(_)), Some(Outcome::Busy)]
                | [Some(Outcome::Busy), Some(Outcome::Ready(_))]
        ),
        "expected one ready fixture and one busy fixture, got {outcomes:?} ({statuses:?})"
    );
    assert!(connected);
    assert!(
        statuses.iter().all(|status| status.success()),
        "{statuses:?}"
    );
    assert!(!path.exists());
}

#[test]
fn socket_process_fixture() {
    let Some(path) = std::env::var_os("ONEDRIVE_TEST_SOCKET") else {
        return;
    };
    match IpcServer::bind(path.as_ref()) {
        Ok(server) => {
            println!("socket fixture ready {}", std::process::id());
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
