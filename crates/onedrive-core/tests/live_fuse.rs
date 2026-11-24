//! Exercise the mounted filesystem in a bounded subprocess.
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use onedrive_core::{Engine, FakeCloudDrive, FuseAdapter, ItemKind, RemoteItem, SystemClock};

const WORKER_DIR: &str = "ONEDRIVE_LIVE_FUSE_WORKER_DIR";
const HEADER: usize = 64 * 1024;
const STREAM: usize = 2 * 1024 * 1024;

fn command(program: &str) -> Command {
    let mut command = Command::new("timeout");
    command.args(["-k", "2s", "10s", program]);
    command
}

fn mount_type(path: &Path) -> Option<String> {
    fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace().skip(1);
            (Path::new(fields.next()?) == path).then(|| fields.next().unwrap().to_owned())
        })
}

#[test]
fn live_read_only_mount() {
    if let Some(dir) = std::env::var_os(WORKER_DIR) {
        exercise_mount(Path::new(&dir));
        return;
    }

    let temp = tempfile::tempdir_in("/tmp").unwrap();
    assert!(
        !temp
            .path()
            .canonicalize()
            .unwrap()
            .starts_with(fs::canonicalize(std::env::var_os("HOME").unwrap()).unwrap())
    );
    let mount = temp.path().join("mount");
    fs::create_dir(&mount).unwrap();
    let mut worker = Command::new("timeout");
    let result = worker
        .args(["-k", "3s", "45s"])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", "live_read_only_mount", "--nocapture"])
        .env(WORKER_DIR, temp.path())
        .status();

    // The supervisor can still unmount if the worker panics or times out.
    if mount_type(&mount).is_some() {
        let cleanup = command("fusermount3").arg("-u").arg(&mount).status();
        if !cleanup.is_ok_and(|status| status.success()) || mount_type(&mount).is_some() {
            let retained = temp.keep();
            panic!("could not unmount {}", retained.join("mount").display());
        }
    }
    assert!(result.unwrap().success());
}

fn exercise_mount(dir: &Path) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mount = dir.join("mount");
    let cache = dir.join("cache");
    let drive = Arc::new(FakeCloudDrive::new(100));
    let root = RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        size: 0,
        mtime: 1_700_000_000,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    };
    let bytes: Vec<_> = (0..STREAM + 29).map(|i| (i % 251) as u8).collect();
    let mut file = RemoteItem {
        item_id: "file".into(),
        parent_id: Some("root".into()),
        name: Some("content.bin".into()),
        kind: ItemKind::File,
        size: bytes.len() as u64,
        etag: Some("\"version-1\"".into()),
        ctag: Some("content-1".into()),
        ..root.clone()
    };
    drive.upsert(root.clone());
    drive.upsert(file.clone());
    drive.upsert(RemoteItem {
        item_id: "folder".into(),
        parent_id: Some("root".into()),
        name: Some("folder".into()),
        ..root
    });
    drive.set_content("file", bytes.clone());
    let engine = Arc::new(
        Engine::open_with_cache(dir.join("db"), &cache, drive.clone(), Arc::new(SystemClock))
            .unwrap(),
    );
    runtime.block_on(engine.sync()).unwrap();
    let delta_requests = drive.requests();
    let mounted = FuseAdapter::new(engine.clone(), runtime.handle().clone())
        .mount(&mount)
        .unwrap();

    assert_eq!(mount_type(&mount).as_deref(), Some("fuse.onedrive-rs"));
    let mut names: Vec<_> = fs::read_dir(&mount)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names, ["content.bin", "folder"]);
    assert_eq!(fs::read_dir(mount.join("folder")).unwrap().count(), 0);
    let path = mount.join("content.bin");
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), bytes.len() as u64);
    assert_eq!(metadata.mtime(), file.mtime);
    assert_eq!(metadata.ino(), engine.item("file").unwrap().unwrap().inode);
    assert_eq!(drive.requests(), delta_requests);
    assert!(drive.range_requests().is_empty());

    let gio = command("gio")
        .args(["info", "-f", "-a", "filesystem::*"])
        .arg(&mount)
        .output()
        .unwrap();
    assert!(gio.status.success(), "{gio:?}");
    let gio_info = String::from_utf8(gio.stdout).unwrap();
    assert!(gio_info.contains("filesystem::remote: FALSE"), "{gio_info}");
    let host = rustix::fs::statvfs(&cache).unwrap();
    let mounted_space = rustix::fs::statvfs(&mount).unwrap();
    assert_eq!(mounted_space.f_blocks, host.f_blocks);
    assert_eq!(mounted_space.f_frsize, host.f_frsize);
    assert!(mounted_space.f_bavail > 0 && mounted_space.f_bavail <= mounted_space.f_blocks);
    let err = fs::create_dir(mount.join(".Trash-1000")).unwrap_err();
    assert!(
        [rustix::io::Errno::ROFS, rustix::io::Errno::ACCESS]
            .iter()
            .any(|errno| err.raw_os_error() == Some(errno.raw_os_error()))
    );

    assert_xattrs(&path, b"Cloud-only", b"\"version-1\"");
    let mut handle = File::open(&path).unwrap();
    assert!(drive.range_requests().is_empty());
    let mut sniff = vec![0; 32768];
    handle.read_exact(&mut sniff).unwrap();
    assert_eq!(sniff, bytes[..32768]);
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
    handle.read_exact_at(&mut sniff, 32768).unwrap();
    assert_eq!(sniff, bytes[32768..HEADER]);
    assert_eq!(drive.range_requests().len(), 1);
    let mut span = [0; 32];
    handle.read_exact_at(&mut span, STREAM as u64 - 16).unwrap();
    assert_eq!(span, bytes[STREAM - 16..STREAM + 16]);
    assert_eq!(
        drive.range_requests(),
        [
            ("file".into(), 0, HEADER),
            ("file".into(), 0, STREAM),
            ("file".into(), STREAM as u64, STREAM),
        ]
    );
    assert_xattrs(&path, b"Hydrated", b"\"version-1\"");
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(drive.range_requests().len(), 3);

    file.mtime += 1;
    file.etag = Some("\"version-2\"".into());
    drive.upsert(file.clone());
    let new_bytes = vec![147; bytes.len()];
    drive.set_content("file", new_bytes.clone());
    runtime.block_on(engine.sync()).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().mtime(), file.mtime);
    assert_xattrs(&path, b"Cloud-only", b"\"version-2\"");
    handle.read_exact_at(&mut sniff, 0).unwrap();
    assert_eq!(sniff, new_bytes[..32768]);
    assert_eq!(
        drive.range_requests().last().unwrap(),
        &("file".into(), 0, HEADER)
    );
    assert_eq!(drive.range_requests().len(), 4);
    drop(handle);

    assert!(
        command("fusermount3")
            .arg("-u")
            .arg(&mount)
            .status()
            .unwrap()
            .success()
    );
    assert!(mount_type(&mount).is_none());
    mounted.join().unwrap();
}

fn assert_xattrs(path: &Path, state: &[u8], etag: &[u8]) {
    let mut names: Vec<_> = xattr::list(path).unwrap().collect();
    names.sort();
    assert_eq!(
        names,
        [
            "user.onedrive.etag",
            "user.onedrive.pin",
            "user.onedrive.state"
        ]
    );
    for (name, expected) in [("state", state), ("pin", b"none"), ("etag", etag)] {
        assert_eq!(
            xattr::get(path, format!("user.onedrive.{name}"))
                .unwrap()
                .unwrap(),
            expected
        );
    }
    assert_eq!(xattr::get(path, "user.onedrive.missing").unwrap(), None);
}
