//! Exercise the mounted filesystem in a bounded subprocess.
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use onedrive_core::{
    CloudError, Engine, EngineError, FakeCloudDrive, FuseAdapter, ItemKind, PinState, RemoteItem,
    SyncState, SystemClock,
};

const WORKER_DIR: &str = "ONEDRIVE_LIVE_FUSE_WORKER_DIR";
const DELTA_PAGE_SIZE: usize = 100;
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
    let temp = temp.keep();
    assert!(
        command("rm")
            .arg("-rf")
            .arg("--")
            .arg(temp)
            .status()
            .unwrap()
            .success()
    );
    assert!(result.unwrap().success());
}

fn exercise_mount(dir: &Path) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mount = dir.join("mount");
    let cache = dir.join("cache");
    let drive = Arc::new(FakeCloudDrive::new(DELTA_PAGE_SIZE));
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
        ..root.clone()
    });
    let expected_names: Vec<_> = (0..1500).map(|i| format!("entry-{i:04}")).collect();
    for name in &expected_names {
        drive.upsert(RemoteItem {
            item_id: name.clone(),
            parent_id: Some("folder".into()),
            name: Some(name.clone()),
            kind: ItemKind::File,
            ..root.clone()
        });
    }
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
    let listing_started = std::time::Instant::now();
    let mut nested: Vec<_> = fs::read_dir(mount.join("folder"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    eprintln!(
        "Mounted listing of 1,500 entries: {:?}",
        listing_started.elapsed()
    );
    nested.sort();
    assert_eq!(nested, expected_names);
    let path = mount.join("content.bin");
    let lookup_started = std::time::Instant::now();
    let metadata = fs::metadata(&path).unwrap();
    eprintln!("Mounted lookup and stat: {:?}", lookup_started.elapsed());
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

    assert_xattrs(&path, b"Cloud-only", b"none", b"\"version-1\"");
    let mut handle = File::open(&path).unwrap();
    assert!(drive.range_requests().is_empty());
    let mut sniff = vec![0; 32768];
    handle.read_exact(&mut sniff).unwrap();
    assert_eq!(sniff, bytes[..32768]);
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
    handle.read_exact_at(&mut sniff, 32768).unwrap();
    assert_eq!(sniff, bytes[32768..HEADER]);
    assert_eq!(drive.range_requests().len(), 1);
    let mut mapping = map_prefix(&path, HEADER);
    assert_mapped_bytes(&mut mapping, &bytes[..HEADER]);
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
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
    engine.set_pin_state("file", PinState::Pinned).unwrap();
    assert_xattrs(&path, b"Hydrated", b"pinned", b"\"version-1\"");
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(drive.range_requests().len(), 3);

    // Content changes with unchanged size and mtime require explicit kernel
    // invalidation. The old descriptor already has these pages resident.
    file.ctag = Some("content-2".into());
    file.etag = Some("\"version-2\"".into());
    drive.upsert(file.clone());
    let new_bytes = vec![147; bytes.len()];
    drive.set_content("file", new_bytes.clone());
    runtime.block_on(engine.sync()).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().mtime(), file.mtime);
    assert_xattrs(&path, b"Cloud-only", b"pinned", b"\"version-2\"");
    let mut updated = vec![0; HEADER];
    handle.read_exact_at(&mut updated, 0).unwrap();
    assert_eq!(updated, new_bytes[..HEADER]);
    assert_eq!(
        drive.range_requests().last().unwrap(),
        &("file".into(), 0, HEADER)
    );
    assert_eq!(drive.range_requests().len(), 4);
    mapping.stdin.take().unwrap().write_all(b"r").unwrap();
    assert_mapped_bytes(&mut mapping, &new_bytes[..HEADER]);
    assert!(mapping.wait().unwrap().success());
    assert_eq!(drive.range_requests().len(), 4);

    assert!(!mount.join("renamed").exists());
    assert!(mount.join("folder").is_dir());
    drive.upsert(RemoteItem {
        item_id: "folder".into(),
        parent_id: Some("root".into()),
        name: Some("renamed".into()),
        mtime: root.mtime + 1,
        ..root.clone()
    });
    runtime.block_on(engine.sync()).unwrap();
    assert!(!mount.join("folder").exists());
    assert_eq!(
        fs::metadata(mount.join("renamed")).unwrap().mtime(),
        file.mtime + 1
    );
    assert_eq!(fs::read_dir(mount.join("renamed")).unwrap().count(), 1500);
    assert!(engine.set_sync_state("file", SyncState::Dirty).unwrap());
    let alias = mount.join("content (pending).bin");
    let conflict = mount.join("content (conflict).bin");
    assert!(!alias.exists());
    assert!(!conflict.exists());

    let replacement = RemoteItem {
        item_id: "replacement".into(),
        ctag: Some("replacement".into()),
        ..file.clone()
    };
    drive.upsert(replacement);
    drive.set_content("replacement", vec![23; bytes.len()]);
    // Fill the first page so the replacement is visible before the final reconciliation.
    for index in 1..DELTA_PAGE_SIZE {
        drive.upsert(RemoteItem {
            item_id: format!("nameless-{index}"),
            parent_id: Some("root".into()),
            name: None,
            kind: ItemKind::File,
            ..root.clone()
        });
    }
    drive.delete("file");
    drive.fail_after(1, CloudError::Unavailable);
    assert!(matches!(
        runtime.block_on(engine.sync()),
        Err(EngineError::Cloud(CloudError::Unavailable))
    ));
    let old_inode = metadata.ino();
    let replacement_inode = fs::metadata(&path).unwrap().ino();
    assert_ne!(replacement_inode, old_inode);
    assert_eq!(fs::metadata(&alias).unwrap().ino(), old_inode);
    assert!(
        fs::read_dir(&mount)
            .unwrap()
            .any(|entry| entry.unwrap().file_name() == alias.file_name().unwrap())
    );
    let mut prefix = [0; 32];
    File::open(&path).unwrap().read_exact(&mut prefix).unwrap();
    assert_eq!(prefix, [23; 32]);
    let mut aliased = File::open(&alias).unwrap();
    aliased.read_exact(&mut prefix).unwrap();
    assert_eq!(prefix, new_bytes[..32]);
    assert!(!conflict.exists());

    runtime.block_on(engine.sync()).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().ino(), replacement_inode);
    assert!(!alias.exists());
    assert_eq!(fs::metadata(&conflict).unwrap().ino(), old_inode);
    aliased.read_exact_at(&mut prefix, 0).unwrap();
    assert_eq!(prefix, new_bytes[..32]);
    handle.read_exact_at(&mut prefix, 0).unwrap();
    assert_eq!(prefix, new_bytes[..32]);
    drop(aliased);
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

fn map_prefix(path: &Path, length: usize) -> Child {
    command("python3")
        .args([
            "-c",
            "import mmap, sys
with open(sys.argv[1], 'rb') as file:
    with mmap.mmap(file.fileno(), int(sys.argv[2]), access=mmap.ACCESS_READ) as data:
        sys.stdout.buffer.write(data[:])
        sys.stdout.buffer.flush()
        if sys.stdin.buffer.read(1):
            sys.stdout.buffer.write(data[:])",
        ])
        .arg(path)
        .arg(length.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
}

fn assert_mapped_bytes(mapping: &mut Child, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    mapping
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut actual)
        .unwrap();
    assert!(actual == expected, "mapped bytes did not match");
}

fn assert_xattrs(path: &Path, state: &[u8], pin: &[u8], etag: &[u8]) {
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
    for (name, expected) in [("state", state), ("pin", pin), ("etag", etag)] {
        assert_eq!(
            xattr::get(path, format!("user.onedrive.{name}"))
                .unwrap()
                .unwrap(),
            expected
        );
    }
    assert_eq!(xattr::get(path, "user.onedrive.missing").unwrap(), None);
}
