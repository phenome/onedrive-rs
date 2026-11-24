//! Exercise the mounted filesystem in a bounded subprocess.
//!
//! Run `cargo test -p onedrive-core --test live_fuse live_thumbnail_validation -- --nocapture`
//! on a host with FUSE3, GIO, and Python GI with GnomeDesktop 4.0.
//! The worker uses an isolated XDG cache under /tmp. The supervisor unmounts
//! on failure and reports the retained mount path if cleanup itself fails.
//! To recover that directory, run `timeout -k 2s 10s fusermount3 -u <reported-mount-path>`
//! before removing its temporary parent directory.
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::mem::MaybeUninit;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use rustix::fs::inotify;

use onedrive_core::{
    CloudDrive, CloudError, DeltaPage, DeltaRequest, Engine, EngineError, FakeCloudDrive,
    FuseAdapter, ItemKind, PinState, RemoteItem, RemoteThumbnail, SyncState, SystemClock,
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
fn live_cached_reads_on_writable_mount() {
    supervise_mount("live_cached_reads_on_writable_mount", exercise_mount);
}

#[test]
fn live_thumbnail_validation() {
    supervise_mount("live_thumbnail_validation", exercise_thumbnail_validation);
}

#[test]
fn live_offline_save_restart_and_recycle() {
    supervise_mount("live_offline_save_restart_and_recycle", exercise_writeback);
}

fn supervise_mount(test: &str, exercise: fn(&Path)) {
    if let Some(dir) = std::env::var_os(WORKER_DIR) {
        exercise(Path::new(&dir));
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
        .args(["--exact", test, "--nocapture"])
        .env("XDG_CACHE_HOME", temp.path().join("xdg-cache"))
        .env("GIO_USE_VFS", "local")
        .env("GSETTINGS_BACKEND", "memory")
        .env("LC_ALL", "C")
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
    let directory = temp.keep();
    assert!(
        command("rm")
            .args(["-rf", "--"])
            .arg(&directory)
            .status()
            .unwrap()
            .success(),
        "could not remove {}",
        directory.display(),
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

struct OfflineThumbnailDrive {
    inner: Arc<FakeCloudDrive>,
    offline: AtomicBool,
}

#[async_trait]
impl CloudDrive for OfflineThumbnailDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.inner.delta(request).await
    }

    async fn read_range(
        &self,
        id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        self.inner.read_range(id, offset, length).await
    }

    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        if self.offline.load(Ordering::Relaxed) {
            return Err(CloudError::Unavailable);
        }
        self.inner.list_thumbnails(folder_id).await
    }

    async fn download_thumbnail(&self, rendition: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        self.inner.download_thumbnail(rendition).await
    }
}

fn exercise_thumbnail_validation(dir: &Path) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mount = dir.join("mount");
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
    drive.upsert(root.clone());
    let seeded = mount.join("café; #%.png");
    let failed = mount.join("failed.j2k");
    let unseen = mount.join("unseen/retry.qoi");
    let code = mount.join("code/script.ts");
    for folder in ["unseen", "code"] {
        drive.upsert(RemoteItem {
            item_id: folder.into(),
            parent_id: Some("root".into()),
            name: Some(folder.into()),
            ..root.clone()
        });
    }
    for (name, folder) in [("retry.qoi", "unseen"), ("script.ts", "code")] {
        drive.upsert(RemoteItem {
            item_id: name.into(),
            parent_id: Some(folder.into()),
            name: Some(name.into()),
            kind: ItemKind::File,
            ..root.clone()
        });
    }
    let mut rendition = Cursor::new(Vec::new());
    image::RgbImage::from_pixel(800, 400, image::Rgb([0, 100, 200]))
        .write_to(&mut rendition, image::ImageFormat::Png)
        .unwrap();
    let rendition = rendition.into_inner();
    let mut original = rendition.clone();
    original.resize(12 * 1024 * 1024, 0);
    for path in [&seeded, &failed] {
        let name = path.file_name().unwrap().to_str().unwrap();
        drive.upsert(RemoteItem {
            item_id: name.into(),
            parent_id: Some("root".into()),
            name: Some(name.into()),
            kind: ItemKind::File,
            size: original.len() as u64,
            ..root.clone()
        });
        drive.set_content(name, original.clone());
    }
    drive.set_thumbnail(seeded.file_name().unwrap().to_str().unwrap(), rendition);
    let network = Arc::new(OfflineThumbnailDrive {
        inner: drive.clone(),
        offline: AtomicBool::new(false),
    });
    let engine =
        Arc::new(Engine::open(dir.join("db"), network.clone(), Arc::new(SystemClock)).unwrap());
    runtime.block_on(engine.sync()).unwrap();
    let mounted = FuseAdapter::new(engine, runtime.handle().clone())
        .mount(&mount)
        .unwrap();

    let watch =
        inotify::init(inotify::CreateFlags::NONBLOCK | inotify::CreateFlags::CLOEXEC).unwrap();
    for path in [&seeded, &failed, &unseen, &code] {
        inotify::add_watch(
            &watch,
            path,
            inotify::WatchFlags::OPEN | inotify::WatchFlags::ACCESS,
        )
        .unwrap();
    }
    let mut buffer = [MaybeUninit::uninit(); 512];
    let mut events = inotify::Reader::new(watch, &mut buffer);

    let directory = fs::read_dir(&mount).unwrap();
    assert!(drive.thumbnail_list_requests().is_empty());
    assert_eq!(directory.collect::<Result<Vec<_>, _>>().unwrap().len(), 4);

    for path in [&seeded, &failed] {
        let gio = command("gio")
            .args(["info", "-a", "thumbnail::*,time::*"])
            .arg(path)
            .output()
            .unwrap();
        assert!(gio.status.success(), "{gio:?}");
        let info = String::from_utf8(gio.stdout).unwrap();
        assert!(info.contains("thumbnail::is-valid: TRUE"), "{info}");
        assert!(info.contains("time::modified: 1700000000"), "{info}");
        if path == &seeded {
            assert!(
                info.contains("thumbnail::path: ") && info.contains("/x-large/"),
                "{info}"
            );
        } else {
            assert!(info.contains("thumbnail::failed: TRUE"), "{info}");
        }
    }

    let gnome = command("python")
        .args([
            "-c",
            r#"
import sys
import gi
gi.require_version("GnomeDesktop", "4.0")
from gi.repository import Gio, GnomeDesktop

seeded, failed = (Gio.File.new_for_path(path).get_uri() for path in sys.argv[1:])
factory = GnomeDesktop.DesktopThumbnailFactory.new(GnomeDesktop.DesktopThumbnailSize.XLARGE)
assert factory.lookup(seeded, 1700000000)
assert factory.has_valid_failed_thumbnail(failed, 1700000000)
assert not factory.can_thumbnail(failed, "image/x-jp2-codestream", 1700000000)
"#,
        ])
        .arg(&seeded)
        .arg(&failed)
        .output()
        .unwrap();
    assert!(
        gnome.status.success(),
        "{}",
        String::from_utf8_lossy(&gnome.stderr)
    );

    network.offline.store(true, Ordering::Relaxed);
    let list = |path: &Path| {
        command("gio")
            .args(["list", "-a", "standard::name"])
            .arg(path)
            .output()
            .unwrap()
    };
    let seeded_listing = list(&mount);
    assert!(seeded_listing.status.success(), "{seeded_listing:?}");
    assert!(String::from_utf8_lossy(&seeded_listing.stdout).contains("failed.j2k"));
    let code_listing = list(code.parent().unwrap());
    assert!(code_listing.status.success(), "{code_listing:?}");
    assert!(
        code_listing.stdout.starts_with(b"script.ts\t"),
        "{code_listing:?}"
    );
    let unseen_folder = unseen.parent().unwrap();
    let error = fs::read_dir(unseen_folder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(rustix::io::Errno::IO.raw_os_error())
    );
    let unavailable = list(unseen_folder);
    assert!(unavailable.stdout.is_empty(), "{unavailable:?}");
    assert_eq!(drive.thumbnail_list_requests(), ["root"]);

    network.offline.store(false, Ordering::Relaxed);
    let retried = list(unseen_folder);
    assert!(retried.status.success(), "{retried:?}");
    assert!(retried.stdout.starts_with(b"retry.qoi\t"), "{retried:?}");
    assert_eq!(drive.thumbnail_list_requests(), ["root", "unseen"]);
    assert_eq!(events.next().unwrap_err(), rustix::io::Errno::AGAIN);
    assert!(drive.range_requests().is_empty());

    // Verify the observers detect a deliberate source open and read.
    let mut byte = [0];
    File::open(&seeded).unwrap().read_exact(&mut byte).unwrap();
    assert_eq!(byte, [0x89]);
    assert!(
        events
            .next()
            .unwrap()
            .events()
            .contains(inotify::ReadFlags::OPEN)
    );
    assert!(
        events
            .next()
            .unwrap()
            .events()
            .contains(inotify::ReadFlags::ACCESS)
    );
    assert_eq!(drive.range_requests(), [("café; #%.png".into(), 0, HEADER)]);

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

fn exercise_writeback(dir: &Path) {
    use onedrive_core::{EngineError, SyncState};
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mount = dir.join("mount");
    let drive = Arc::new(FakeCloudDrive::new(100));
    drive.upsert(RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        size: 0,
        mtime: 100,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    });
    let open =
        || Arc::new(Engine::open(dir.join("db"), drive.clone(), Arc::new(SystemClock)).unwrap());
    let engine = open();
    runtime.block_on(engine.sync()).unwrap();
    drive.set_online(false);
    let session = FuseAdapter::new(engine.clone(), runtime.handle().clone())
        .mount(&mount)
        .unwrap();

    let draft = mount.join("document");
    fs::write(&draft, b"previous-longer-content").unwrap();
    fs::write(&draft, b"first").unwrap();
    let editor = OpenOptions::new().write(true).open(&draft).unwrap();
    editor.write_all_at(b"X", 1).unwrap();
    editor.set_len(7).unwrap();
    editor.sync_all().unwrap();
    fs::set_permissions(&draft, fs::Permissions::from_mode(0o640)).unwrap();
    assert_eq!(fs::metadata(&draft).unwrap().mode() & 0o777, 0o640);
    let mut append = OpenOptions::new().append(true).open(&draft).unwrap();
    append.write_all(b"!").unwrap();
    assert_eq!(fs::read(&draft).unwrap(), b"fXrst\0\0!");
    drop(append);
    drop(editor);

    assert!(
        command("python3")
            .args([
                "-c",
                "import mmap, sys
with open(sys.argv[1], 'r+b') as file:
 with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE) as view:
  view[1:2] = b'Y'
  view.flush()"
            ])
            .arg(&draft)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(fs::read(&draft).unwrap(), b"fYrst\0\0!");

    fs::create_dir(mount.join("work")).unwrap();
    fs::write(mount.join("work/final"), b"previous").unwrap();
    let old_handle = File::open(mount.join("work/final")).unwrap();
    fs::rename(&draft, mount.join("work/final")).unwrap();
    File::open(mount.join("work")).unwrap().sync_all().unwrap();
    assert!(!draft.exists());
    assert_eq!(old_handle.metadata().unwrap().nlink(), 0);
    let mut previous = Vec::new();
    (&old_handle).read_to_end(&mut previous).unwrap();
    assert_eq!(previous, b"previous");
    drop(old_handle);

    let file = engine.resolve_path("/work/final").unwrap().unwrap();
    assert_xattrs(&mount.join("work/final"), b"Dirty", b"none", b"");
    assert!(matches!(
        engine.check_purge(&file.item_id),
        Err(EngineError::PendingUpload { .. })
    ));
    assert_eq!(
        engine.resolve_path("/work").unwrap().unwrap().sync_state,
        SyncState::Dirty
    );
    assert!(fs::create_dir(mount.join(".Trash-1000")).is_err());

    unmount(&mount, session);
    drop(engine);

    let engine = open();
    let session = FuseAdapter::new(engine.clone(), runtime.handle().clone())
        .mount(&mount)
        .unwrap();
    assert_eq!(fs::read(mount.join("work/final")).unwrap(), b"fYrst\0\0!");
    assert_eq!(
        fs::metadata(mount.join("work/final")).unwrap().mode() & 0o777,
        0o640
    );
    assert_xattrs(&mount.join("work/final"), b"Dirty", b"none", b"");
    drive.set_online(true);
    let deadline = Instant::now() + Duration::from_secs(7);
    let uploaded = loop {
        let item = engine.resolve_path("/work/final").unwrap().unwrap();
        if item.sync_state == SyncState::Clean {
            break item;
        }
        assert!(Instant::now() < deadline, "offline bytes did not upload");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(uploaded.inode, file.inode);
    assert_ne!(uploaded.item_id, file.item_id);
    assert!(uploaded.etag.is_some() && uploaded.ctag.is_some());
    engine.check_purge(&uploaded.item_id).unwrap();
    runtime.block_on(engine.sync()).unwrap();
    assert_eq!(
        engine.resolve_path("/work/final").unwrap().unwrap().inode,
        file.inode
    );
    assert_eq!(engine.list_children("root").unwrap().len(), 1);
    assert_eq!(fs::read(mount.join("work/final")).unwrap(), b"fYrst\0\0!");
    assert_ne!(
        xattr::get(mount.join("work/final"), "user.onedrive.state")
            .unwrap()
            .unwrap(),
        b"Dirty"
    );

    let folder = engine.resolve_path("/work").unwrap().unwrap();
    fs::remove_file(mount.join("work/final")).unwrap();
    fs::remove_dir(mount.join("work")).unwrap();
    assert!(engine.resolve_path("/work").unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(7);
    while !drive.recycled_items().contains(&folder.item_id) {
        assert!(
            Instant::now() < deadline,
            "deletes did not reach the recycle bin"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(drive.recycled_items(), [uploaded.item_id, folder.item_id]);
    unmount(&mount, session);
}

fn unmount(path: &Path, session: fuser::BackgroundSession) {
    assert!(
        command("fusermount3")
            .arg("-u")
            .arg(path)
            .status()
            .unwrap()
            .success()
    );
    assert!(mount_type(path).is_none());
    session.join().unwrap();
}
