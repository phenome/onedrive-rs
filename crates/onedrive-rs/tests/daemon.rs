use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, Config, DeltaCursor, DeltaPage, DeltaRequest, FakeCloudDrive, ItemKind,
    RemoteItem, RemoteThumbnail,
};
use onedrive_rs::{daemon, ipc::socket_path};
use rustix::process::{Pid, Signal, kill_process};
use serde_json::Value;

const WORKER_DIR: &str = "ONEDRIVE_DAEMON_TEST_DIR";
const STALL_DELTA: &str = "ONEDRIVE_DAEMON_TEST_STALL_DELTA";

struct IsolatedCloud {
    fake: FakeCloudDrive,
    dir: PathBuf,
    stall_delta: bool,
}

#[async_trait]
impl CloudDrive for IsolatedCloud {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        if self.stall_delta || self.dir.join("stall-manual").exists() {
            let DeltaRequest::Link(link) = request else {
                panic!("expected persisted continuation");
            };
            fs::write(self.dir.join("resumed-cursor"), link).unwrap();
            return std::future::pending().await;
        }
        let page = self.fake.delta(request).await?;
        if let DeltaCursor::Complete(link) = &page.cursor {
            fs::write(self.dir.join("issued-cursor"), link).unwrap();
        }
        Ok(page)
    }

    async fn read_range(
        &self,
        item_id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        if item_id == "blocked" {
            fs::write(self.dir.join("read-started"), "").unwrap();
            return std::future::pending().await;
        }
        assert!(
            !matches!(item_id, "photo" | "missing-thumbnail"),
            "read original while seeding"
        );
        self.fake.read_range(item_id, offset, length).await
    }
    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        if folder_id == "blocked-folder" {
            fs::write(self.dir.join("thumbnail-started"), "").unwrap();
            return std::future::pending().await;
        }
        self.fake.list_thumbnails(folder_id).await
    }

    async fn download_thumbnail(&self, rendition: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        self.fake.download_thumbnail(rendition).await
    }
}

#[test]
fn daemon_worker() {
    let Some(dir) = std::env::var_os(WORKER_DIR).map(PathBuf::from) else {
        return;
    };
    fs::write(dir.join("worker-pid"), std::process::id().to_string()).unwrap();
    let fake = FakeCloudDrive::new(10);
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
    fake.upsert(root.clone());
    for (id, parent, name, kind) in [
        ("content", "root", "content.txt", ItemKind::File),
        ("cached", "root", "cached.txt", ItemKind::File),
        ("blocked", "root", "blocked.txt", ItemKind::File),
        ("photo", "root", "photo.png", ItemKind::File),
        ("missing-thumbnail", "root", "missing.png", ItemKind::File),
        ("blocked-folder", "root", "blocked-folder", ItemKind::Folder),
        (
            "blocked-thumbnail",
            "blocked-folder",
            "blocked.png",
            ItemKind::File,
        ),
    ] {
        fake.upsert(RemoteItem {
            item_id: id.into(),
            parent_id: Some(parent.into()),
            name: Some(name.into()),
            kind,
            size: if kind == ItemKind::File { 5 } else { 0 },
            etag: Some("fixture-etag".into()),
            ..root.clone()
        });
    }
    let mut thumbnail = Vec::new();
    let mut encoder = png::Encoder::new(&mut thumbnail, 1, 1);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    encoder
        .write_header()
        .unwrap()
        .write_image_data(&[0, 0, 0, 255])
        .unwrap();
    fake.set_thumbnail("photo", thumbnail);
    let stall_delta = std::env::var_os(STALL_DELTA).is_some();
    if !stall_delta {
        fake.set_content("content", b"hello".to_vec());
        fake.set_content("cached", b"world".to_vec());
    }
    let cloud = IsolatedCloud {
        fake,
        dir,
        stall_delta,
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(daemon::run(Config::load().unwrap(), None, Arc::new(cloud)))
        .unwrap();
}

#[test]
fn mounted_daemon_lifecycle() {
    let mut sandbox = Sandbox::new();
    sandbox.start(false);
    let mount = sandbox.mount.clone();
    let socket = socket_path(&sandbox.dir().join("run"));
    wait_for(|| mounted(&mount) && socket.exists());
    wait_for(|| sandbox.cli("status")["result"]["sync_state"] == "idle");
    assert_eq!(
        bounded("cat")
            .arg(mount.join("content.txt"))
            .output()
            .unwrap()
            .stdout,
        b"hello"
    );
    assert_eq!(sandbox.cli("status")["result"]["unpinned_cache_bytes"], 5);
    assert_eq!(
        sandbox.cli("status")["result"]["free_disk_floor_warning"],
        true
    );
    assert_eq!(sandbox.cli("pause")["ok"], true);
    assert_eq!(sandbox.cli("status")["result"]["paused"], true);
    assert_eq!(sandbox.cli("sync")["ok"], true);
    assert_eq!(sandbox.cli("resume")["ok"], true);
    assert_eq!(sandbox.cli("uploads")["ok"], false);
    let content_path = mount.join("content.txt");
    let content = content_path.to_str().unwrap();
    assert_eq!(xattr(&content_path, "state"), "Hydrated");
    assert_eq!(xattr(&content_path, "etag"), "fixture-etag");
    assert_eq!(sandbox.cli_args(&["pin", content])["ok"], true);
    assert_eq!(xattr(&content_path, "pin"), "pinned");
    assert_eq!(sandbox.cli("status")["result"]["pinned_cache_bytes"], 5);
    assert_eq!(sandbox.cli_args(&["unpin", content])["ok"], true);
    assert_eq!(xattr(&content_path, "pin"), "none");
    assert_eq!(sandbox.cli("status")["result"]["unpinned_cache_bytes"], 5);
    assert_eq!(sandbox.cli_args(&["purge", content])["ok"], true);
    assert_eq!(xattr(&content_path, "state"), "Cloud-only");
    assert_eq!(sandbox.cli("status")["result"]["unpinned_cache_bytes"], 0);
    let missing_path = mount.join("absent.txt");
    let missing = sandbox.cli_args(&["pin", missing_path.to_str().unwrap()]);
    assert_eq!(missing["ok"], false);
    assert!(missing["error"].as_str().unwrap().contains("was not found"));
    assert_eq!(
        bounded("cat").arg(content).output().unwrap().stdout,
        b"hello"
    );
    assert_eq!(sandbox.cli_args(&["pin", content])["ok"], true);
    assert_eq!(
        bounded("cat")
            .arg(mount.join("cached.txt"))
            .output()
            .unwrap()
            .stdout,
        b"world"
    );

    assert!(bounded("ls").arg(&mount).output().unwrap().status.success());
    let thumbnail_root = sandbox.dir().join("cache/thumbnails");
    for (directory, uri) in [
        ("x-large", "photo.png"),
        ("fail/gnome-thumbnail-factory", "missing.png"),
    ] {
        let mut entries = fs::read_dir(thumbnail_root.join(directory)).unwrap();
        let file = fs::File::open(entries.next().unwrap().unwrap().path()).unwrap();
        assert!(entries.next().is_none());
        let decoder = png::Decoder::new(BufReader::new(file));
        let mut reader = decoder.read_info().unwrap();
        let mut pixels = [0; 4];
        reader.next_frame(&mut pixels).unwrap();
        let fields = &reader.info().uncompressed_latin1_text;
        assert!(fields.iter().any(|entry| entry.keyword == "Thumb::URI"
            && entry.text == format!("file://{}/{}", mount.display(), uri)));
        assert!(
            fields
                .iter()
                .any(|entry| entry.keyword == "Thumb::MTime" && entry.text == "1700000000")
        );
        assert_eq!(pixels[3], if directory == "x-large" { 255 } else { 0 });
    }
    assert_eq!(xattr(&mount.join("photo.png"), "state"), "Cloud-only");

    assert_eq!(sandbox.cli("pause")["ok"], true);
    assert_eq!(sandbox.cli("sync")["ok"], true);
    fs::write(sandbox.dir().join("stall-manual"), "").unwrap();
    let mut manual = bounded(env!("CARGO_BIN_EXE_onedrive-rs"));
    sandbox.env(&mut manual);
    let manual = manual
        .args(["--json", "sync"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for(|| sandbox.dir().join("resumed-cursor").exists());

    let duplicates: Vec<_> = (0..32)
        .map(|_| {
            let mut peer = UnixStream::connect(&socket).unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            peer.write_all(b"{\"op\":\"sync\"}\n").unwrap();
            BufReader::new(peer)
        })
        .collect();
    assert_eq!(sandbox.cli("status")["result"]["sync_state"], "syncing");
    assert_eq!(sandbox.cli("resume")["ok"], true);
    assert_eq!(sandbox.cli("pause")["ok"], true);
    for mut peer in duplicates {
        let mut reply = String::new();
        peer.read_line(&mut reply).unwrap();
        let reply: Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(reply["error"], "OneDrive delta sync is already running");
    }
    let mut listing = bounded("ls")
        .arg(mount.join("blocked-folder"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(|| sandbox.dir().join("thumbnail-started").exists());

    let mut reader = bounded("cat")
        .arg(mount.join("blocked.txt"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for(|| sandbox.dir().join("read-started").exists());
    sandbox.stop(Signal::TERM);
    let result = manual.wait_with_output().unwrap();
    assert_eq!(result.status.code(), Some(1));
    let reply: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(reply["ok"], false);
    assert!(!reader.wait().unwrap().success());
    assert!(!listing.wait().unwrap().success());
    assert!(!socket.exists());
    assert!(!mounted(&mount));

    let checkpoint = fs::read_to_string(sandbox.dir().join("issued-cursor")).unwrap();
    let prior_cursor = sandbox.dir().join("resumed-cursor");
    assert_eq!(fs::read_to_string(&prior_cursor).unwrap(), checkpoint);
    fs::remove_file(prior_cursor).unwrap();
    fs::remove_file(sandbox.dir().join("stall-manual")).unwrap();
    let catalog_directory = sandbox.dir().join("data/onedrive-rs");
    assert!(catalog_directory.join("catalog.sqlite3").is_file());
    fs::create_dir(sandbox.dir().join("state")).unwrap();
    fs::rename(&catalog_directory, sandbox.dir().join("state/onedrive-rs")).unwrap();
    sandbox.use_custom_mount();
    let mount = sandbox.mount.clone();
    sandbox.start(true);
    wait_for(|| sandbox.dir().join("resumed-cursor").exists());
    assert_eq!(
        fs::read_to_string(sandbox.dir().join("resumed-cursor")).unwrap(),
        checkpoint
    );
    assert!(mounted(&mount));
    assert_eq!(sandbox.cli("status")["result"]["sync_state"], "syncing");
    assert!(catalog_directory.join("catalog.sqlite3").is_file());
    assert!(
        !sandbox
            .dir()
            .join("state/onedrive-rs/catalog.sqlite3")
            .exists()
    );
    assert_eq!(sandbox.cli("status")["result"]["pinned_cache_bytes"], 5);
    assert_eq!(sandbox.cli("status")["result"]["unpinned_cache_bytes"], 5);
    assert_eq!(
        bounded("cat")
            .arg(mount.join("content.txt"))
            .output()
            .unwrap()
            .stdout,
        b"hello"
    );
    sandbox.stop(Signal::INT);
    assert!(!socket.exists());
    assert!(!mounted(&mount));
    fs::remove_file(sandbox.dir().join("resumed-cursor")).unwrap();
    fs::remove_dir_all(sandbox.dir().join("cache/onedrive-rs")).unwrap();
    sandbox.start(true);
    wait_for(|| sandbox.dir().join("resumed-cursor").exists());
    assert_eq!(
        fs::read_to_string(sandbox.dir().join("resumed-cursor")).unwrap(),
        checkpoint
    );
    let status = sandbox.cli("status");
    assert_eq!(status["result"]["pinned_cache_bytes"], 0);
    assert_eq!(status["result"]["unpinned_cache_bytes"], 0);
    assert_eq!(xattr(&mount.join("content.txt"), "pin"), "pinned");
    assert_eq!(xattr(&mount.join("content.txt"), "state"), "Cloud-only");
    sandbox.stop(Signal::TERM);
}

struct Sandbox {
    temp: Option<tempfile::TempDir>,
    mount: PathBuf,
    worker: Option<Child>,
}

impl Sandbox {
    fn new() -> Self {
        let temp = tempfile::tempdir_in("/tmp").unwrap();
        let mount = temp.path().join("run/onedrive");
        let config = temp.path().join("config/onedrive-rs");
        fs::create_dir_all(&config).unwrap();
        fs::create_dir(temp.path().join("run")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(temp.path().join("run"), fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            config.join("config.toml"),
            "min_free_disk_bytes = 9223372036854775807\n",
        )
        .unwrap();
        Self {
            temp: Some(temp),
            mount,
            worker: None,
        }
    }

    fn use_custom_mount(&mut self) {
        self.mount = self.dir().join("custom-mount");
        fs::write(
            self.dir().join("config/onedrive-rs/config.toml"),
            format!("mount_path = {:?}\n", self.mount),
        )
        .unwrap();
    }

    fn dir(&self) -> &Path {
        self.temp.as_ref().unwrap().path()
    }

    fn env(&self, command: &mut Command) {
        command
            .env("HOME", self.dir().join("home"))
            .env("XDG_CONFIG_HOME", self.dir().join("config"))
            .env("XDG_RUNTIME_DIR", self.dir().join("run"))
            .env("XDG_DATA_HOME", self.dir().join("data"))
            .env("XDG_STATE_HOME", self.dir().join("state"))
            .env("XDG_CACHE_HOME", self.dir().join("cache"));
    }

    fn start(&mut self, stall_delta: bool) {
        let pid_file = self.dir().join("worker-pid");
        if pid_file.exists() {
            fs::remove_file(&pid_file).unwrap();
        }
        let mut command = Command::new("timeout");
        command
            .args(["-k", "2s", "30s"])
            .arg(std::env::current_exe().unwrap());
        command
            .args(["--exact", "daemon_worker", "--nocapture"])
            .env(WORKER_DIR, self.dir());
        command.env_remove(STALL_DELTA);
        if stall_delta {
            command.env(STALL_DELTA, "1");
        }
        self.env(&mut command);
        self.worker = Some(command.spawn().unwrap());
    }

    fn stop(&mut self, signal: Signal) {
        let pid = fs::read_to_string(self.dir().join("worker-pid")).unwrap();
        kill_process(Pid::from_raw(pid.parse().unwrap()).unwrap(), signal).unwrap();
        let worker = self.worker.as_mut().unwrap();
        let mut status: Option<ExitStatus> = None;
        wait_for(|| {
            status = worker.try_wait().unwrap();
            status.is_some()
        });
        assert!(status.unwrap().success());
        self.worker.take();
    }

    fn cli(&self, subcommand: &str) -> Value {
        self.cli_args(&[subcommand])
    }

    fn cli_args(&self, args: &[&str]) -> Value {
        let mut command = bounded(env!("CARGO_BIN_EXE_onedrive-rs"));
        self.env(&mut command);
        let output = command.arg("--json").args(args).output().unwrap();
        serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("invalid CLI response ({error}): {output:?}"))
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if let Some(mut worker) = self.worker.take() {
            let _ = kill_process(Pid::from_child(&worker), Signal::TERM);
            let _ = worker.wait();
        }
        if mounted(&self.mount) {
            let _ = bounded("fusermount3").arg("-u").arg(&self.mount).status();
        }
        let root = self.temp.take().unwrap().keep();
        if mounted(&self.mount) {
            eprintln!("Retained test directory with active mount at {root:?}");
        } else {
            let _ = bounded("rm").args(["-rf", "--"]).arg(root).status();
        }
    }
}

fn xattr(path: &Path, attribute: &str) -> String {
    let output = bounded("getfattr")
        .args(["--only-values", "-n", &format!("user.onedrive.{attribute}")])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).unwrap()
}

fn mounted(path: &Path) -> bool {
    fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|line| {
            line.split_ascii_whitespace().nth(4).map(Path::new) == Some(path)
                && line.contains(" - fuse.onedrive-rs ")
        })
}

fn bounded(program: &str) -> Command {
    let mut command = Command::new("timeout");
    command.args(["-k", "1s", "5s", program]);
    command
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(10));
    }
}
