use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, Config, DeltaPage, DeltaRequest, FakeCloudDrive, ItemKind, RemoteItem,
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
        self.fake.delta(request).await
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
        self.fake.read_range(item_id, offset, length).await
    }
}

#[test]
fn daemon_worker() {
    let Some(dir) = std::env::var_os(WORKER_DIR).map(PathBuf::from) else {
        return;
    };
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
    for (id, name) in [("content", "content.txt"), ("blocked", "blocked.txt")] {
        fake.upsert(RemoteItem {
            item_id: id.into(),
            parent_id: Some("root".into()),
            name: Some(name.into()),
            kind: ItemKind::File,
            size: 5,
            ..root.clone()
        });
    }
    let stall_delta = std::env::var_os(STALL_DELTA).is_some();
    if !stall_delta {
        fake.set_content("content", b"hello".to_vec());
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
    assert_eq!(
        sandbox.cli_args(&["pin", mount.join("content.txt").to_str().unwrap()])["error"],
        "Cache retention commands are unavailable in this build"
    );

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
    assert!(!socket.exists());
    assert!(!mounted(&mount));

    let checkpoint = sandbox.checkpoint();
    let prior_cursor = sandbox.dir().join("resumed-cursor");
    assert_eq!(fs::read_to_string(&prior_cursor).unwrap(), checkpoint);
    fs::remove_file(prior_cursor).unwrap();
    fs::remove_file(sandbox.dir().join("stall-manual")).unwrap();
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
    assert_eq!(
        sandbox.cli_args(&["purge", mount.join("content.txt").to_str().unwrap()])["error"],
        "Cache retention commands are unavailable in this build"
    );
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
    assert_eq!(sandbox.checkpoint(), checkpoint);
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
            .env("XDG_STATE_HOME", self.dir().join("state"))
            .env("XDG_CACHE_HOME", self.dir().join("cache"));
    }

    fn start(&mut self, stall_delta: bool) {
        let mut command = Command::new(std::env::current_exe().unwrap());
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
        let worker = self.worker.as_mut().unwrap();
        kill_process(Pid::from_child(worker), signal).unwrap();
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

    fn checkpoint(&self) -> String {
        let db = rusqlite::Connection::open(self.dir().join("state/onedrive-rs/catalog.sqlite3"))
            .unwrap();
        let mode: String = db
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        db.query_row(
            "SELECT link FROM delta_checkpoint WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if let Some(mut worker) = self.worker.take() {
            let _ = worker.kill();
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
