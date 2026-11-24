use std::env;
use std::fs::{self, DirBuilder, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use onedrive_core::{
    CloudDrive, Config, Engine, FuseAdapter, SyncState as ItemSyncState, SystemClock,
};
use rustix::fs::{FlockOperation, flock};
use serde_json::Value;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use crate::ipc::{IpcServer, MountPath, Request, Status, Upload, serve_connection, socket_path};

const MAX_CONNECTIONS: usize = 32;
const MAX_LONG_JOBS: usize = 24;

pub fn runtime_dir() -> Result<PathBuf, String> {
    absolute_env("XDG_RUNTIME_DIR")
        .ok_or_else(|| "XDG_RUNTIME_DIR must be an absolute directory path".to_owned())
}

pub fn mount_path(
    runtime: &Path,
    config: &Config,
    cli_override: Option<&Path>,
) -> Result<PathBuf, String> {
    let path = cli_override
        .or(config.mount_path.as_deref())
        .map_or_else(|| runtime.join("onedrive"), Path::to_path_buf);
    if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
        return Err("The OneDrive mount path must be absolute and cannot contain ..".into());
    }
    Ok(path)
}

fn absolute_env(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

fn xdg_home(variable: &str, fallback: &str) -> Result<PathBuf, String> {
    absolute_env(variable)
        .or_else(|| absolute_env("HOME").map(|home| home.join(fallback)))
        .ok_or_else(|| format!("Set {variable} or HOME to an absolute path"))
}

/// Run the foreground daemon with the selected cloud backend.
pub async fn run(
    config: Config,
    cli_mount: Option<&Path>,
    cloud: Arc<dyn CloudDrive>,
) -> Result<(), String> {
    let mut interrupt = signal(SignalKind::interrupt()).map_err(|error| error.to_string())?;
    let mut terminate = signal(SignalKind::terminate()).map_err(|error| error.to_string())?;
    let runtime = runtime_dir()?;
    let mount = mount_path(&runtime, &config, cli_mount)?;
    let data = xdg_home("XDG_DATA_HOME", ".local/share")?.join("onedrive-rs");
    let cache = xdg_home("XDG_CACHE_HOME", ".cache")?.join("onedrive-rs");
    let period = Duration::from_secs(config.delta_poll_interval_secs);
    if period.is_zero() || Instant::now().checked_add(period).is_none() {
        return Err("delta_poll_interval_secs must be a positive, representable duration".into());
    }

    let server = IpcServer::bind(&socket_path(&runtime))
        .map_err(|error| format!("Cannot bind OneDrive IPC socket ({error})"))?;
    let listener = server.async_listener().map_err(|error| error.to_string())?;
    let _data_lock = prepare_storage(&data, &cache)?;
    let _legacy_lock = match xdg_home("XDG_STATE_HOME", ".local/state") {
        Ok(state) => migrate_legacy_catalog(&state.join("onedrive-rs"), &data)
            .map_err(|error| format!("Cannot migrate OneDrive catalog ({error})"))?,
        Err(_) => None,
    };
    let engine = Arc::new(
        Engine::open_with_config(
            data.join("catalog.sqlite3"),
            cache,
            cloud,
            Arc::new(SystemClock),
            config,
        )
        .map_err(|error| format!("Cannot open OneDrive catalog at {data:?} ({error})"))?,
    );
    prepare_mount(&mount)
        .map_err(|error| format!("Cannot prepare mount at {mount:?} ({error})"))?;
    let (shutdown, fuse_shutdown) = watch::channel(());
    let mounted = FuseAdapter::new(engine.clone(), tokio::runtime::Handle::current())
        .with_shutdown(fuse_shutdown)
        .mount(&mount)
        .map_err(|error| format!("Cannot mount OneDrive at {mount:?} ({error})"))?;

    let delta = Arc::new(DeltaSync::new(engine, mount.clone()));
    let mut clients = JoinSet::new();
    let result = tokio::select! {
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
        _ = delta.poll(period) => Ok(()),
        _ = delta.upload_worker() => Ok(()),
        _ = delta.hydration_worker() => Ok(()),
        result = async {
            loop {
                tokio::select! {
                    _ = clients.join_next(), if !clients.is_empty() => {}
                    accepted = listener.accept(), if clients.len() < MAX_CONNECTIONS => {
                        let (stream, _) = accepted?;
                        let delta = delta.clone();
                        clients.spawn(async move {
                            let _ = serve_connection(stream, async |request| delta.dispatch(request).await).await;
                        });
                    }
                }
            }
        } => result,
    };
    drop(listener);
    drop(shutdown);
    clients.shutdown().await;
    let unmount = tokio::task::spawn_blocking(move || mounted.umount_and_join())
        .await
        .map_err(|error| error.to_string())?;
    unmount.map_err(|error| format!("Cannot unmount OneDrive at {mount:?} ({error})"))?;
    result.map_err(|error: io::Error| format!("OneDrive IPC failed ({error})"))
}

fn prepare_storage(state: &Path, cache: &Path) -> Result<File, String> {
    for path in [state, cache] {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(|error| format!("Cannot create OneDrive directory at {path:?} ({error})"))?;
        validate_storage(path)?;
    }
    lock_storage(state)
}

fn validate_storage(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("Cannot inspect OneDrive directory at {path:?} ({error})"))?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(format!(
            "OneDrive directory at {path:?} must be private and owned by the effective user"
        ));
    }
    Ok(())
}

fn lock_storage(path: &Path) -> Result<File, String> {
    validate_storage(path)?;
    let lock = File::open(path).map_err(|error| format!("Cannot open {path:?} ({error})"))?;
    flock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|error| format!("Cannot lock OneDrive storage at {path:?} ({error})"))?;
    Ok(lock)
}

fn migrate_legacy_catalog(
    state: &Path,
    data: &Path,
) -> Result<Option<File>, Box<dyn std::error::Error>> {
    if !Engine::catalog_storage_present(&state.join("catalog.sqlite3"))?
        || state.canonicalize()? == data.canonicalize()?
    {
        return Ok(None);
    }
    let legacy_lock = lock_storage(state)?;
    Engine::relocate_catalog(
        &state.join("catalog.sqlite3"),
        &data.join("catalog.sqlite3"),
    )?;
    Ok(Some(legacy_lock))
}

fn prepare_mount(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    if !fs::symlink_metadata(path)?.is_dir() || fs::read_dir(path)?.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mount path must be an empty directory",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SyncState {
    Starting,
    Syncing,
    Idle,
    Error,
}

impl SyncState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Syncing => "syncing",
            Self::Idle => "idle",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PollState {
    paused: bool,
    resume_count: u64,
}

struct DeltaSync {
    engine: Arc<Engine>,
    poll_state: watch::Sender<PollState>,
    last_result: Mutex<SyncState>,
    long_jobs: Semaphore,
    mount: PathBuf,
    hydration_requested: Notify,
}

impl DeltaSync {
    fn new(engine: Arc<Engine>, mount: PathBuf) -> Self {
        Self {
            engine,
            mount,
            poll_state: watch::channel(PollState::default()).0,
            last_result: Mutex::new(SyncState::Starting),
            long_jobs: Semaphore::new(MAX_LONG_JOBS),
            hydration_requested: Notify::new(),
        }
    }

    async fn sync(&self, background: bool) -> Result<(), String> {
        let Ok(mut result) = self.last_result.try_lock() else {
            return if background {
                Ok(())
            } else {
                Err("OneDrive delta sync is already running".into())
            };
        };
        if background && self.poll_state.borrow().paused {
            return Ok(());
        }
        let synced = async {
            self.engine.sync_catalog().await?;
            self.hydration_requested.notify_one();
            if !background {
                self.engine.drain_uploads().await?;
            }
            Ok::<_, onedrive_core::EngineError>(())
        }
        .await;
        *result = if synced.is_ok() {
            SyncState::Idle
        } else {
            SyncState::Error
        };
        synced.map_err(|error| error.to_string())
    }

    async fn poll(&self, period: Duration) {
        let mut state = self.poll_state.subscribe();
        loop {
            let resume_count = state
                .wait_for(|state| !state.paused)
                .await
                .unwrap()
                .resume_count;
            if let Err(error) = self.sync(true).await {
                eprintln!("OneDrive delta sync failed ({error})");
            }
            let next = Instant::now() + period;
            tokio::select! {
                _ = sleep_until(next) => {}
                _ = state.wait_for(|state| state.paused || state.resume_count != resume_count) => {}
            }
        }
    }

    async fn hydration_worker(&self) {
        let mut state = self.poll_state.subscribe();
        loop {
            self.hydration_requested.notified().await;
            state.wait_for(|current| !current.paused).await.unwrap();
            tokio::select! {
                _ = state.wait_for(|current| current.paused) => {
                    self.hydration_requested.notify_one();
                }
                result = self.engine.hydrate_pending_pins() => {
                    if let Err(error) = result {
                        eprintln!("OneDrive pinned download failed ({error})");
                    }
                }
            }
        }
    }

    async fn upload_worker(&self) {
        let mut state = self.poll_state.subscribe();
        loop {
            let resume_count = state
                .wait_for(|state| !state.paused)
                .await
                .unwrap()
                .resume_count;
            tokio::select! {
                biased;
                _ = state.wait_for(|state| state.paused || state.resume_count != resume_count) => {}
                _ = Engine::run_upload_worker(Arc::downgrade(&self.engine)) => return,
            }
        }
    }

    fn uploads(&self) -> Result<Value, String> {
        let entries = self
            .engine
            .pending_upload_paths()
            .map_err(|error| error.to_string())?;
        let uploads = entries
            .into_iter()
            .map(|(path, state)| {
                let sync_state = match state {
                    ItemSyncState::Dirty => "dirty",
                    ItemSyncState::Uploading => "uploading",
                    ItemSyncState::Conflict => "conflict",
                    ItemSyncState::Clean => "clean",
                };
                Ok(Upload {
                    path: MountPath::try_from(
                        path.into_os_string()
                            .into_string()
                            .map_err(|_| "OneDrive upload path is not UTF-8")?,
                    )?,
                    sync_state: sync_state.into(),
                })
            })
            .collect::<Result<Vec<_>, &str>>()?;
        serde_json::to_value(uploads).map_err(|error| error.to_string())
    }

    fn resolve_item(&self, path: &MountPath) -> Result<String, String> {
        self.engine
            .resolve_path(path.as_str())
            .map_err(|error| error.to_string())?
            .map(|item| item.item_id)
            .ok_or_else(|| format!("OneDrive path {:?} was not found", path.as_str()))
    }

    async fn dispatch(&self, request: Request) -> Result<Value, String> {
        let _permit = if request.is_long_running() {
            Some(
                self.long_jobs
                    .try_acquire()
                    .map_err(|_| "OneDrive is busy")?,
            )
        } else {
            None
        };
        match request {
            Request::Status {} => {
                let status = self
                    .engine
                    .cache_status()
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(Status {
                    sync_state: self
                        .last_result
                        .try_lock()
                        .map_or(SyncState::Syncing, |state| *state)
                        .as_str()
                        .into(),
                    unpinned_cache_bytes: status.unpinned_bytes,
                    pinned_cache_bytes: status.pinned_bytes,
                    free_disk_floor_warning: status.free_disk_floor_warning,
                    pending_upload_count: self
                        .engine
                        .pending_upload_count()
                        .map_err(|error| error.to_string())?,
                    paused: self.poll_state.borrow().paused,
                })
                .map_err(|error| error.to_string())
            }
            Request::Sync {} => self.sync(false).await.map(|()| Value::Null),
            Request::Pause {} | Request::Resume {} => {
                self.poll_state.send_if_modified(|state| {
                    let paused = matches!(request, Request::Pause {});
                    if paused == state.paused {
                        return false;
                    }
                    state.paused = paused;
                    if !paused {
                        state.resume_count = state.resume_count.wrapping_add(1);
                    }
                    true
                });
                Ok(Value::Null)
            }
            Request::Pin { path } => {
                self.engine
                    .pin(&self.resolve_item(&path)?)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }
            Request::Unpin { path } => {
                self.engine
                    .unpin(&self.resolve_item(&path)?)
                    .map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }
            Request::Purge { path } => {
                self.engine
                    .purge(&self.resolve_item(&path)?)
                    .map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }
            Request::Uploads {} => self.uploads(),
            Request::DesktopChanges {
                mount,
                epoch,
                since,
            } => {
                if mount != self.mount {
                    return Err("OneDrive mount does not match this daemon".into());
                }
                let changes = self
                    .engine
                    .desktop_changes(epoch.as_deref(), since)
                    .map_err(|error| error.to_string())?;
                Ok(serde_json::json!({
                    "epoch": changes.epoch,
                    "revision": changes.revision,
                    "inodes": changes.inodes,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onedrive_core::{Clock, FakeCloudDrive, ItemKind, RemoteItem};
    use tokio::time::advance;

    struct PollClock(Instant);

    #[async_trait::async_trait]
    impl Clock for PollClock {
        fn now(&self) -> Duration {
            self.0.elapsed()
        }

        async fn sleep(&self, duration: Duration) {
            tokio::time::sleep(duration).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn default_poll_pause_resume_and_manual_sync() {
        let dir = tempfile::tempdir().unwrap();
        let cloud = Arc::new(FakeCloudDrive::new(10));
        let root = RemoteItem {
            item_id: "root".into(),
            parent_id: None,
            name: None,
            kind: ItemKind::Folder,
            size: 0,
            mtime: 1,
            etag: None,
            ctag: None,
            quickxor_hash: None,
        };
        cloud.upsert(root.clone());
        cloud.upsert(RemoteItem {
            item_id: "entry".into(),
            parent_id: Some(root.item_id.clone()),
            name: Some("entry".into()),
            kind: ItemKind::File,
            size: 1,
            ..root
        });
        cloud.set_content("entry", vec![1]);
        let engine = Arc::new(
            Engine::open_with_config(
                dir.path().join("catalog"),
                dir.path().join("cache"),
                cloud.clone(),
                Arc::new(PollClock(Instant::now())),
                Config {
                    max_idle_age_days: 0,
                    min_free_disk_bytes: 0,
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        let sync = Arc::new(DeltaSync::new(engine.clone(), dir.path().join("mount")));
        let poller = tokio::spawn({
            let sync = sync.clone();
            async move {
                sync.poll(Duration::from_secs(
                    Config::default().delta_poll_interval_secs,
                ))
                .await
            }
        });

        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 1);
        let inode = engine.item("entry").unwrap().unwrap().inode;
        assert_eq!(engine.read(inode, 0, 1).await.unwrap(), vec![1]);
        advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 1);
        assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 1);
        advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 2);
        assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 0);
        assert_eq!(engine.read(inode, 0, 1).await.unwrap(), vec![1]);

        sync.dispatch(Request::Pause {}).await.unwrap();
        tokio::task::yield_now().await;
        advance(Duration::from_secs(180)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 2);
        assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 1);
        sync.dispatch(Request::Sync {}).await.unwrap();
        assert_eq!(cloud.requests().len(), 3);

        sync.dispatch(Request::Resume {}).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 4);
        advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 5);
        sync.dispatch(Request::Pause {}).await.unwrap();
        sync.dispatch(Request::Resume {}).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 6);
        poller.abort();
    }
}
