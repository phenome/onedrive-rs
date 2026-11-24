use std::env;
use std::fs::{self, DirBuilder, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use onedrive_core::{CloudDrive, Config, Engine, FuseAdapter, SystemClock};
use rustix::fs::{FlockOperation, flock};
use serde_json::Value;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use crate::ipc::{IpcServer, Request, Status, serve_connection, socket_path};

const MAX_CONNECTIONS: usize = 32;

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
    let state = xdg_home("XDG_STATE_HOME", ".local/state")?.join("onedrive-rs");
    let cache = xdg_home("XDG_CACHE_HOME", ".cache")?.join("onedrive-rs");
    let period = Duration::from_secs(config.delta_poll_interval_secs);
    if period.is_zero() || Instant::now().checked_add(period).is_none() {
        return Err("delta_poll_interval_secs must be a positive, representable duration".into());
    }

    let server = IpcServer::bind(&socket_path(&runtime))
        .map_err(|error| format!("Cannot bind OneDrive IPC socket ({error})"))?;
    let listener = server.async_listener().map_err(|error| error.to_string())?;
    let _state_lock = prepare_storage(&state, &cache)?;
    let engine = Arc::new(
        Engine::open_with_cache(
            state.join("catalog.sqlite3"),
            cache,
            cloud,
            Arc::new(SystemClock),
        )
        .map_err(|error| format!("Cannot open OneDrive catalog at {state:?} ({error})"))?,
    );
    prepare_mount(&mount)
        .map_err(|error| format!("Cannot prepare mount at {mount:?} ({error})"))?;
    let (shutdown, fuse_shutdown) = watch::channel(());
    let mounted = FuseAdapter::new(engine.clone(), tokio::runtime::Handle::current())
        .with_shutdown(fuse_shutdown)
        .mount(&mount)
        .map_err(|error| format!("Cannot mount OneDrive at {mount:?} ({error})"))?;

    let delta = Arc::new(DeltaSync::new(engine, config.min_free_disk_bytes));
    let mut clients = JoinSet::new();
    let result = tokio::select! {
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
        _ = delta.poll(period) => Ok(()),
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
    clients.shutdown().await;
    drop(shutdown);
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
    }
    let lock = File::open(state).map_err(|error| format!("Cannot open {state:?} ({error})"))?;
    flock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|error| format!("Cannot lock OneDrive state at {state:?} ({error})"))?;
    Ok(lock)
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

struct DeltaSync {
    engine: Arc<Engine>,
    min_free_disk_bytes: u64,
    paused: watch::Sender<bool>,
    last_result: Mutex<&'static str>,
}

impl DeltaSync {
    fn new(engine: Arc<Engine>, min_free_disk_bytes: u64) -> Self {
        Self {
            engine,
            min_free_disk_bytes,
            paused: watch::channel(false).0,
            last_result: Mutex::new("starting"),
        }
    }

    async fn sync(&self, background: bool) -> Result<(), String> {
        let mut result = self.last_result.lock().await;
        if background && *self.paused.borrow() {
            return Ok(());
        }
        let synced = self.engine.sync().await;
        *result = if synced.is_ok() { "idle" } else { "error" };
        synced.map_err(|error| error.to_string())
    }

    async fn poll(&self, period: Duration) {
        let mut paused = self.paused.subscribe();
        loop {
            let _ = paused.wait_for(|paused| !paused).await;
            if let Err(error) = self.sync(true).await {
                eprintln!("OneDrive delta sync failed ({error})");
            }
            let next = Instant::now() + period;
            tokio::select! {
                _ = sleep_until(next) => {}
                _ = paused.wait_for(|paused| *paused) => {}
            }
        }
    }

    async fn dispatch(&self, request: Request) -> Result<Value, String> {
        match request {
            Request::Status {} => {
                let status = self
                    .engine
                    .sparse_cache_status(self.min_free_disk_bytes)
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(Status {
                    sync_state: self
                        .last_result
                        .try_lock()
                        .map_or("syncing", |state| *state)
                        .into(),
                    unpinned_cache_bytes: status.unpinned_bytes,
                    pinned_cache_bytes: status.pinned_bytes,
                    free_disk_floor_warning: status.below_free_disk_floor,
                    pending_upload_count: status.pending_uploads,
                    paused: *self.paused.borrow(),
                })
                .map_err(|error| error.to_string())
            }
            Request::Sync {} => self.sync(false).await.map(|()| Value::Null),
            Request::Pause {} | Request::Resume {} => {
                self.paused.send_if_modified(|paused| {
                    let next = matches!(request, Request::Pause {});
                    let changed = *paused != next;
                    *paused = next;
                    changed
                });
                Ok(Value::Null)
            }
            Request::Pin { .. } | Request::Unpin { .. } | Request::Purge { .. } => {
                Err("Cache retention commands are unavailable in this build".into())
            }
            Request::Uploads {} => Err("The upload queue is unavailable in this build".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onedrive_core::FakeCloudDrive;
    use tokio::time::advance;

    #[tokio::test(start_paused = true)]
    async fn default_poll_pause_resume_and_manual_sync() {
        let dir = tempfile::tempdir().unwrap();
        let cloud = Arc::new(FakeCloudDrive::new(10));
        let engine = Arc::new(
            Engine::open(
                dir.path().join("catalog"),
                cloud.clone(),
                Arc::new(SystemClock),
            )
            .unwrap(),
        );
        let sync = Arc::new(DeltaSync::new(engine, 0));
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
        advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 1);
        advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 2);

        sync.dispatch(Request::Pause {}).await.unwrap();
        tokio::task::yield_now().await;
        advance(Duration::from_secs(180)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 2);
        sync.dispatch(Request::Sync {}).await.unwrap();
        assert_eq!(cloud.requests().len(), 3);

        sync.dispatch(Request::Resume {}).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 4);
        advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(cloud.requests().len(), 5);
        poller.abort();
    }
}
