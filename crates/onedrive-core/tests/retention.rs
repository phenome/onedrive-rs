use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use onedrive_core::{Clock, Config, Engine, FakeCloudDrive, ItemKind, PinState, RemoteItem};
use tempfile::TempDir;

#[derive(Default)]
struct FakeClock(AtomicU64);

#[async_trait::async_trait]
impl Clock for FakeClock {
    fn now(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::Relaxed))
    }

    async fn sleep(&self, duration: Duration) {
        self.advance(duration.as_secs());
    }
}

impl FakeClock {
    fn advance(&self, secs: u64) {
        self.0.fetch_add(secs, Ordering::Relaxed);
    }
}

struct Fixture {
    temp: TempDir,
    drive: Arc<FakeCloudDrive>,
    clock: Arc<FakeClock>,
    config: Config,
}

impl Fixture {
    fn new() -> Self {
        let fixture = Self {
            temp: TempDir::new().unwrap(),
            drive: Arc::new(FakeCloudDrive::new(2)),
            clock: Arc::default(),
            config: Config {
                min_free_disk_bytes: 0,
                ..Config::default()
            },
        };
        fixture.add("root", None, ItemKind::Folder, 0);
        fixture
    }

    fn add(&self, id: &str, parent: Option<&str>, kind: ItemKind, size: usize) {
        self.drive.upsert(RemoteItem {
            item_id: id.into(),
            parent_id: parent.map(str::to_owned),
            name: Some(id.into()),
            kind,
            size: size as u64,
            mtime: 1,
            etag: None,
            ctag: None,
            quickxor_hash: None,
        });
        if kind == ItemKind::File {
            self.drive.set_content(id, vec![13; size]);
        }
    }

    fn open(&self) -> Engine {
        Engine::open_with_config(
            self.temp.path().join("catalog.sqlite"),
            self.temp.path().join("cache"),
            self.drive.clone(),
            self.clock.clone(),
            self.config.clone(),
        )
        .unwrap()
    }
}

fn inode(engine: &Engine, id: &str) -> u64 {
    engine.item(id).unwrap().unwrap().inode
}

fn xattr(engine: &Engine, id: &str, name: &str) -> String {
    let bytes = engine.xattr(inode(engine, id), name).unwrap().unwrap();
    String::from_utf8(bytes).unwrap()
}

fn state(engine: &Engine, id: &str) -> String {
    xattr(engine, id, "user.onedrive.state")
}

#[tokio::test]
async fn pin_hydrates_full_file_and_survives_restart_and_eviction() {
    let mut fixture = Fixture::new();
    fixture.config.max_unpinned_cache_bytes = 0;
    let size = 2 * 1024 * 1024 + 3;
    fixture.add("file", Some("root"), ItemKind::File, size);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    assert_eq!(state(&engine, "file"), "Cloud-only");
    engine.pin("file").await.unwrap();
    drop(engine);

    fixture.clock.advance(8 * 86400);
    let engine = fixture.open();
    let requests = fixture.drive.range_requests();
    engine.evict().unwrap();
    assert_eq!(engine.pin_state("file").unwrap(), PinState::Pinned);
    assert_eq!(xattr(&engine, "file", "user.onedrive.pin"), "pinned");
    assert_eq!(state(&engine, "file"), "Hydrated");
    assert_eq!(
        engine.read(inode(&engine, "file"), 0, size).await.unwrap(),
        vec![13; size]
    );
    assert_eq!(fixture.drive.range_requests(), requests);
}
