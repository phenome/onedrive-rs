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

#[tokio::test]
async fn purge_inherited_file_persists_override_and_preserves_siblings() {
    let fixture = Fixture::new();
    fixture.add("folder", Some("root"), ItemKind::Folder, 0);
    fixture.add("nested", Some("folder"), ItemKind::Folder, 0);
    fixture.add("file", Some("nested"), ItemKind::File, 10);
    fixture.add("sibling", Some("folder"), ItemKind::File, 20);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("folder").await.unwrap();
    assert_eq!(state(&engine, "file"), "Hydrated");
    assert_eq!(xattr(&engine, "file", "user.onedrive.pin"), "pinned");
    engine.purge("file").unwrap();
    assert_eq!(state(&engine, "file"), "Cloud-only");
    assert_eq!(engine.pin_state("file").unwrap(), PinState::UnpinOverride);
    drop(engine);

    let engine = fixture.open();
    fixture.add("new", Some("nested"), ItemKind::File, 30);
    engine.sync().await.unwrap();
    assert_eq!(state(&engine, "new"), "Hydrated");
    assert_eq!(state(&engine, "sibling"), "Hydrated");
    assert_eq!(state(&engine, "file"), "Cloud-only");
    assert_eq!(
        xattr(&engine, "file", "user.onedrive.pin"),
        "unpin_override"
    );
    assert_eq!(
        fixture
            .drive
            .range_requests()
            .iter()
            .filter(|r| r.0 == "file")
            .count(),
        1
    );
}

#[tokio::test]
async fn purge_folder_is_atomic_on_pending_upload_then_clears_descendant_pins() {
    use onedrive_core::{EngineError, SyncState};

    let fixture = Fixture::new();
    fixture.add("folder", Some("root"), ItemKind::Folder, 0);
    fixture.add("dirty", Some("folder"), ItemKind::File, 10);
    fixture.add("clean", Some("folder"), ItemKind::File, 20);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("folder").await.unwrap();
    engine.pin("clean").await.unwrap();
    engine
        .set_sync_state("dirty", SyncState::Uploading)
        .unwrap();
    for target in ["dirty", "folder"] {
        assert!(
            matches!(engine.purge(target), Err(EngineError::PendingUpload { item_id }) if item_id == "dirty")
        );
    }
    assert_eq!(state(&engine, "dirty"), "Dirty");
    assert_eq!(state(&engine, "clean"), "Hydrated");
    assert_eq!(engine.pin_state("folder").unwrap(), PinState::Pinned);

    engine.set_sync_state("dirty", SyncState::Clean).unwrap();
    engine.purge("folder").unwrap();
    for id in ["folder", "dirty", "clean"] {
        assert_eq!(engine.pin_state(id).unwrap(), PinState::None);
        assert_eq!(state(&engine, id), "Cloud-only");
    }
    let requests = fixture.drive.range_requests().len();
    assert_eq!(
        engine.read(inode(&engine, "dirty"), 0, 10).await.unwrap(),
        vec![13; 10]
    );
    assert_eq!(fixture.drive.range_requests().len(), requests + 1);
}
