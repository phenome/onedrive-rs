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
async fn header_reads_refetch_only_when_the_partial_generation_is_evicted() {
    const HEADER: usize = 64 * 1024;
    const SNIFF: usize = 32 * 1024;
    for (cap, cached_bytes, fetches) in [(SNIFF, 0, 2), (2 * HEADER, HEADER, 1)] {
        let mut fixture = Fixture::new();
        fixture.config.max_unpinned_cache_bytes = cap as u64;
        fixture.config.max_idle_age_days = 1;
        fixture.add("file", Some("root"), ItemKind::File, 2 * HEADER);
        let content: Vec<_> = (0..2 * HEADER).map(|offset| (offset % 251) as u8).collect();
        fixture.drive.set_content("file", content.clone());
        let engine = fixture.open();
        engine.sync().await.unwrap();

        for _ in 0..2 {
            assert_eq!(
                engine.read(inode(&engine, "file"), 0, SNIFF).await.unwrap(),
                content[..SNIFF]
            );
            assert_eq!(
                engine.cache_status().unwrap().unpinned_bytes,
                cached_bytes as u64
            );
            assert_eq!(state(&engine, "file"), "Cloud-only");
            fixture.clock.advance(86_401);
            engine.evict().unwrap();
        }
        assert_eq!(
            fixture.drive.range_requests(),
            vec![("file".into(), 0, HEADER); fetches]
        );
    }
}

#[tokio::test]
async fn budget_pressure_evicts_clean_partial_ranges_and_protects_pinned_and_dirty_bytes() {
    use onedrive_core::SyncState;

    const HEADER: usize = 64 * 1024;
    let mut fixture = Fixture::new();
    fixture.config.max_unpinned_cache_bytes = HEADER as u64;
    for id in ["pinned", "dirty", "clean"] {
        fixture.add(id, Some("root"), ItemKind::File, 2 * HEADER);
    }
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("pinned").await.unwrap();
    engine.read(inode(&engine, "dirty"), 0, 1).await.unwrap();
    engine.set_sync_state("dirty", SyncState::Dirty).unwrap();
    fixture.clock.advance(1);

    assert_eq!(
        engine.read(inode(&engine, "clean"), 0, 1).await.unwrap(),
        [13]
    );
    let status = engine.cache_status().unwrap();
    assert_eq!(status.unpinned_bytes, HEADER as u64);
    assert_eq!(status.pinned_bytes, (2 * HEADER) as u64);
    assert_eq!(state(&engine, "pinned"), "Hydrated");
    assert_eq!(state(&engine, "dirty"), "Dirty");

    let requests = fixture.drive.range_requests();
    assert_eq!(
        engine.read(inode(&engine, "dirty"), 0, 1).await.unwrap(),
        [13]
    );
    assert_eq!(fixture.drive.range_requests(), requests);
    assert_eq!(
        engine.read(inode(&engine, "clean"), 0, 1).await.unwrap(),
        [13]
    );
    assert_eq!(
        &fixture.drive.range_requests()[requests.len()..],
        &[("clean".into(), 0, HEADER)]
    );
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
    assert_eq!(state(&engine, "folder"), "Hydrated");
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
    let namespace = std::fs::read_dir(fixture.temp.path().join("cache"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(std::fs::read_dir(namespace).unwrap().count(), 0);
    let requests = fixture.drive.range_requests().len();
    assert_eq!(
        engine.read(inode(&engine, "dirty"), 0, 10).await.unwrap(),
        vec![13; 10]
    );
    assert_eq!(fixture.drive.range_requests().len(), requests + 1);
}

#[tokio::test]
async fn automatic_hydration_pauses_at_free_disk_floor_and_retries() {
    let mut fixture = Fixture::new();
    fixture.add("folder", Some("root"), ItemKind::Folder, 0);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("folder").await.unwrap();
    drop(engine);

    fixture.config.min_free_disk_bytes = u64::MAX;
    fixture.add("new", Some("folder"), ItemKind::File, 10);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    assert!(engine.cache_status().unwrap().hydration_paused_low_disk);
    assert_eq!(state(&engine, "new"), "Cloud-only");
    assert_eq!(xattr(&engine, "new", "user.onedrive.pin"), "pinned");
    assert!(fixture.drive.range_requests().is_empty());
    drop(engine);

    fixture.config.min_free_disk_bytes = 0;
    let engine = fixture.open();
    engine.sync().await.unwrap();
    let status = engine.cache_status().unwrap();
    assert!(!status.hydration_paused_low_disk);
    assert_eq!(status.pinned_bytes, 10);
    assert_eq!(status.unpinned_bytes, 0);
    assert_eq!(state(&engine, "new"), "Hydrated");
}

#[tokio::test]
async fn reads_evict_lru_by_cached_bytes_and_periodic_sync_evicts_idle_overrides() {
    let mut fixture = Fixture::new();
    fixture.config.max_unpinned_cache_bytes = 20;
    fixture.add("folder", Some("root"), ItemKind::Folder, 0);
    fixture.add("pinned", Some("folder"), ItemKind::File, 50);
    fixture.add("override", Some("folder"), ItemKind::File, 10);
    for name in ["lru", "new"] {
        fixture.add(name, Some("root"), ItemKind::File, 10);
    }
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("folder").await.unwrap();
    engine.purge("override").unwrap();
    for name in ["override", "lru", "override", "new"] {
        fixture.clock.advance(1);
        assert_eq!(
            engine.read(inode(&engine, name), 0, 10).await.unwrap(),
            vec![13; 10]
        );
    }

    assert_eq!(state(&engine, "lru"), "Cloud-only");
    assert_eq!(state(&engine, "override"), "Hydrated");
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, 50);
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 20);
    engine
        .set_sync_state("new", onedrive_core::SyncState::Dirty)
        .unwrap();
    fixture.clock.advance(7 * 86400);
    engine.sync().await.unwrap();
    assert_eq!(state(&engine, "override"), "Cloud-only");
    assert_eq!(state(&engine, "pinned"), "Hydrated");
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 10);
    engine
        .set_sync_state("new", onedrive_core::SyncState::Clean)
        .unwrap();
    assert_eq!(state(&engine, "new"), "Hydrated");
}

#[tokio::test]
async fn unpin_retains_bytes_and_direct_purge_clears_pin_even_for_empty_files() {
    let fixture = Fixture::new();
    fixture.add("file", Some("root"), ItemKind::File, 10);
    fixture.add("empty", Some("root"), ItemKind::File, 0);
    let engine = fixture.open();
    engine.sync().await.unwrap();

    engine.pin("file").await.unwrap();
    engine.unpin("file").unwrap();
    assert_eq!(engine.effective_pin_state("file").unwrap(), PinState::None);
    assert_eq!(state(&engine, "file"), "Hydrated");
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 10);

    for name in ["file", "empty"] {
        engine.pin(name).await.unwrap();
        assert_eq!(state(&engine, name), "Hydrated");
        engine.purge(name).unwrap();
        assert_eq!(state(&engine, name), "Cloud-only");
        assert_eq!(engine.pin_state(name).unwrap(), PinState::None);
    }
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 0);
}

#[tokio::test]
async fn legacy_cached_content_gets_one_full_idle_window_on_open() {
    let mut fixture = Fixture::new();
    fixture.config.max_idle_age_days = 1;
    fixture.add("legacy", Some("root"), ItemKind::File, 8);
    fixture.add("cloud-only", Some("root"), ItemKind::File, 8);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.read(inode(&engine, "legacy"), 0, 8).await.unwrap();
    drop(engine);

    // Earlier sparse cache versions published ranges without recording atime.
    let db = rusqlite::Connection::open(fixture.temp.path().join("catalog.sqlite")).unwrap();
    db.execute("UPDATE items SET last_accessed_at = NULL", [])
        .unwrap();
    drop(db);

    fixture.clock.advance(1_800_000_000);
    let engine = fixture.open();
    engine.evict().unwrap();
    assert_eq!(state(&engine, "legacy"), "Hydrated");
    assert_eq!(
        engine.item("legacy").unwrap().unwrap().last_accessed_at,
        Some(1_800_000_000_000)
    );
    assert_eq!(
        engine.item("cloud-only").unwrap().unwrap().last_accessed_at,
        None
    );
    drop(engine);

    fixture.clock.advance(86_400);
    let engine = fixture.open();
    engine.evict().unwrap();
    assert_eq!(state(&engine, "legacy"), "Hydrated");
    fixture.clock.advance(1);
    engine.evict().unwrap();
    assert_eq!(state(&engine, "legacy"), "Cloud-only");
}

#[tokio::test]
async fn empty_hydration_ages_from_pin_and_evicts_after_unpin() {
    let mut fixture = Fixture::new();
    fixture.config.max_idle_age_days = 1;
    fixture.clock.advance(1_800_000_000);
    fixture.add("empty", Some("root"), ItemKind::File, 0);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.pin("empty").await.unwrap();
    engine.unpin("empty").unwrap();

    engine.evict().unwrap();
    assert_eq!(state(&engine, "empty"), "Hydrated");
    fixture.clock.advance(86_401);
    engine.evict().unwrap();
    assert_eq!(state(&engine, "empty"), "Cloud-only");
    assert!(fixture.drive.range_requests().is_empty());
}

#[tokio::test]
async fn failed_full_hydration_retry_keeps_low_disk_warning() {
    use onedrive_core::EngineError;

    let mut fixture = Fixture::new();
    fixture.config.min_free_disk_bytes = u64::MAX;
    fixture.add("file", Some("root"), ItemKind::File, 65_537);
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.read(inode(&engine, "file"), 0, 1).await.unwrap();
    assert!(matches!(
        engine.pin("file").await,
        Err(EngineError::LowDisk)
    ));
    assert!(engine.cache_status().unwrap().hydration_paused_low_disk);

    // Opening a socket as a cached range produces an I/O error before the disk check.
    let namespace = std::fs::read_dir(fixture.temp.path().join("cache"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let generation = std::fs::read_dir(namespace)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let header = generation.join("-1");
    std::fs::remove_file(&header).unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(header).unwrap();
    assert!(matches!(engine.sync().await, Err(EngineError::CacheIo(_))));
    assert!(engine.cache_status().unwrap().hydration_paused_low_disk);

    engine.purge("file").unwrap();
    engine.sync().await.unwrap();
    assert!(!engine.cache_status().unwrap().hydration_paused_low_disk);
}

#[tokio::test]
async fn failed_range_evicts_an_idle_hydrated_file_and_over_budget_partial_bytes() {
    use onedrive_core::EngineError;

    let mut fixture = Fixture::new();
    fixture.config.max_idle_age_days = 1;
    fixture.config.max_unpinned_cache_bytes = 32 * 1024;
    fixture.add("idle", Some("root"), ItemKind::File, 8);
    fixture.add(
        "interrupted",
        Some("root"),
        ItemKind::File,
        2 * 1024 * 1024 + 1,
    );
    let engine = fixture.open();
    engine.sync().await.unwrap();
    engine.read(inode(&engine, "idle"), 0, 8).await.unwrap();
    fixture.clock.advance(86_401);

    // The first block is valid while the next range is short.
    fixture
        .drive
        .set_content("interrupted", vec![13; 2 * 1024 * 1024]);
    assert!(matches!(
        engine
            .read(inode(&engine, "interrupted"), 0, 2 * 1024 * 1024 + 1)
            .await,
        Err(EngineError::InvalidRange)
    ));
    assert_eq!(state(&engine, "idle"), "Cloud-only");
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 0);
}

#[tokio::test]
async fn sticky_pin_hydrates_eligible_files_around_a_failed_range() {
    use onedrive_core::EngineError;

    let fixture = Fixture::new();
    for name in ["a", "b", "c"] {
        fixture.add(name, Some("root"), ItemKind::File, 8);
    }
    let engine = fixture.open();
    engine.sync().await.unwrap();
    fixture.drive.set_content("b", vec![]);
    assert!(matches!(
        engine.pin("root").await,
        Err(EngineError::InvalidRange)
    ));
    assert_eq!(state(&engine, "a"), "Hydrated");
    assert_eq!(state(&engine, "b"), "Cloud-only");
    assert_eq!(state(&engine, "c"), "Hydrated");
}
