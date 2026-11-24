use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;

use onedrive_core::{
    CloudDrive, CloudError, Config, DeltaPage, DeltaRequest, Engine, EngineError, FakeCloudDrive,
    ItemKind, RemoteItem, SystemClock,
};
use tempfile::TempDir;

const HEADER: usize = 64 * 1024;
const STREAM: usize = 2 * 1024 * 1024;

fn fixture_file(size: usize) -> RemoteItem {
    RemoteItem {
        item_id: "file".into(),
        parent_id: Some("root".into()),
        name: Some("file.bin".into()),
        kind: ItemKind::File,
        size: size as u64,
        mtime: 1_700_000_000,
        etag: Some("\"version-1\"".into()),
        ctag: Some("content-1".into()),
        quickxor_hash: None,
    }
}

fn open(temp: &TempDir, drive: &Arc<FakeCloudDrive>) -> Engine {
    Engine::open_with_cache(
        temp.path().join("metadata.db"),
        temp.path().join("cache"),
        drive.clone(),
        Arc::new(SystemClock),
    )
    .unwrap()
}

async fn setup(size: usize) -> (TempDir, Arc<FakeCloudDrive>, Engine, u64, Vec<u8>) {
    let temp = tempfile::tempdir_in("/tmp").unwrap();
    let drive = Arc::new(FakeCloudDrive::new(100));
    drive.upsert(RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        ..fixture_file(0)
    });
    let bytes: Vec<u8> = (0..size).map(|offset| (offset % 251) as u8).collect();
    drive.upsert(fixture_file(size));
    drive.set_content("file", bytes.clone());
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let inode = engine.lookup("root", "file.bin").unwrap().unwrap().inode;
    (temp, drive, engine, inode, bytes)
}

#[tokio::test]
async fn header_sniff_fetches_only_64_kib_and_survives_engine_restart() {
    let (temp, drive, engine, inode, bytes) = setup(3 * STREAM).await;
    assert_eq!(engine.read(inode, 0, 32768).await.unwrap(), bytes[..32768]);
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
    drop(engine);
    let engine = open(&temp, &drive);
    assert_eq!(
        engine.read(inode, 40000, 123).await.unwrap(),
        bytes[40000..40123]
    );
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
}

#[tokio::test]
async fn streaming_reads_coalesce_and_hydrate_across_arbitrary_offsets() {
    let (_temp, drive, engine, inode, bytes) = setup(2 * STREAM + 37).await;
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );
    let (first, second) = tokio::join!(
        engine.read(inode, STREAM as u64 + 17, 950),
        engine.read(inode, STREAM as u64 + 300, STREAM),
    );
    assert_eq!(first.unwrap(), bytes[STREAM + 17..STREAM + 967]);
    assert_eq!(second.unwrap(), bytes[STREAM + 300..]);
    assert_eq!(
        engine.read(inode, HEADER as u64 - 5, 200).await.unwrap(),
        bytes[HEADER - 5..HEADER + 195]
    );
    assert_eq!(
        drive.range_requests(),
        [
            ("file".into(), STREAM as u64, STREAM),
            ("file".into(), 2 * STREAM as u64, STREAM),
            ("file".into(), 0, STREAM),
        ]
    );
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Hydrated"
    );
    assert_eq!(engine.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    assert_eq!(drive.range_requests().len(), 3);
}

#[tokio::test]
async fn ctag_and_mtime_changes_each_retire_cached_bytes() {
    let (temp, drive, engine, inode, bytes) = setup(8192).await;
    assert_eq!(engine.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    let mut item = fixture_file(bytes.len());
    for (version, byte) in [("ctag", 13), ("mtime", 42)] {
        if version == "ctag" {
            item.ctag = Some("content-2".into());
        } else {
            item.mtime += 1;
        }
        drive.upsert(item.clone());
        drive.set_content("file", vec![byte; bytes.len()]);
        engine.sync().await.unwrap();
        assert_eq!(
            std::fs::read_dir(cache_namespace(&temp)).unwrap().count(),
            0
        );
        assert_eq!(
            engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
            b"Cloud-only"
        );
        assert_eq!(
            engine.read(inode, 0, bytes.len()).await.unwrap(),
            vec![byte; bytes.len()]
        );
    }
    assert_eq!(drive.range_requests(), vec![("file".into(), 0, HEADER); 3]);
}

struct InFlightRange {
    fake: Arc<FakeCloudDrive>,
    pause_offset: u64,
    pause_once: AtomicBool,
    captured: Notify,
    resume: Notify,
}

impl InFlightRange {
    fn open(temp: &TempDir, fake: Arc<FakeCloudDrive>) -> (Arc<Self>, Engine) {
        Self::open_at(temp, fake, 0, Config::default().max_unpinned_cache_bytes)
    }

    fn open_at(
        temp: &TempDir,
        fake: Arc<FakeCloudDrive>,
        pause_offset: u64,
        max_unpinned_cache_bytes: u64,
    ) -> (Arc<Self>, Engine) {
        let cloud = Arc::new(Self {
            fake,
            pause_offset,
            pause_once: AtomicBool::new(true),
            captured: Notify::new(),
            resume: Notify::new(),
        });
        let engine = Engine::open_with_config(
            temp.path().join("metadata.db"),
            temp.path().join("cache"),
            cloud.clone(),
            Arc::new(SystemClock),
            Config {
                min_free_disk_bytes: 0,
                max_unpinned_cache_bytes,
                ..Config::default()
            },
        )
        .unwrap();
        (cloud, engine)
    }
}

#[async_trait]
impl CloudDrive for InFlightRange {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.fake.delta(request).await
    }

    async fn read_range_at_version(
        &self,
        item: &str,
        etag: Option<&str>,
        ctag: Option<&str>,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        let bytes = self
            .fake
            .read_range_at_version(item, etag, ctag, offset, length)
            .await?;
        if offset == self.pause_offset && self.pause_once.swap(false, Ordering::Relaxed) {
            self.captured.notify_one();
            self.resume.notified().await;
        }
        Ok(bytes)
    }
}

#[tokio::test]
async fn eviction_reclaims_over_budget_partial_bytes_after_read_cancellation() {
    let (temp, fake, engine, inode, original) = setup(STREAM + 200).await;
    drop(engine);
    let (cloud, engine) = InFlightRange::open_at(&temp, fake.clone(), STREAM as u64, 32 * 1024);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            result = engine.read(inode, 0, original.len()) => panic!("read finished early: {:?}", result.map(|bytes| bytes.len())),
            () = cloud.captured.notified() => (),
        }
    })
    .await
    .unwrap();
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, STREAM as u64);

    engine.evict().unwrap();
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 0);
    assert_eq!(engine.read(inode, 0, 1).await.unwrap(), original[..1]);
    assert_eq!(
        fake.range_requests(),
        [
            ("file".into(), 0, STREAM),
            ("file".into(), STREAM as u64, STREAM),
            ("file".into(), 0, HEADER)
        ]
    );
}

#[tokio::test]
async fn in_flight_range_retries_after_delta_retires_its_generation() {
    let (temp, fake, engine, inode, original) = setup(STREAM + 200).await;
    drop(engine);
    let (cloud, engine) = InFlightRange::open(&temp, fake.clone());
    let fresh = vec![193; original.len()];
    let apply_delta = async {
        cloud.captured.notified().await;
        let mut changed = fixture_file(original.len());
        changed.ctag = Some("content-2".into());
        fake.upsert(changed);
        fake.set_content("file", fresh.clone());
        engine.sync().await.unwrap();
        cloud.resume.notify_one();
    };
    let (bytes, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(engine.read(inode, 0, fresh.len()), apply_delta)
    })
    .await
    .unwrap();
    assert_eq!(bytes.unwrap(), fresh);
    assert_eq!(
        fake.range_requests(),
        [
            ("file".into(), 0, STREAM),
            ("file".into(), 0, STREAM),
            ("file".into(), STREAM as u64, STREAM),
        ]
    );
}

#[tokio::test]
async fn an_in_flight_pin_cannot_restore_a_purged_generation() {
    let (temp, fake, engine, inode, _) = setup(STREAM + 200).await;
    drop(engine);
    let (cloud, engine) = InFlightRange::open(&temp, fake);
    let purge = async {
        cloud.captured.notified().await;
        engine.purge("file").unwrap();
        cloud.resume.notify_one();
    };
    let (pin, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(engine.pin("file"), purge)
    })
    .await
    .unwrap();
    pin.unwrap();
    assert_eq!(
        engine.pin_state("file").unwrap(),
        onedrive_core::PinState::None
    );
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 0);
}

#[tokio::test]
async fn cache_status_counts_sparse_holes_and_overlapping_headers_correctly() {
    let (_temp, _fake, engine, inode, _) = setup(3 * STREAM + 17).await;
    engine.read(inode, 0, 1).await.unwrap();
    engine.read(inode, 3 * STREAM as u64, 1).await.unwrap();
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 65536 + 17);

    engine.read(inode, HEADER as u64, 1).await.unwrap();
    assert_eq!(engine.cache_status().unwrap().unpinned_bytes, 2097152 + 17);
}

#[tokio::test]
async fn pin_drops_an_in_flight_download_when_a_local_upload_becomes_pending() {
    use onedrive_core::{EngineError, SyncState};

    let (temp, fake, engine, inode, _) = setup(STREAM + 200).await;
    engine.read(inode, 0, 1).await.unwrap();
    drop(engine);

    let (cloud, engine) = InFlightRange::open(&temp, fake);
    let begin_writeback = async {
        cloud.captured.notified().await;
        engine.set_sync_state("file", SyncState::Dirty).unwrap();
        assert!(matches!(
            engine.purge("file"),
            Err(EngineError::PendingUpload { .. })
        ));
        cloud.resume.notify_one();
    };
    let (pin, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(engine.pin("file"), begin_writeback)
    })
    .await
    .unwrap();
    pin.unwrap();
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, 65536);
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Dirty"
    );
}

fn cache_namespace(temp: &TempDir) -> std::path::PathBuf {
    std::fs::read_dir(temp.path().join("cache"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
}

#[tokio::test]
async fn catalogs_sharing_a_cache_root_never_share_bytes_or_reclamation() {
    let (temp, first_drive, first, inode, authentic) = setup(8192).await;
    assert_eq!(
        first.read(inode, 0, authentic.len()).await.unwrap(),
        authentic
    );
    let second_drive = Arc::new(FakeCloudDrive::new(100));
    second_drive.upsert(RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        ..fixture_file(0)
    });
    let mut remote = fixture_file(authentic.len());
    second_drive.upsert(remote.clone());
    let other = vec![171; authentic.len()];
    second_drive.set_content("file", other.clone());
    let second = Engine::open_with_cache(
        temp.path().join("other.db"),
        temp.path().join("cache"),
        second_drive.clone(),
        Arc::new(SystemClock),
    )
    .unwrap();
    second.sync().await.unwrap();
    assert_eq!(
        second.lookup("root", "file.bin").unwrap().unwrap().inode,
        inode
    );
    assert_eq!(second.read(inode, 0, other.len()).await.unwrap(), other);
    remote.ctag = Some("content-2".into());
    second_drive.upsert(remote);
    second.sync().await.unwrap();
    drop(first);
    assert_eq!(
        open(&temp, &first_drive)
            .read(inode, 0, authentic.len())
            .await
            .unwrap(),
        authentic
    );
    assert_eq!(first_drive.range_requests().len(), 1);
}

#[tokio::test]
async fn missing_or_truncated_range_retires_coverage_and_recovers_authentic_bytes() {
    let (temp, drive, mut engine, inode, bytes) = setup(STREAM + 200).await;
    for (restart, remove, query_state_first) in [
        (true, true, true),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        assert_eq!(engine.read(inode, 0, bytes.len()).await.unwrap(), bytes);
        let directory = std::fs::read_dir(cache_namespace(&temp))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let damaged = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| std::fs::metadata(path).unwrap().len() == STREAM as u64)
            .unwrap();
        if remove {
            std::fs::remove_file(damaged).unwrap();
        } else {
            let handle = std::fs::OpenOptions::new()
                .write(true)
                .open(damaged)
                .unwrap();
            handle.set_len(1).unwrap();
            // A later extension creates holes and restores the original size.
            if restart {
                handle.set_len(STREAM as u64).unwrap();
            }
        }
        if restart {
            drop(engine);
            engine = open(&temp, &drive);
        }
        if query_state_first {
            assert_eq!(
                engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
                b"Cloud-only"
            );
        }
        let requests = drive.range_requests().len();
        assert_eq!(engine.read(inode, 0, 32768).await.unwrap(), bytes[..32768]);
        assert_eq!(
            drive.range_requests()[requests..],
            [("file".into(), 0, HEADER)]
        );
        assert!(!directory.exists());
        assert_eq!(
            engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
            b"Cloud-only"
        );
    }
}

#[tokio::test]
async fn restart_reclaims_an_installed_range_without_published_coverage() {
    let (temp, drive, engine, inode, bytes) = setup(STREAM + 200).await;
    assert_eq!(engine.read(inode, 0, 32768).await.unwrap(), bytes[..32768]);
    drop(engine);

    let db = rusqlite::Connection::open(temp.path().join("metadata.db")).unwrap();
    let generation: i64 = db
        .query_row(
            "SELECT generation FROM sparse_files WHERE inode = ?1",
            [inode],
            |row| row.get(0),
        )
        .unwrap();
    let block = STREAM as i64;
    db.execute(
        "INSERT INTO sparse_pending VALUES (?1, ?2)",
        [generation, block],
    )
    .unwrap();
    let directory = cache_namespace(&temp).join(format!("{generation}.sparse"));
    let abandoned = directory.join(block.to_string());
    std::fs::write(&abandoned, vec![187; 200]).unwrap();
    drop(db);

    let engine = open(&temp, &drive);
    assert!(!abandoned.exists());
    assert_eq!(engine.read(inode, 0, 32768).await.unwrap(), bytes[..32768]);
    assert_eq!(drive.range_requests(), [("file".into(), 0, HEADER)]);
    assert_eq!(
        engine.read(inode, STREAM as u64, 200).await.unwrap(),
        bytes[STREAM..]
    );
    assert_eq!(drive.range_requests().len(), 2);
}

#[tokio::test]
async fn sync_recovers_damaged_content_for_an_existing_pin() {
    let (temp, drive, engine, inode, bytes) = setup(8192).await;
    engine
        .set_pin_state("file", onedrive_core::PinState::Pinned)
        .unwrap();
    // Fill through a read so this test does not depend on the host's free-disk floor.
    engine.read(inode, 0, bytes.len()).await.unwrap();

    let generation = std::fs::read_dir(cache_namespace(&temp))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(generation.join("-1")).unwrap();
    let engine = Engine::open_with_config(
        temp.path().join("metadata.db"),
        temp.path().join("cache"),
        drive.clone(),
        Arc::new(SystemClock),
        onedrive_core::Config {
            min_free_disk_bytes: 0,
            ..onedrive_core::Config::default()
        },
    )
    .unwrap();
    engine.sync().await.unwrap();

    assert!(!generation.exists());
    assert_eq!(
        engine.cache_status().unwrap().pinned_bytes,
        bytes.len() as u64
    );
    assert_eq!(engine.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    assert_eq!(drive.range_requests().len(), 2);
}

fn open_snapshot_engine(temp: &TempDir, cloud: Arc<dyn CloudDrive>) -> Engine {
    Engine::open_with_config(
        temp.path().join("metadata.db"),
        temp.path().join("cache"),
        cloud,
        Arc::new(SystemClock),
        Config {
            min_free_disk_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap()
}

struct UnversionedDrive(Arc<FakeCloudDrive>);

#[async_trait]
impl CloudDrive for UnversionedDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.0.delta(request).await
    }

    async fn read_range(
        &self,
        item: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        self.0.read_range(item, offset, length).await
    }
}

#[tokio::test]
async fn unversioned_blocks_never_combine_or_establish_multiblock_hydration() {
    let (temp, drive, engine, inode, original) = setup(STREAM + 100).await;
    drop(engine);
    let engine = open_snapshot_engine(&temp, Arc::new(UnversionedDrive(drive.clone())));

    assert_eq!(
        engine.read(inode, 0, STREAM).await.unwrap(),
        original[..STREAM]
    );
    let replacement = vec![23; original.len()];
    drive.set_content("file", replacement.clone());
    assert_eq!(
        engine.read(inode, STREAM as u64, 100).await.unwrap(),
        replacement[STREAM..]
    );
    assert_eq!(
        engine.read(inode, 0, original.len()).await.unwrap(),
        replacement
    );
    assert_eq!(
        drive.range_requests(),
        [
            ("file".into(), 0, STREAM),
            ("file".into(), STREAM as u64, STREAM),
            ("file".into(), 0, original.len()),
        ]
    );
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );
    engine.pin("file").await.unwrap();
    assert_eq!(
        engine.xattr(inode, "user.onedrive.pin").unwrap().unwrap(),
        b"pinned"
    );
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Hydrated"
    );
    drive.set_content("file", vec![80; original.len()]);
    assert_eq!(
        engine.read(inode, 0, original.len()).await.unwrap(),
        replacement
    );
    assert_eq!(drive.range_requests().len(), 4);
}

#[tokio::test]
async fn etag_change_retires_ranges_only_without_a_stable_ctag() {
    let (_temp, drive, engine, inode, original) = setup(8192).await;
    engine.read(inode, 0, original.len()).await.unwrap();
    let mut item = fixture_file(original.len());
    item.etag = Some("metadata-only".into());
    drive.upsert(item.clone());
    engine.sync().await.unwrap();
    assert_eq!(
        engine.read(inode, 0, original.len()).await.unwrap(),
        original
    );
    assert_eq!(drive.range_requests().len(), 1);

    item.ctag = None;
    drive.upsert(item.clone());
    engine.sync().await.unwrap();
    engine.read(inode, 0, original.len()).await.unwrap();
    item.etag = Some("replacement".into());
    drive.upsert(item);
    let replacement = vec![42; original.len()];
    drive.set_content("file", replacement.clone());
    engine.sync().await.unwrap();
    assert_eq!(
        engine.read(inode, 0, original.len()).await.unwrap(),
        replacement
    );
    assert_eq!(drive.range_requests().len(), 3);
}

#[tokio::test]
async fn nonregular_cache_block_is_refetched() {
    let (temp, drive, engine, inode, bytes) = setup(8192).await;
    engine.read(inode, 0, bytes.len()).await.unwrap();
    let generation = std::fs::read_dir(cache_namespace(&temp))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let block = generation.join("-1");
    std::fs::remove_file(&block).unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(block).unwrap();
    assert_eq!(engine.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    assert_eq!(drive.range_requests().len(), 2);
}

#[tokio::test]
async fn upgrade_retires_stale_etag_only_ranges() {
    for recorded_version in [true, false] {
        let (temp, drive, engine, inode, original) = setup(8192).await;
        let mut item = fixture_file(original.len());
        item.ctag = None;
        drive.upsert(item.clone());
        engine.sync().await.unwrap();
        engine.read(inode, 0, original.len()).await.unwrap();
        drop(engine);

        item.etag = Some("replacement".into());
        let db = rusqlite::Connection::open(temp.path().join("metadata.db")).unwrap();
        db.execute_batch(
            "PRAGMA user_version = 0; DROP TRIGGER sparse_invalidate_on_content_change;",
        )
        .unwrap();
        db.execute(
            "UPDATE items SET etag = ?1 WHERE inode = ?2",
            rusqlite::params![item.etag, inode],
        )
        .unwrap();
        if !recorded_version {
            db.execute("DELETE FROM sparse_range_versions", []).unwrap();
        }
        drop(db);
        drive.upsert(item);
        let replacement = vec![42; original.len()];
        drive.set_content("file", replacement.clone());
        let engine = open(&temp, &drive);
        assert_eq!(
            engine.read(inode, 0, original.len()).await.unwrap(),
            replacement
        );
        assert_eq!(drive.range_requests().len(), 2);
    }
}

#[tokio::test]
async fn large_snapshot_without_streaming_reports_unsupported() {
    let (temp, drive, engine, inode, _) = setup(2 * STREAM + 1).await;
    drop(engine);
    let engine = open_snapshot_engine(&temp, Arc::new(UnversionedDrive(drive.clone())));

    assert!(matches!(
        engine.pin("file").await,
        Err(onedrive_core::EngineError::Cloud(CloudError::Unsupported))
    ));
    assert!(drive.range_requests().is_empty());
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );
}

struct PausedSnapshot {
    drive: Arc<FakeCloudDrive>,
    staged: Arc<Notify>,
    finish: Arc<Notify>,
}

#[async_trait]
impl CloudDrive for PausedSnapshot {
    async fn read_range(
        &self,
        item: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        self.drive.read_range(item, offset, length).await
    }

    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.drive.delta(request).await
    }

    async fn stream_snapshot(
        &self,
        _: &str,
        size: u64,
    ) -> Result<onedrive_core::SnapshotStream, CloudError> {
        use bytes::Bytes;
        use futures_util::{StreamExt, stream};

        let staged = self.staged.clone();
        let finish = self.finish.clone();
        let fill = snapshot_fill(size as usize);
        let chunk = Bytes::from(vec![fill; HEADER]);
        let full_block = stream::iter((0..STREAM / HEADER).map(move |_| Ok(chunk.clone())));
        Ok(Box::pin(full_block.chain(stream::once(async move {
            staged.notify_one();
            finish.notified().await;
            Ok(Bytes::from(vec![fill; size as usize - STREAM]))
        }))))
    }
}

fn snapshot_fill(size: usize) -> u8 {
    (size % 251) as u8
}

impl PausedSnapshot {
    fn open(temp: &TempDir, drive: Arc<FakeCloudDrive>) -> (Arc<Self>, Arc<Engine>) {
        let cloud = Arc::new(Self {
            drive,
            staged: Arc::new(Notify::new()),
            finish: Arc::new(Notify::new()),
        });
        let engine = Arc::new(open_snapshot_engine(temp, cloud.clone()));
        (cloud, engine)
    }

    async fn wait_until_staged(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.staged.notified())
            .await
            .expect("snapshot did not stage its first block");
    }
}

#[tokio::test]
async fn paused_pin_snapshot_allows_header_and_unrelated_reads() {
    let (temp, drive, original, inode, bytes) = setup(STREAM + 1).await;
    let mut other = fixture_file(1);
    other.item_id = "other".into();
    other.name = Some("other.bin".into());
    drive.upsert(other);
    drive.set_content("other", vec![99]);
    drop(original);

    let (cloud, engine) = PausedSnapshot::open(&temp, drive.clone());
    engine.sync_catalog().await.unwrap();
    let other_inode = engine.lookup("root", "other.bin").unwrap().unwrap().inode;
    let worker = engine.clone();
    let pin = tokio::spawn(async move { worker.pin("file").await });
    cloud.wait_until_staged().await;

    let (header, unrelated) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(engine.read(inode, 0, 1), engine.read(other_inode, 0, 1))
    })
    .await
    .expect("reads should finish while the snapshot stream is stopped");
    assert_eq!(header.unwrap(), bytes[..1]);
    assert_eq!(unrelated.unwrap(), [99]);
    let mut requests = drive.range_requests();
    requests.sort();
    assert_eq!(
        requests,
        [("file".into(), 0, HEADER), ("other".into(), 0, HEADER)]
    );
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, HEADER as u64);
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );

    cloud.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), pin)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.read(inode, 0, bytes.len()).await.unwrap(),
        vec![snapshot_fill(bytes.len()); bytes.len()]
    );
    assert_eq!(drive.range_requests().len(), 2);
}

#[tokio::test]
async fn pin_retries_a_snapshot_retired_by_a_changed_size_delta() {
    let (temp, drive, original, inode, _) = setup(STREAM + 1).await;
    drop(original);
    let (cloud, engine) = PausedSnapshot::open(&temp, drive.clone());
    let worker = engine.clone();
    let pin = tokio::spawn(async move { worker.pin("file").await });
    cloud.wait_until_staged().await;

    let size = STREAM + 17;
    drive.upsert(fixture_file(size));
    engine.sync_catalog().await.unwrap();
    cloud.finish.notify_one();
    cloud.wait_until_staged().await;
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, 0);

    cloud.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), pin)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Hydrated"
    );
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, size as u64);
    assert_eq!(
        engine.read(inode, 0, size).await.unwrap(),
        vec![snapshot_fill(size); size]
    );
    assert!(drive.range_requests().is_empty());
    assert_eq!(
        std::fs::read_dir(cache_namespace(&temp)).unwrap().count(),
        1
    );
}

#[tokio::test]
async fn pin_reports_incomplete_hydration_when_delta_keeps_retiring_snapshots() {
    let (temp, drive, original, inode, _) = setup(STREAM + 1).await;
    drop(original);
    let (cloud, engine) = PausedSnapshot::open(&temp, drive.clone());
    let worker = engine.clone();
    let pin = tokio::spawn(async move { worker.pin("file").await });

    for size in STREAM + 2..=STREAM + 4 {
        cloud.wait_until_staged().await;
        drive.upsert(fixture_file(size));
        engine.sync_catalog().await.unwrap();
        cloud.finish.notify_one();
    }

    let result = tokio::time::timeout(Duration::from_secs(5), pin)
        .await
        .expect("Pin should stop retrying after repeated retirement")
        .unwrap();
    assert!(matches!(
        result,
        Err(EngineError::PinHydrationSuperseded { item_id }) if item_id == "file"
    ));
    assert_eq!(
        engine.pin_state("file").unwrap(),
        onedrive_core::PinState::Pinned
    );
    assert_eq!(
        engine.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
        b"Cloud-only"
    );
    assert_eq!(engine.cache_status().unwrap().pinned_bytes, 0);
    assert!(
        std::fs::read_dir(cache_namespace(&temp))
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn snapshot_is_invisible_until_complete_and_discarded_on_invalidation_or_cancel() {
    for action in ["purge", "cancel"] {
        let (temp, drive, original, inode, _) = setup(STREAM + 1).await;
        drop(original);
        let (cloud, engine) = PausedSnapshot::open(&temp, drive.clone());
        let pin = tokio::spawn(async move { engine.pin("file").await });
        cloud.wait_until_staged().await;

        // Reopening the catalog runs crash recovery even while Pin is active.
        let peer = open(&temp, &drive);
        let files = || {
            std::fs::read_dir(cache_namespace(&temp))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>()
        };
        let paths = files();
        assert_eq!(paths.len(), 1);
        assert_eq!(
            std::fs::metadata(paths[0].join("0")).unwrap().len(),
            STREAM as u64
        );
        assert_eq!(peer.cache_status().unwrap().pinned_bytes, 0);
        assert_eq!(
            peer.xattr(inode, "user.onedrive.state").unwrap().unwrap(),
            b"Cloud-only"
        );

        match action {
            "purge" => peer.purge("file").unwrap(),
            _ => pin.abort(),
        }
        cloud.finish.notify_one();
        match pin.await {
            Ok(result) => result.unwrap(),
            Err(error) => assert!(error.is_cancelled() && action == "cancel"),
        }
        assert!(files().is_empty(), "{action} left staged bytes");
        assert_eq!(peer.cache_status().unwrap().pinned_bytes, 0);
    }
}

#[tokio::test]
async fn snapshot_requires_space_for_all_staged_blocks_before_consuming_the_body() {
    let (temp, drive, original, _, _) = setup(1).await;
    let space = original.cache_capacity().unwrap();
    let size = space.f_bavail * space.f_frsize + STREAM as u64;
    drive.upsert(RemoteItem {
        size,
        ..fixture_file(1)
    });
    original.sync_catalog().await.unwrap();
    drop(original);
    let engine = Engine::open_with_cache(
        temp.path().join("metadata.db"),
        temp.path().join("cache"),
        Arc::new(PausedSnapshot {
            drive,
            staged: Arc::new(Notify::new()),
            finish: Arc::new(Notify::new()),
        }),
        Arc::new(SystemClock),
    )
    .unwrap();

    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), engine.pin("file"))
            .await
            .unwrap(),
        Err(onedrive_core::EngineError::LowDisk)
    ));
    assert!(engine.cache_status().unwrap().hydration_paused_low_disk);
    assert!(
        std::fs::read_dir(cache_namespace(&temp))
            .unwrap()
            .next()
            .is_none()
    );
}
