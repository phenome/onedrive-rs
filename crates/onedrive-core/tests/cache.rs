use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::Notify;

use onedrive_core::{
    CloudDrive, CloudError, Config, DeltaPage, DeltaRequest, Engine, FakeCloudDrive, ItemKind,
    RemoteItem, SystemClock,
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
            std::fs::read_dir(temp.path().join("cache"))
                .unwrap()
                .count(),
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
    pause_once: AtomicBool,
    captured: Notify,
    resume: Notify,
}

impl InFlightRange {
    fn open(temp: &TempDir, fake: Arc<FakeCloudDrive>) -> (Arc<Self>, Engine) {
        let cloud = Arc::new(Self {
            fake,
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

    async fn read_range(
        &self,
        item: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        let bytes = self.fake.read_range(item, offset, length).await?;
        if self.pause_once.swap(false, Ordering::Relaxed) {
            self.captured.notify_one();
            self.resume.notified().await;
        }
        Ok(bytes)
    }
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
