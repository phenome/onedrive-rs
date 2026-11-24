use std::sync::Arc;

use onedrive_core::{Engine, FakeCloudDrive, ItemKind, RemoteItem, SystemClock};
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
