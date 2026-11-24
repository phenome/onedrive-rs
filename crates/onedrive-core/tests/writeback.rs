use std::sync::Arc;

use onedrive_core::{Engine, FakeCloudDrive, ItemKind, RemoteItem, SyncState, SystemClock};

fn drive() -> Arc<FakeCloudDrive> {
    let drive = Arc::new(FakeCloudDrive::new(10));
    drive.upsert(RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        size: 0,
        mtime: 100,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    });
    drive
}

#[tokio::test]
async fn staged_bytes_survive_restart_and_upload_keeps_the_inode() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("catalog.db");
    let drive = drive();
    let engine = Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();

    let file = engine.create(1, "draft.txt", ItemKind::File).unwrap();
    engine.write(file.inode, 2, b"draft", false).await.unwrap();
    drop(engine);

    let engine = Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    let dirty = engine.resolve_path("/draft.txt").unwrap().unwrap();
    assert_eq!(dirty.inode, file.inode);
    assert_eq!(dirty.sync_state, SyncState::Dirty);
    assert_eq!(engine.read(file.inode, 0, 20).await.unwrap(), b"\0\0draft");
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);

    let uploaded = engine.resolve_path("/draft.txt").unwrap().unwrap();
    assert_eq!(uploaded.inode, file.inode);
    assert_ne!(uploaded.item_id, file.item_id);
    assert_eq!(uploaded.sync_state, SyncState::Clean);
    assert!(uploaded.etag.is_some() && uploaded.ctag.is_some());
    assert_eq!(engine.read(file.inode, 0, 20).await.unwrap(), b"\0\0draft");
}
