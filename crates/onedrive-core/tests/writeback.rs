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
    drive.set_online(false);
    assert_eq!(engine.read(file.inode, 0, 20).await.unwrap(), b"\0\0draft");
}

#[tokio::test]
async fn conditional_upload_preserves_remote_and_local_versions() {
    let dir = tempfile::tempdir().unwrap();
    let drive = drive();
    let mut remote = RemoteItem {
        item_id: "document".into(),
        parent_id: Some("root".into()),
        name: Some("report.txt".into()),
        kind: ItemKind::File,
        size: 4,
        mtime: 100,
        etag: Some("original".into()),
        ctag: Some("content-1".into()),
        quickxor_hash: None,
    };
    drive.upsert(remote.clone());
    drive.set_content("document", b"base".to_vec());
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let editor = engine.item("document").unwrap().unwrap().inode;
    engine.write(editor, 1, b"EDIT", false).await.unwrap();

    remote.etag = Some("new-remote".into());
    remote.ctag = Some("content-2".into());
    remote.mtime += 1;
    remote.size = 6;
    drive.upsert(remote);
    drive.set_content("document", b"theirs".to_vec());
    engine.sync().await.unwrap();

    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
    let original = engine.resolve_path("/report.txt").unwrap().unwrap();
    assert_eq!(original.item_id, "document");
    assert_eq!(engine.read(original.inode, 0, 20).await.unwrap(), b"theirs");
    let copy = engine.getattr(editor).unwrap().unwrap();
    assert!(copy.name.starts_with("report (conflict "));
    assert!(copy.name.ends_with(").txt"));
    assert_eq!(copy.sync_state, SyncState::Conflict);
    assert_eq!(
        engine.xattr(copy.inode, "user.onedrive.state").unwrap(),
        Some(b"Conflict".to_vec())
    );
    assert_eq!(engine.read(copy.inode, 0, 20).await.unwrap(), b"bEDIT");
    assert_eq!(
        onedrive_core::CloudDrive::read_range(&*drive, &copy.item_id, 0, 20)
            .await
            .unwrap(),
        b"bEDIT"
    );
}

#[tokio::test]
async fn moving_a_child_then_removing_the_old_folder_preserves_the_child_remotely() {
    let dir = tempfile::tempdir().unwrap();
    let drive = drive();
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let folder = engine.create(1, "old", ItemKind::Folder).unwrap();
    let file = engine.create(folder.inode, "file", ItemKind::File).unwrap();
    engine
        .write(file.inode, 0, b"keep me", false)
        .await
        .unwrap();
    engine.drain_uploads().await.unwrap();

    engine
        .rename(folder.inode, "file", 1, "saved", false)
        .unwrap();
    engine.unlink(1, "old", ItemKind::Folder).unwrap();
    engine.drain_uploads().await.unwrap();
    let saved = engine.resolve_path("/saved").unwrap().unwrap();
    assert_eq!(saved.sync_state, SyncState::Clean);
    assert_eq!(
        onedrive_core::CloudDrive::read_range(&*drive, &saved.item_id, 0, 20)
            .await
            .unwrap(),
        b"keep me"
    );
}

#[tokio::test]
async fn an_edit_during_first_upload_reuses_the_acknowledged_cloud_id() {
    use onedrive_core::{CloudDrive, CloudError, DeltaPage, DeltaRequest, UploadTarget};
    use tokio::sync::Notify;

    struct PausedDrive {
        inner: Arc<FakeCloudDrive>,
        started: Notify,
        continue_upload: Notify,
    }

    #[async_trait::async_trait]
    impl CloudDrive for PausedDrive {
        async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
            self.inner.delta(request).await
        }

        async fn upload(
            &self,
            target: UploadTarget<'_>,
            bytes: &[u8],
            etag: Option<&str>,
        ) -> Result<RemoteItem, CloudError> {
            if matches!(target, UploadTarget::New { .. }) {
                self.started.notify_one();
                self.continue_upload.notified().await;
            }
            self.inner.upload(target, bytes, etag).await
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let drive = Arc::new(PausedDrive {
        inner: drive(),
        started: Notify::new(),
        continue_upload: Notify::new(),
    });
    let engine = Arc::new(Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap());
    engine.sync().await.unwrap();
    let file = engine.create(1, "draft", ItemKind::File).unwrap();
    engine.write(file.inode, 0, b"draft", false).await.unwrap();

    let uploader = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_uploads().await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), drive.started.notified())
        .await
        .unwrap();
    engine.write(file.inode, 0, b"final", false).await.unwrap();
    engine.rename(1, "draft", 1, "finished", false).unwrap();
    assert_eq!(engine.read(file.inode, 0, 10).await.unwrap(), b"final");
    drive.continue_upload.notify_one();
    // The fake forwards content uploads and pauses the move by rejecting it.
    assert!(matches!(
        uploader.await.unwrap(),
        Err(onedrive_core::EngineError::Cloud(CloudError::Unsupported))
    ));
    drop(engine);

    let engine = Engine::open(&db, drive.inner.clone(), Arc::new(SystemClock)).unwrap();
    let intermediate = engine.resolve_path("/finished").unwrap().unwrap();
    assert_ne!(intermediate.item_id, file.item_id);
    assert_eq!(intermediate.sync_state, SyncState::Dirty);
    assert_eq!(
        drive
            .inner
            .read_range(&intermediate.item_id, 0, 10)
            .await
            .unwrap(),
        b"final"
    );
    engine.drain_uploads().await.unwrap();
    engine.sync().await.unwrap();
    let items = engine.list_children("root").unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name, "finished");
    assert_eq!(items[0].inode, file.inode);
    assert_eq!(items[0].item_id, intermediate.item_id);
    assert_eq!(items[0].sync_state, SyncState::Clean);
}

#[tokio::test]
async fn offline_rename_cycle_uploads_both_names_and_keeps_both_items() {
    let dir = tempfile::tempdir().unwrap();
    let drive = drive();
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let a = engine.create(1, "a", ItemKind::File).unwrap();
    let b = engine.create(1, "b", ItemKind::File).unwrap();
    engine.write(a.inode, 0, b"alpha", false).await.unwrap();
    engine.write(b.inode, 0, b"beta", false).await.unwrap();
    engine.drain_uploads().await.unwrap();

    engine.rename(1, "a", 1, "temp", false).unwrap();
    engine.rename(1, "b", 1, "a", false).unwrap();
    engine.rename(1, "temp", 1, "b", false).unwrap();
    engine.drain_uploads().await.unwrap();

    let observer = Engine::open(dir.path().join("observer"), drive, Arc::new(SystemClock)).unwrap();
    observer.sync().await.unwrap();
    assert_eq!(observer.list_children("root").unwrap().len(), 2);
    for (path, expected) in [("/a", b"beta".as_slice()), ("/b", b"alpha".as_slice())] {
        let item = observer.resolve_path(path).unwrap().unwrap();
        assert_eq!(observer.read(item.inode, 0, 20).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn remote_create_at_a_dirty_path_leaves_the_staged_version_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let drive = drive();
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let local = engine.create(1, "draft.txt", ItemKind::File).unwrap();
    engine.write(local.inode, 0, b"local", false).await.unwrap();
    drive.upsert(RemoteItem {
        item_id: "remote".into(),
        parent_id: Some("root".into()),
        name: Some("draft.txt".into()),
        kind: ItemKind::File,
        size: 6,
        mtime: 100,
        etag: Some("external-version".into()),
        ctag: Some("external-content".into()),
        quickxor_hash: None,
    });
    drive.set_content("remote", b"remote".to_vec());
    engine.sync().await.unwrap();

    let entries = engine.list_children("root").unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.name == "draft.txt")
            .count(),
        1
    );
    assert_eq!(
        engine.resolve_path("/draft.txt").unwrap().unwrap().inode,
        local.inode
    );
    engine.drain_uploads().await.unwrap();
    let original = engine.resolve_path("/draft.txt").unwrap().unwrap();
    assert_eq!(engine.read(original.inode, 0, 20).await.unwrap(), b"remote");
    assert_eq!(engine.read(local.inode, 0, 20).await.unwrap(), b"local");
    assert_eq!(engine.list_children("root").unwrap().len(), 2);
}
