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
    assert_eq!(uploaded.mtime, 1_700_000_002);
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
    assert_eq!(engine.read(editor, 0, 4).await.unwrap(), b"base");
    let calls = drive.range_requests();
    engine.write(editor, 1, b"EDIT", false).await.unwrap();
    assert_eq!(drive.range_requests(), calls);

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
    assert_eq!(engine.pending_upload_count().unwrap(), 0);
    assert_eq!(
        engine.pending_upload_paths().unwrap(),
        vec![(
            std::path::Path::new("/").join(&copy.name),
            SyncState::Conflict
        )]
    );
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
        engine.resolve_path("/draft.txt").unwrap().unwrap().item_id,
        "remote"
    );
    assert_eq!(
        engine
            .resolve_path("/draft (conflict).txt")
            .unwrap()
            .unwrap()
            .inode,
        local.inode
    );
    engine.drain_uploads().await.unwrap();
    let original = engine.resolve_path("/draft.txt").unwrap().unwrap();
    assert_eq!(engine.read(original.inode, 0, 20).await.unwrap(), b"remote");
    assert_eq!(engine.read(local.inode, 0, 20).await.unwrap(), b"local");
    assert_eq!(engine.list_children("root").unwrap().len(), 2);
}

struct InterruptedDrive {
    pause_move: std::sync::atomic::AtomicBool,
    peer_before_move: std::sync::atomic::AtomicBool,
    inner: Arc<FakeCloudDrive>,
    pause: std::sync::atomic::AtomicBool,
    accepted: tokio::sync::Notify,
}

impl InterruptedDrive {
    async fn after_acceptance(&self) {
        if self.pause.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.accepted.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

use onedrive_core::{
    CloudDrive, CloudError, DeltaPage, DeltaRequest, EngineError, UploadSource, UploadTarget,
};

#[async_trait::async_trait]
impl CloudDrive for InterruptedDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.inner.delta(request).await
    }

    async fn upload_from(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        etag: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        if source.len() > 1024 * 1024 * 1024 {
            let UploadSource::File { file, size } = source else {
                panic!("large upload should use the seekable staging file");
            };
            use std::os::unix::fs::MetadataExt;
            assert!(file.metadata().unwrap().blocks() * 512 < 1024 * 1024);
            assert_eq!(source.read(size - 4, 4).await?, [0; 4]);
            return Err(CloudError::Unavailable);
        }
        let remote = self.inner.upload_from(target, source, etag).await?;
        self.after_acceptance().await;
        Ok(remote)
    }

    async fn create_folder(&self, parent_id: &str, name: &str) -> Result<RemoteItem, CloudError> {
        let remote = self.inner.create_folder(parent_id, name).await?;
        self.after_acceptance().await;
        Ok(remote)
    }

    async fn read_range(
        &self,
        id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        self.inner.read_range(id, offset, length).await
    }

    async fn read_range_at_version(
        &self,
        id: &str,
        etag: Option<&str>,
        ctag: Option<&str>,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        self.inner
            .read_range_at_version(id, etag, ctag, offset, length)
            .await
    }

    async fn upload_destination(
        &self,
        target: UploadTarget<'_>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        self.inner.upload_destination(target).await
    }

    async fn delete_if_match(&self, id: &str, etag: &str) -> Result<(), CloudError> {
        self.inner.delete_if_match(id, etag).await?;
        self.after_acceptance().await;
        Ok(())
    }

    async fn move_if_match(
        &self,
        id: &str,
        parent_id: &str,
        name: &str,
        etag: &str,
    ) -> Result<RemoteItem, CloudError> {
        if self
            .peer_before_move
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .upload(UploadTarget::Existing { item_id: id }, b"peer", None)
                .await?;
        }
        let remote = self.inner.move_if_match(id, parent_id, name, etag).await?;
        if self
            .pause_move
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.after_acceptance().await;
        Ok(remote)
    }
}

fn interrupted_drive(inner: Arc<FakeCloudDrive>, pause: bool) -> Arc<InterruptedDrive> {
    Arc::new(InterruptedDrive {
        pause_move: false.into(),
        peer_before_move: false.into(),
        inner,
        pause: pause.into(),
        accepted: tokio::sync::Notify::new(),
    })
}

async fn cancel_after_acceptance(engine: Arc<Engine>, drive: &InterruptedDrive) {
    let task = tokio::spawn(async move { engine.drain_uploads().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), drive.accepted.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn accepted_file_and_folder_survive_restart_and_delta_before_acknowledgment() {
    for folder in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("catalog");
        let cloud = interrupted_drive(drive(), true);
        let open = || Arc::new(Engine::open(&db, cloud.clone(), Arc::new(SystemClock)).unwrap());
        let engine = open();
        engine.sync().await.unwrap();
        let kind = if folder {
            ItemKind::Folder
        } else {
            ItemKind::File
        };
        let entry = engine.create(1, "entry", kind).unwrap();
        let child = if folder {
            engine.create(entry.inode, "child", ItemKind::File).unwrap()
        } else {
            entry.clone()
        };
        engine
            .write(child.inode, 0, b"staged", false)
            .await
            .unwrap();
        cancel_after_acceptance(engine, &cloud).await;

        let engine = open();
        engine.sync().await.unwrap();
        assert_eq!(engine.list_children("root").unwrap().len(), 1);
        engine.drain_uploads().await.unwrap();
        engine.sync().await.unwrap();
        let contents = engine.list_children("root").unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].inode, entry.inode);
        assert_eq!(contents[0].name, "entry");
        assert_eq!(contents[0].sync_state, SyncState::Clean);
        assert_eq!(engine.read(child.inode, 0, 20).await.unwrap(), b"staged");
        assert!(engine.pending_uploads().unwrap().is_empty());
    }
}

#[tokio::test]
async fn accepted_update_replays_without_overwriting_a_competing_version() {
    for competing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("catalog");
        let inner = drive();
        let target = UploadTarget::New {
            parent_id: "root",
            name: "report",
        };
        let original = inner.upload(target, b"base", None).await.unwrap();
        let cloud = interrupted_drive(inner.clone(), true);
        let open = || Arc::new(Engine::open(&db, cloud.clone(), Arc::new(SystemClock)).unwrap());
        let engine = open();
        engine.sync().await.unwrap();
        let file = engine.item(&original.item_id).unwrap().unwrap();
        engine.write(file.inode, 0, b"ours", false).await.unwrap();
        cancel_after_acceptance(engine, &cloud).await;
        let target = UploadTarget::Existing {
            item_id: &original.item_id,
        };
        let accepted = inner.upload_destination(target).await.unwrap().unwrap();
        let expected = if competing {
            inner
                .upload(target, b"peer", accepted.etag.as_deref())
                .await
                .unwrap()
        } else {
            accepted
        };

        let engine = open();
        engine.sync().await.unwrap();
        engine.drain_uploads().await.unwrap();
        engine.sync().await.unwrap();
        let canonical = engine.resolve_path("/report").unwrap().unwrap();
        assert_eq!(canonical.item_id, original.item_id);
        assert_eq!(canonical.etag, expected.etag);
        assert_eq!(engine.read(file.inode, 0, 20).await.unwrap(), b"ours");
        if competing {
            assert_eq!(engine.read(canonical.inode, 0, 20).await.unwrap(), b"peer");
            assert_eq!(
                engine.getattr(file.inode).unwrap().unwrap().sync_state,
                SyncState::Conflict
            );
        } else {
            assert_eq!(canonical.inode, file.inode);
        }
        assert_eq!(
            engine.list_children("root").unwrap().len(),
            if competing { 2 } else { 1 }
        );
        assert!(engine.pending_uploads().unwrap().is_empty());
    }
}

#[tokio::test]
async fn full_scan_reveals_restored_ids_after_missing_tombstones_and_keeps_pending_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("catalog");
    let drive = drive();
    let open = || Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    for name in ["restored", "pending"] {
        drive
            .upload(
                UploadTarget::New {
                    parent_id: "root",
                    name,
                },
                name.as_bytes(),
                None,
            )
            .await
            .unwrap();
    }
    let engine = open();
    engine.sync().await.unwrap();
    let id = engine.resolve_path("/restored").unwrap().unwrap().item_id;
    let target = UploadTarget::Existing { item_id: &id };
    let remote = drive.upload_destination(target).await.unwrap().unwrap();
    engine.unlink(1, "restored", ItemKind::File).unwrap();
    engine.drain_uploads().await.unwrap();
    engine.unlink(1, "pending", ItemKind::File).unwrap();
    drive.upsert(remote.clone());
    drive.set_content(&id, b"restored".to_vec());
    drive.expire_tokens();
    drop(engine);

    let engine = open();
    engine.sync().await.unwrap();
    assert_eq!(
        engine.resolve_path("/restored").unwrap().unwrap().item_id,
        id
    );
    assert!(engine.resolve_path("/pending").unwrap().is_none());
    assert_eq!(engine.pending_uploads().unwrap().len(), 1);
    drop(engine);
    let engine = open();
    engine.sync().await.unwrap();
    assert_eq!(engine.list_children("root").unwrap().len(), 1);
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
}

#[tokio::test]
async fn create_and_open_survives_unlink_until_handle_release() {
    let dir = tempfile::tempdir().unwrap();
    let drive = drive();
    let engine = Engine::open(dir.path().join("catalog"), drive, Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let file = engine.create_and_open(1, "temporary", Some(0o600)).unwrap();
    engine.unlink(1, "temporary", ItemKind::File).unwrap();
    assert!(engine.resolve_path("/temporary").unwrap().is_none());
    assert_eq!(engine.drain_uploads().await.unwrap(), 0);
    engine
        .write(file.inode, 0, b"still open", false)
        .await
        .unwrap();
    assert_eq!(engine.read(file.inode, 0, 20).await.unwrap(), b"still open");
    engine.release_file(file.inode);
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
    assert!(engine.getattr(file.inode).unwrap().is_none());
}

#[tokio::test]
async fn offline_editor_replacement_preserves_a_newer_remote_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("catalog");
    let drive = drive();
    let open = || Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    let original = drive
        .upload(
            UploadTarget::New {
                parent_id: "root",
                name: "report.txt",
            },
            b"base",
            None,
        )
        .await
        .unwrap();
    let engine = open();
    engine.sync().await.unwrap();
    drive.set_online(false);
    let draft = engine
        .create_and_open(1, ".report.txt.tmp", Some(0o600))
        .unwrap();
    engine
        .write(draft.inode, 0, b"editor", false)
        .await
        .unwrap();
    engine.release_file(draft.inode);
    engine
        .rename(1, ".report.txt.tmp", 1, "report.txt", false)
        .unwrap();
    drop(engine);
    drive.set_online(true);
    drive
        .upload(
            UploadTarget::Existing {
                item_id: &original.item_id,
            },
            b"newer!",
            original.etag.as_deref(),
        )
        .await
        .unwrap();

    let engine = open();
    engine.sync().await.unwrap();
    engine.drain_uploads().await.unwrap();
    let canonical = engine.resolve_path("/report.txt").unwrap().unwrap();
    assert_eq!(canonical.item_id, original.item_id);
    assert_eq!(
        engine.read(canonical.inode, 0, 20).await.unwrap(),
        b"newer!"
    );
    let conflict = engine.getattr(draft.inode).unwrap().unwrap();
    assert_eq!(conflict.sync_state, SyncState::Conflict);
    assert_eq!(engine.read(conflict.inode, 0, 20).await.unwrap(), b"editor");
    assert!(drive.recycled_items().is_empty());
    assert!(engine.pending_uploads().unwrap().is_empty());
}

#[tokio::test]
async fn failed_large_sparse_upload_does_not_block_a_small_independent_file() {
    let dir = tempfile::tempdir().unwrap();
    let cloud = interrupted_drive(drive(), false);
    let engine = Engine::open(dir.path().join("catalog"), cloud, Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let large = engine.create(1, "large", ItemKind::File).unwrap();
    engine
        .truncate(large.inode, 2 * 1024 * 1024 * 1024 * 1024)
        .await
        .unwrap();
    let small = engine.create(1, "small", ItemKind::File).unwrap();
    engine
        .write(small.inode, 0, b"progress", false)
        .await
        .unwrap();
    assert!(matches!(
        engine.drain_uploads().await,
        Err(EngineError::Cloud(CloudError::Unavailable))
    ));
    assert_eq!(
        engine.getattr(small.inode).unwrap().unwrap().sync_state,
        SyncState::Clean
    );
    assert_eq!(engine.pending_uploads().unwrap()[0].inode, large.inode);
    assert_eq!(engine.read(small.inode, 0, 20).await.unwrap(), b"progress");
}

#[tokio::test]
async fn publication_race_preserves_staged_and_peer_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let inner = drive();
    let cloud = interrupted_drive(inner.clone(), false);
    cloud
        .peer_before_move
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let open =
        || Engine::open(dir.path().join("db"), cloud.clone(), Arc::new(SystemClock)).unwrap();
    let engine = open();
    engine.sync().await.unwrap();
    let file = engine.create(1, "draft.txt", ItemKind::File).unwrap();
    engine.write(file.inode, 0, b"ours", false).await.unwrap();
    engine.drain_uploads().await.unwrap();
    drop(engine);

    let engine = open();
    engine.sync().await.unwrap();
    let copy = engine.getattr(file.inode).unwrap().unwrap();
    assert!(copy.name.starts_with("draft (conflict "));
    assert!(copy.name.ends_with(").txt"));
    assert_eq!(copy.sync_state, SyncState::Conflict);
    assert_eq!(
        engine
            .resolve_path(format!("/{}", copy.name))
            .unwrap()
            .unwrap()
            .inode,
        file.inode
    );
    assert_eq!(engine.read(file.inode, 0, 10).await.unwrap(), b"ours");
    assert_eq!(
        inner.read_range(&copy.item_id, 0, 10).await.unwrap(),
        b"ours"
    );
    let entries = engine.list_children("root").unwrap();
    assert_eq!(entries.len(), 2);
    let peer = entries
        .iter()
        .find(|item| item.inode != file.inode)
        .unwrap();
    assert!(peer.name.starts_with("onedrive-rs-upload-"));
    assert_eq!(engine.read(peer.inode, 0, 10).await.unwrap(), b"peer");
    assert!(engine.pending_uploads().unwrap().is_empty());
}

#[tokio::test]
async fn accepted_move_reconciles_before_undo_after_restart() {
    for cross_parent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let inner = drive();
        let original = inner
            .upload(
                UploadTarget::New {
                    parent_id: "root",
                    name: "a",
                },
                b"ours",
                None,
            )
            .await
            .unwrap();
        if cross_parent {
            inner.create_folder("root", "other").await.unwrap();
        }
        let cloud = interrupted_drive(inner.clone(), true);
        let open = || {
            Arc::new(
                Engine::open(dir.path().join("db"), cloud.clone(), Arc::new(SystemClock)).unwrap(),
            )
        };
        let engine = open();
        engine.sync().await.unwrap();
        let inode = engine.resolve_path("/a").unwrap().unwrap().inode;
        let destination = if cross_parent {
            engine.resolve_path("/other").unwrap().unwrap().inode
        } else {
            1
        };
        engine.rename(1, "a", destination, "b", false).unwrap();
        cancel_after_acceptance(engine, &cloud).await;

        let engine = open();
        engine.rename(destination, "b", 1, "a", false).unwrap();
        engine.drain_uploads().await.unwrap();
        let remote = inner
            .upload_destination(UploadTarget::Existing {
                item_id: &original.item_id,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.name.as_deref(), Some("a"));
        assert_eq!(remote.parent_id.as_deref(), Some("root"));
        engine.sync().await.unwrap();
        let file = engine.resolve_path("/a").unwrap().unwrap();
        assert_eq!(file.inode, inode);
        assert_eq!(file.item_id, original.item_id);
        assert_eq!(file.sync_state, SyncState::Clean);
        assert_eq!(engine.read(inode, 0, 10).await.unwrap(), b"ours");
        let abandoned_path = if cross_parent { "/other/b" } else { "/b" };
        assert!(engine.resolve_path(abandoned_path).unwrap().is_none());
        assert!(engine.pending_uploads().unwrap().is_empty());
    }
}

#[tokio::test]
async fn interrupted_publication_preserves_a_superseding_partial_edit() {
    use std::sync::atomic::Ordering;

    for competing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let inner = drive();
        let cloud = interrupted_drive(inner.clone(), false);
        let open = || {
            Arc::new(
                Engine::open(dir.path().join("db"), cloud.clone(), Arc::new(SystemClock)).unwrap(),
            )
        };
        let engine = open();
        engine.sync().await.unwrap();
        let file = engine.create(1, "draft", ItemKind::File).unwrap();
        engine.write(file.inode, 0, b"ours", false).await.unwrap();
        cloud.pause_move.store(true, Ordering::SeqCst);
        cancel_after_acceptance(engine, &cloud).await;

        let published = inner
            .upload_destination(UploadTarget::New {
                parent_id: "root",
                name: "draft",
            })
            .await
            .unwrap()
            .unwrap();
        if competing {
            inner
                .upload(
                    UploadTarget::Existing {
                        item_id: &published.item_id,
                    },
                    b"peer",
                    published.etag.as_deref(),
                )
                .await
                .unwrap();
        }
        let engine = open();
        engine.sync().await.unwrap();
        assert_eq!(engine.read(file.inode, 0, 10).await.unwrap(), b"ours");
        engine.write(file.inode, 3, b"!", false).await.unwrap();
        engine.rename(1, "draft", 1, "final", false).unwrap();
        drop(engine);

        let engine = open();
        engine.drain_uploads().await.unwrap();
        engine.sync().await.unwrap();
        let item = engine.getattr(file.inode).unwrap().unwrap();
        assert_eq!(engine.read(item.inode, 0, 10).await.unwrap(), b"our!");
        assert_eq!(
            inner.read_range(&item.item_id, 0, 10).await.unwrap(),
            b"our!"
        );
        if competing {
            assert!(item.name.starts_with("final (conflict "));
            assert_eq!(item.sync_state, SyncState::Conflict);
            let peer = engine.resolve_path("/draft").unwrap().unwrap();
            assert_eq!(peer.item_id, published.item_id);
            assert_eq!(engine.read(peer.inode, 0, 10).await.unwrap(), b"peer");
        } else {
            assert_eq!(
                engine.resolve_path("/final").unwrap().unwrap().inode,
                file.inode
            );
            assert!(engine.resolve_path("/draft").unwrap().is_none());
            assert_eq!(item.item_id, published.item_id);
            assert_eq!(item.sync_state, SyncState::Clean);
        }
        assert!(engine.pending_uploads().unwrap().is_empty());
    }
}

#[tokio::test]
async fn interrupted_cycle_move_is_recovered_before_publishing_either_file() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let inner = drive();
    for (name, bytes) in [("a", b"alpha".as_slice()), ("b", b"beta".as_slice())] {
        inner
            .upload(
                UploadTarget::New {
                    parent_id: "root",
                    name,
                },
                bytes,
                None,
            )
            .await
            .unwrap();
    }
    let cloud = interrupted_drive(inner.clone(), false);
    let open = || {
        Arc::new(Engine::open(dir.path().join("db"), cloud.clone(), Arc::new(SystemClock)).unwrap())
    };
    let engine = open();
    engine.sync().await.unwrap();
    let a = engine.resolve_path("/a").unwrap().unwrap();
    let b = engine.resolve_path("/b").unwrap().unwrap();
    engine.rename(1, "a", 1, "temp", false).unwrap();
    engine.rename(1, "b", 1, "a", false).unwrap();
    engine.rename(1, "temp", 1, "b", false).unwrap();
    cloud.pause_move.store(true, Ordering::SeqCst);
    cancel_after_acceptance(engine, &cloud).await;

    let engine = open();
    engine.sync().await.unwrap();
    engine.drain_uploads().await.unwrap();
    engine.sync().await.unwrap();
    assert_eq!(engine.list_children("root").unwrap().len(), 2);
    for (path, original, bytes) in [
        ("/a", b, b"beta".as_slice()),
        ("/b", a, b"alpha".as_slice()),
    ] {
        let item = engine.resolve_path(path).unwrap().unwrap();
        assert_eq!(item.inode, original.inode);
        assert_eq!(item.item_id, original.item_id);
        assert_eq!(item.sync_state, SyncState::Clean);
        assert_eq!(engine.read(item.inode, 0, 10).await.unwrap(), bytes);
        let remote = inner
            .upload_destination(UploadTarget::Existing {
                item_id: &item.item_id,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.name.as_deref(), path.strip_prefix('/'));
    }
    assert!(engine.pending_uploads().unwrap().is_empty());
}

fn cloud_file(size: u64) -> RemoteItem {
    RemoteItem {
        item_id: "cold".into(),
        parent_id: Some("root".into()),
        name: Some("cold".into()),
        kind: ItemKind::File,
        size,
        mtime: 100,
        etag: Some("base".into()),
        ctag: Some("content".into()),
        quickxor_hash: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_multigib_edit_is_sparse_and_concurrent_appends_survive_restart() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let drive = drive();
    let original_size = 3 * 1024 * 1024 * 1024;
    // The fake only needs the prefix that survives the later truncate.
    drive.upsert(cloud_file(original_size));
    drive.set_content("cold", b"ABCD".to_vec());
    let engine = Arc::new(Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap());
    engine.sync().await.unwrap();
    let inode = engine.item("cold").unwrap().unwrap().inode;
    drive.set_online(false);
    engine.write(inode, 1, b"xy", false).await.unwrap();
    let append = |bytes| {
        let engine = engine.clone();
        tokio::spawn(async move { engine.write(inode, 0, bytes, true).await.unwrap() })
    };
    let left = append(b"L");
    let right = append(b"R");
    left.await.unwrap();
    right.await.unwrap();
    assert!(drive.range_requests().is_empty());
    let stage = std::fs::read_dir(db.with_added_extension("staging"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .metadata()
        .unwrap();
    assert_eq!(stage.len(), original_size + 2);
    assert!(stage.blocks() * 512 < 128 * 1024);
    drop(engine);

    let engine = Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    assert_eq!(engine.read(inode, 1, 2).await.unwrap(), b"xy");
    let tail = engine.read(inode, original_size, 2).await.unwrap();
    assert!(tail == b"LR" || tail == b"RL");
    engine.truncate(inode, 4).await.unwrap();
    engine.truncate(inode, 12).await.unwrap();
    assert_eq!(engine.read(inode, 4, 8).await.unwrap(), [0; 8]);
    assert!(drive.range_requests().is_empty());

    drive.set_online(true);
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
    assert_eq!(
        drive.range_requests(),
        [("cold".into(), 0, 1), ("cold".into(), 3, 1)]
    );
    assert_eq!(
        engine.read(inode, 0, 12).await.unwrap(),
        b"AxyD\0\0\0\0\0\0\0\0"
    );
}

#[tokio::test]
async fn missing_cold_base_retains_local_extents_without_using_newer_remote_bytes() {
    use onedrive_core::{CloudDrive, EngineError};

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let drive = drive();
    let mut remote = cloud_file(4);
    drive.upsert(remote.clone());
    drive.set_content("cold", b"base".to_vec());
    let engine = Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let inode = engine.item("cold").unwrap().unwrap().inode;
    engine.write(inode, 1, b"XY", false).await.unwrap();
    assert!(drive.range_requests().is_empty());

    // A cTag mismatch alone rejects bytes even if the eTag has not changed.
    remote.ctag = Some("new-content".into());
    drive.upsert(remote.clone());
    drive.set_content("cold", b"peer".to_vec());
    assert!(matches!(
        engine.drain_uploads().await,
        Err(EngineError::OriginalVersionUnavailable)
    ));
    assert_eq!(
        engine.pending_uploads().unwrap()[0].sync_state,
        SyncState::Conflict
    );
    drop(engine);

    let engine = Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    assert_eq!(engine.read(inode, 1, 2).await.unwrap(), b"XY");
    assert!(matches!(
        engine.read(inode, 0, 4).await,
        Err(EngineError::OriginalVersionUnavailable)
    ));
    assert_eq!(drive.read_range("cold", 0, 4).await.unwrap(), b"peer");

    // Completing the stage locally can recover a full Conflict Copy.
    remote.etag = Some("new-base".into());
    drive.upsert(remote);
    engine.write(inode, 0, b"A", false).await.unwrap();
    engine.write(inode, 3, b"D", false).await.unwrap();
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
    assert_eq!(engine.read(inode, 0, 4).await.unwrap(), b"AXYD");
    assert_eq!(drive.read_range("cold", 0, 4).await.unwrap(), b"peer");
    assert_eq!(
        engine.getattr(inode).unwrap().unwrap().sync_state,
        SyncState::Conflict
    );
}

#[tokio::test]
async fn offline_unlink_and_rmdir_restore_identity_when_remote_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let drive = drive();
    let folder = drive.create_folder("root", "folder").await.unwrap();
    let mut file = cloud_file(4);
    drive.upsert(file.clone());
    drive.set_content("cold", b"base".to_vec());
    let open = || Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap();
    let engine = open();
    engine.sync().await.unwrap();
    let file_inode = engine.item("cold").unwrap().unwrap().inode;
    let folder_inode = engine.item(&folder.item_id).unwrap().unwrap().inode;
    drive.set_online(false);
    engine.unlink(1, "cold", ItemKind::File).unwrap();
    engine.unlink(1, "folder", ItemKind::Folder).unwrap();
    assert!(engine.list_children("root").unwrap().is_empty());
    drop(engine);

    drive.set_online(true);
    file.etag = Some("peer-version".into());
    file.ctag = Some("peer-content".into());
    drive.upsert(file);
    drive.set_content("cold", b"peer".to_vec());
    // FakeCloudDrive keeps the folder's original eTag when adding its child.
    let child = drive
        .upload(
            UploadTarget::New {
                parent_id: &folder.item_id,
                name: "remote-child",
            },
            b"keep me",
            None,
        )
        .await
        .unwrap();

    let engine = open();
    assert!(matches!(
        engine.drain_uploads().await,
        Err(EngineError::Cloud(CloudError::Conflict))
    ));
    assert!(drive.recycled_items().is_empty());
    assert!(engine.pending_uploads().unwrap().is_empty());
    engine.sync().await.unwrap();
    let restored_file = engine.resolve_path("/cold").unwrap().unwrap();
    assert_eq!(
        (restored_file.inode, restored_file.sync_state),
        (file_inode, SyncState::Conflict)
    );
    assert_eq!(
        engine.read(restored_file.inode, 0, 4).await.unwrap(),
        b"peer"
    );
    let restored_folder = engine.resolve_path("/folder").unwrap().unwrap();
    assert_eq!(
        (restored_folder.inode, restored_folder.sync_state),
        (folder_inode, SyncState::Conflict)
    );
    let restored_child = engine
        .resolve_path("/folder/remote-child")
        .unwrap()
        .unwrap();
    assert_eq!(restored_child.item_id, child.item_id);
    assert_eq!(
        engine.read(restored_child.inode, 0, 7).await.unwrap(),
        b"keep me"
    );
}

#[tokio::test]
async fn accepted_checked_delete_replays_by_id_without_touching_new_path_occupant() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let inner = drive();
    inner.upsert(cloud_file(4));
    inner.set_content("cold", b"base".to_vec());
    let drive = interrupted_drive(inner.clone(), true);
    let open = || Arc::new(Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap());
    let engine = open();
    engine.sync().await.unwrap();
    engine.unlink(1, "cold", ItemKind::File).unwrap();
    cancel_after_acceptance(engine.clone(), &drive).await;
    drop(engine);

    let replacement = inner
        .upload(
            UploadTarget::New {
                parent_id: "root",
                name: "cold",
            },
            b"new item",
            None,
        )
        .await
        .unwrap();
    let engine = open();
    engine.sync().await.unwrap();
    assert_eq!(engine.drain_uploads().await.unwrap(), 1);
    assert_eq!(inner.recycled_items(), ["cold"]);
    let survivor = engine.resolve_path("/cold").unwrap().unwrap();
    assert_eq!(survivor.item_id, replacement.item_id);
    assert_eq!(
        engine.read(survivor.inode, 0, 8).await.unwrap(),
        b"new item"
    );
}

#[tokio::test]
async fn accepted_move_rebinds_a_cold_sparse_append_after_restart() {
    cold_edit_during_move(Some("content"), false).await;
}

#[tokio::test]
async fn accepted_move_does_not_rebind_edited_peer_content_after_restart() {
    cold_edit_during_move(Some("content"), true).await;
}

#[tokio::test]
async fn accepted_move_does_not_rebind_a_sparse_base_without_content_proof() {
    cold_edit_during_move(None, false).await;
}

async fn cold_edit_during_move(ctag: Option<&str>, peer_edit: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let inner = drive();
    let mut base = cloud_file(4);
    base.ctag = ctag.map(str::to_owned);
    inner.upsert(base);
    inner.set_content("cold", b"base".to_vec());
    let drive = interrupted_drive(inner.clone(), true);
    let open = || Arc::new(Engine::open(&db, drive.clone(), Arc::new(SystemClock)).unwrap());
    let engine = open();
    engine.sync().await.unwrap();
    let inode = engine.item("cold").unwrap().unwrap().inode;
    engine.rename(1, "cold", 1, "moved", false).unwrap();
    let upload = tokio::spawn({
        let engine = engine.clone();
        async move { engine.drain_uploads().await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), drive.accepted.notified())
        .await
        .unwrap();
    engine.write(inode, 0, b"!", true).await.unwrap();
    assert!(inner.range_requests().is_empty());
    assert_eq!(engine.read(inode, 4, 1).await.unwrap(), b"!");
    upload.abort();
    assert!(upload.await.unwrap_err().is_cancelled());
    drop(engine);

    if peer_edit {
        inner
            .upload(UploadTarget::Existing { item_id: "cold" }, b"peer", None)
            .await
            .unwrap();
    }
    let engine = open();
    let result = engine.drain_uploads().await;
    if peer_edit || ctag.is_none() {
        assert!(matches!(
            result,
            Err(EngineError::OriginalVersionUnavailable)
        ));
        let original = engine.resolve_path("/moved").unwrap().unwrap();
        assert_ne!(original.inode, inode);
        assert_eq!(original.item_id, "cold");
        let unchanged = if peer_edit { b"peer" } else { b"base" };
        assert_eq!(engine.read(original.inode, 0, 4).await.unwrap(), unchanged);
        assert_eq!(
            engine.getattr(inode).unwrap().unwrap().sync_state,
            SyncState::Conflict
        );
        assert_eq!(engine.read(inode, 4, 1).await.unwrap(), b"!");
    } else {
        assert_eq!(result.unwrap(), 2);
        let moved = engine.resolve_path("/moved").unwrap().unwrap();
        assert_eq!(moved.inode, inode);
        assert_eq!(moved.item_id, "cold");
        assert_eq!(moved.sync_state, SyncState::Clean);
        assert_eq!(inner.read_range("cold", 0, 5).await.unwrap(), b"base!");
    }
}
