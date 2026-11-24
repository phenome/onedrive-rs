use std::sync::Arc;

use onedrive_core::{
    CloudError, DeltaRequest, Engine, FakeCloudDrive, ItemKind, RemoteItem, SystemClock,
};
use tempfile::TempDir;

fn item(id: &str, parent_id: Option<&str>, name: &str, kind: ItemKind) -> RemoteItem {
    RemoteItem {
        item_id: id.into(),
        parent_id: parent_id.map(str::to_owned),
        name: Some(name.into()),
        kind,
        size: 0,
        mtime: 1_700_000_000,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    }
}

fn drive(page_size: usize) -> Arc<FakeCloudDrive> {
    let drive = Arc::new(FakeCloudDrive::new(page_size));
    drive.upsert(item("root", None, "root", ItemKind::Folder));
    drive
}

fn open(temp: &TempDir, drive: &Arc<FakeCloudDrive>) -> Engine {
    Engine::open(
        temp.path().join("metadata.db"),
        drive.clone(),
        Arc::new(SystemClock),
    )
    .unwrap()
}

#[tokio::test]
async fn initial_sync_resumes_at_committed_next_link_and_reuses_final_delta_link() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("a", Some("root"), "a.txt", ItemKind::File));
    drive.fail_after(1, CloudError::Unavailable);

    let engine = open(&temp, &drive);
    assert!(engine.sync().await.is_err());
    assert_eq!(engine.root().unwrap().unwrap().item_id, "root");
    let interrupted = drive.requests();
    assert_eq!(interrupted[0], DeltaRequest::Initial);
    assert!(matches!(interrupted[1], DeltaRequest::Link(_)));
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert_eq!(drive.requests()[2], interrupted[1]);
    assert_eq!(
        engine.lookup("root", "a.txt").unwrap().unwrap().item_id,
        "a"
    );
    drop(engine);

    open(&temp, &drive).sync().await.unwrap();
    let requests = drive.requests();
    assert_eq!(requests.len(), 4);
    assert!(matches!(requests[3], DeltaRequest::Link(_)));
    assert_ne!(requests[3], requests[2]);
}

#[tokio::test]
async fn ancestor_rename_repaths_unchanged_descendants_with_stable_inodes() {
    let temp = TempDir::new().unwrap();
    let drive = drive(4);
    drive.upsert(item("folder", Some("root"), "Old", ItemKind::Folder));
    drive.upsert(item("nested", Some("folder"), "Nested", ItemKind::Folder));
    drive.upsert(item("file", Some("nested"), "file.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let before = engine
        .resolve_path("/Old/Nested/file.txt")
        .unwrap()
        .unwrap();

    drive.upsert(item("folder", Some("root"), "New", ItemKind::Folder));
    engine.sync().await.unwrap();

    assert!(
        engine
            .resolve_path("/Old/Nested/file.txt")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine.resolve_path("/New/Nested/file.txt").unwrap(),
        Some(before.clone())
    );
    assert_eq!(
        engine.path_for("file").unwrap().unwrap().to_str(),
        Some("/New/Nested/file.txt")
    );
    assert_eq!(engine.getattr(before.inode).unwrap(), Some(before.clone()));
    assert_eq!(engine.list_children("nested").unwrap(), [before]);
}

#[tokio::test]
async fn incremental_sync_applies_changes_and_tombstones_across_a_restart() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("gone", Some("root"), "Gone", ItemKind::Folder));
    drive.upsert(item("dest", Some("root"), "Destination", ItemKind::Folder));
    drive.upsert(item("keep", Some("gone"), "keep.txt", ItemKind::File));
    drive.upsert(item("remove", Some("gone"), "remove.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let inode = engine.item("keep").unwrap().unwrap().inode;

    drive.delete("gone");
    drive.fail_after(1, CloudError::Unavailable);
    let mut changed = item("keep", Some("dest"), "renamed.txt", ItemKind::File);
    changed.size = 123;
    changed.mtime += 60;
    changed.etag = Some("new-etag".into());
    changed.ctag = Some("new-ctag".into());
    changed.quickxor_hash = Some("new-hash".into());
    drive.upsert(changed);
    let mut stub = item("keep", None, "", ItemKind::Folder);
    stub.name = None;
    drive.upsert(stub);
    drive.upsert(item("new", Some("dest"), "new.txt", ItemKind::File));
    assert!(engine.sync().await.is_err());
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert!(engine.item("gone").unwrap().is_none());
    assert!(engine.item("remove").unwrap().is_none());
    let moved = engine
        .resolve_path("/Destination/renamed.txt")
        .unwrap()
        .unwrap();
    assert_eq!(moved.inode, inode);
    assert_eq!(moved.size, 123);
    assert_eq!(moved.mtime, 1_700_000_060);
    assert_eq!(moved.etag.as_deref(), Some("new-etag"));
    assert_eq!(moved.ctag.as_deref(), Some("new-ctag"));
    assert_eq!(moved.quickxor_hash.as_deref(), Some("new-hash"));
    assert!(
        engine
            .resolve_path("/Destination/new.txt")
            .unwrap()
            .is_some()
    );
    drop(engine);
    let engine = open(&temp, &drive);
    assert_eq!(engine.getattr(inode).unwrap(), Some(moved));
    assert!(engine.resolve_path("/Gone").unwrap().is_none());
}

#[tokio::test]
async fn expired_cursor_reconciles_absent_items_after_restart_and_retains_local_state() {
    use onedrive_core::{PinState, SyncState};

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("folder", Some("root"), "Folder", ItemKind::Folder));
    drive.upsert(item("dirty", Some("folder"), "edit.txt", ItemKind::File));
    drive.upsert(item("pin", Some("root"), "pin.txt", ItemKind::File));
    drive.upsert(item("clean", Some("root"), "gone.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert!(engine.set_sync_state("dirty", SyncState::Dirty).unwrap());
    assert!(engine.set_pin_state("folder", PinState::Pinned).unwrap());
    assert!(
        engine
            .set_pin_state("dirty", PinState::UnpinOverride)
            .unwrap()
    );
    assert!(engine.set_pin_state("pin", PinState::Pinned).unwrap());
    let dirty = engine.item("dirty").unwrap().unwrap();

    drive.delete("folder");
    drive.delete("clean");
    drive.delete("pin");
    drive.upsert(item("new", Some("root"), "new.txt", ItemKind::File));
    drive.expire_tokens();
    drive.fail_after(2, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    assert!(engine.item("clean").unwrap().is_some());
    let requests = drive.requests();
    let checkpoint = requests.last().unwrap();
    assert!(matches!(checkpoint, DeltaRequest::Link(_)));
    assert_eq!(requests[requests.len() - 2], DeltaRequest::Initial);
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert_eq!(&drive.requests()[requests.len()], checkpoint);
    assert!(engine.item("clean").unwrap().is_none());
    assert!(engine.item("pin").unwrap().is_none());
    assert_eq!(engine.pin_state("pin").unwrap(), PinState::Pinned);
    assert_eq!(
        engine.resolve_path("/Folder/edit.txt").unwrap(),
        Some(dirty)
    );
    assert_eq!(engine.pin_state("folder").unwrap(), PinState::Pinned);
    assert!(engine.lookup("root", "new.txt").unwrap().is_some());

    drive.upsert(item("pin", Some("root"), "restored.txt", ItemKind::File));
    engine.sync().await.unwrap();
    assert_eq!(
        engine.item("pin").unwrap().unwrap().pin_state,
        PinState::Pinned
    );

    assert!(engine.set_sync_state("dirty", SyncState::Clean).unwrap());
    drop(engine);
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert!(engine.resolve_path("/Folder").unwrap().is_none());
    assert_eq!(engine.pin_state("dirty").unwrap(), PinState::UnpinOverride);
}

struct FakeClock {
    start: tokio::time::Instant,
}

#[async_trait::async_trait]
impl onedrive_core::Clock for FakeClock {
    fn now(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    async fn sleep(&self, duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_survives_cancellation_and_restart_before_retrying_the_same_link() {
    use std::time::Duration;

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("a", Some("root"), "a.txt", ItemKind::File));
    let clock = Arc::new(FakeClock {
        start: tokio::time::Instant::now(),
    });
    let database = temp.path().join("metadata.db");
    let engine = Arc::new(Engine::open(&database, drive.clone(), clock.clone()).unwrap());
    let wait = Duration::from_millis(30_500);
    drive.fail_after(1, CloudError::Throttled { retry_after: wait });

    let worker = tokio::spawn({
        let engine = engine.clone();
        async move { engine.sync().await }
    });
    tokio::task::yield_now().await;
    let requests = drive.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(requests[1], DeltaRequest::Link(_)));
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(engine);

    let elapsed = Duration::from_millis(10_250);
    tokio::time::advance(elapsed).await;
    let engine = Engine::open(&database, drive.clone(), clock.clone()).unwrap();
    engine.sync().await.unwrap();
    assert_eq!(clock.start.elapsed(), wait);
    assert_eq!(
        drive.requests(),
        [
            DeltaRequest::Initial,
            requests[1].clone(),
            requests[1].clone()
        ]
    );
    assert!(engine.lookup("root", "a.txt").unwrap().is_some());
}

#[tokio::test]
async fn page_with_a_storage_error_rolls_back_both_entries_and_checkpoint_in_wal() {
    use onedrive_core::EngineError;

    let temp = TempDir::new().unwrap();
    let drive = drive(2);
    drive.upsert(item("a", Some("root"), "a.txt", ItemKind::File));
    drive.upsert(item("b", Some("root"), "b.txt", ItemKind::File));
    let mut invalid = item("c", Some("root"), "c.txt", ItemKind::File);
    invalid.size = u64::MAX;
    drive.upsert(invalid);
    let engine = open(&temp, &drive);

    assert!(matches!(engine.sync().await, Err(EngineError::Catalog(_))));
    assert!(temp.path().join("metadata.db-wal").is_file());
    let observer = open(&temp, &drive);
    assert!(observer.item("a").unwrap().is_some());
    assert!(observer.item("b").unwrap().is_none());
    drop(observer);
    drop(engine);

    let engine = open(&temp, &drive);
    assert!(matches!(engine.sync().await, Err(EngineError::Catalog(_))));
    let requests = drive.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[2], requests[1]);
    assert!(engine.item("b").unwrap().is_none());
}

#[tokio::test]
async fn root_uses_kernel_root_inode_even_if_a_child_arrives_first() {
    let temp = TempDir::new().unwrap();
    let drive = Arc::new(FakeCloudDrive::new(1));
    drive.upsert(item("early", Some("root"), "early.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let early = engine.item("early").unwrap().unwrap();
    assert_ne!(early.inode, 1);
    assert!(engine.path_for("early").unwrap().is_none());

    drive.upsert(item("root", None, "root", ItemKind::Folder));
    engine.sync().await.unwrap();
    assert_eq!(engine.root().unwrap().unwrap().inode, 1);
    assert_eq!(engine.resolve_path("/early.txt").unwrap(), Some(early));
}

#[tokio::test]
async fn nameless_sighting_restores_a_folder_retained_for_a_dirty_child() {
    use onedrive_core::SyncState;

    for full_scan in [true, false] {
        let temp = TempDir::new().unwrap();
        let drive = drive(1);
        let mut folder = item("folder", Some("root"), "Folder", ItemKind::Folder);
        drive.upsert(folder.clone());
        drive.upsert(item("child", Some("folder"), "edit.txt", ItemKind::File));
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        engine.set_sync_state("child", SyncState::Dirty).unwrap();
        drive.delete("folder");
        engine.sync().await.unwrap();

        folder.name = None;
        drive.upsert(folder);
        if full_scan {
            drive.expire_tokens();
        }
        engine.sync().await.unwrap();
        engine.set_sync_state("child", SyncState::Clean).unwrap();
        engine.sync().await.unwrap();
        assert_eq!(
            engine.resolve_path("/Folder").unwrap().unwrap().item_id,
            "folder"
        );
        assert!(engine.item("child").unwrap().is_none());
    }
}

#[tokio::test]
async fn expired_continuation_after_resync_progress_recovers_in_the_same_sync() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();

    drive.upsert(item("a", Some("root"), "a.txt", ItemKind::File));
    drive.expire_tokens();
    drive.fail_after(2, CloudError::ResyncRequired);
    engine.sync().await.unwrap();

    assert_eq!(
        engine.lookup("root", "a.txt").unwrap().unwrap().item_id,
        "a"
    );
}

#[tokio::test]
async fn expired_initial_request_returns_the_error_without_restarting() {
    use onedrive_core::EngineError;

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.fail_after(0, CloudError::ResyncRequired);
    let engine = open(&temp, &drive);

    assert!(matches!(
        engine.sync().await,
        Err(EngineError::Cloud(CloudError::ResyncRequired))
    ));
    assert_eq!(drive.requests(), [DeltaRequest::Initial]);
}

#[tokio::test]
async fn replacement_keeps_dirty_file_and_remote_file_reachable_in_either_delta_order() {
    use onedrive_core::{PinState, SyncState};

    for deletion_first in [true, false] {
        let temp = TempDir::new().unwrap();
        let drive = drive(1);
        let mut old = item("old", Some("root"), "report.txt", ItemKind::File);
        old.size = 123;
        old.etag = Some("base-version".into());
        drive.upsert(old);
        drive.upsert(item(
            "sibling",
            Some("root"),
            "report (conflict).txt",
            ItemKind::File,
        ));
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        engine.set_sync_state("old", SyncState::Dirty).unwrap();
        engine.set_pin_state("old", PinState::Pinned).unwrap();
        let mut retained = engine.item("old").unwrap().unwrap();

        if deletion_first {
            drive.delete("old");
        }
        drive.upsert(item("new", Some("root"), "report.txt", ItemKind::File));
        if !deletion_first {
            drive.delete("old");
        }
        drive.fail_after(1, CloudError::Unavailable);
        assert!(engine.sync().await.is_err());
        drop(engine);

        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        assert_eq!(
            engine.resolve_path("/report.txt").unwrap().unwrap().item_id,
            "new"
        );
        retained.name = "report (conflict 2).txt".into();
        assert_eq!(
            engine.resolve_path("/report (conflict 2).txt").unwrap(),
            Some(retained.clone())
        );
        assert_eq!(
            engine
                .resolve_path("/report (conflict).txt")
                .unwrap()
                .unwrap()
                .item_id,
            "sibling"
        );

        drop(engine);
        drive.expire_tokens();
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        assert_eq!(engine.getattr(retained.inode).unwrap(), Some(retained));
        assert_eq!(engine.list_children("root").unwrap().len(), 3);

        drive.upsert(item(
            "newer",
            Some("root"),
            "report (conflict 2).txt",
            ItemKind::File,
        ));
        engine.sync().await.unwrap();
        assert_eq!(
            engine
                .resolve_path("/report (conflict 2).txt")
                .unwrap()
                .unwrap()
                .item_id,
            "newer"
        );
        let old_path = engine.path_for("old").unwrap().unwrap();
        assert_eq!(
            engine.resolve_path(old_path).unwrap().unwrap().item_id,
            "old"
        );
        assert_eq!(engine.list_children("root").unwrap().len(), 4);
    }
}

#[tokio::test]
async fn full_resync_replacement_preserves_nested_dirty_paths_and_pins() {
    use onedrive_core::{PinState, SyncState};

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("old", Some("root"), "Folder.v1", ItemKind::Folder));
    drive.upsert(item("nested", Some("old"), "Nested", ItemKind::Folder));
    drive.upsert(item("dirty", Some("nested"), "edit.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    engine.set_sync_state("dirty", SyncState::Dirty).unwrap();
    engine.set_pin_state("old", PinState::Pinned).unwrap();
    engine
        .set_pin_state("dirty", PinState::UnpinOverride)
        .unwrap();
    let mut old = engine.item("old").unwrap().unwrap();
    let nested = engine.item("nested").unwrap().unwrap();
    let dirty = engine.item("dirty").unwrap().unwrap();

    drive.delete("old");
    drive.upsert(item("new", Some("root"), "Folder.v1", ItemKind::Folder));
    drive.upsert(item("new-nested", Some("new"), "Nested", ItemKind::Folder));
    drive.upsert(item(
        "new-file",
        Some("new-nested"),
        "edit.txt",
        ItemKind::File,
    ));
    drive.expire_tokens();
    engine.sync().await.unwrap();
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert_eq!(
        engine
            .resolve_path("/Folder.v1/Nested/edit.txt")
            .unwrap()
            .unwrap()
            .item_id,
        "new-file"
    );
    old.name = "Folder.v1 (conflict)".into();
    assert_eq!(
        engine.resolve_path("/Folder.v1 (conflict)").unwrap(),
        Some(old)
    );
    assert_eq!(
        engine.resolve_path("/Folder.v1 (conflict)/Nested").unwrap(),
        Some(nested)
    );
    assert_eq!(
        engine
            .resolve_path("/Folder.v1 (conflict)/Nested/edit.txt")
            .unwrap(),
        Some(dirty)
    );
}

#[tokio::test]
async fn remote_rename_across_pages_releases_a_name_before_displacing_a_dirty_item() {
    use onedrive_core::SyncState;

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("local", Some("root"), "a.txt", ItemKind::File));
    drive.upsert(item("remote", Some("root"), "b.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    engine.set_sync_state("local", SyncState::Dirty).unwrap();
    let local = engine.item("local").unwrap().unwrap();

    drive.upsert(item("remote", Some("root"), "a.txt", ItemKind::File));
    drive.upsert(item("remote", Some("root"), "c.txt", ItemKind::File));
    engine.sync().await.unwrap();

    assert_eq!(engine.resolve_path("/a.txt").unwrap(), Some(local));
    assert_eq!(
        engine.resolve_path("/c.txt").unwrap().unwrap().item_id,
        "remote"
    );
    assert_eq!(engine.list_children("root").unwrap().len(), 2);
}

#[tokio::test]
async fn replacement_gives_each_retained_dirty_version_its_own_sibling_name() {
    use onedrive_core::SyncState;

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("first", Some("root"), "edit.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    engine.set_sync_state("first", SyncState::Dirty).unwrap();

    drive.upsert(item("second", Some("root"), "edit.txt", ItemKind::File));
    drive.delete("first");
    drive.fail_after(1, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    assert!(engine.set_sync_state("second", SyncState::Dirty).unwrap());

    drive.delete("second");
    drive.upsert(item("current", Some("root"), "edit.txt", ItemKind::File));
    drive.expire_tokens();
    engine.sync().await.unwrap();

    for (name, item_id, state) in [
        ("edit.txt", "current", SyncState::Clean),
        ("edit (conflict).txt", "first", SyncState::Dirty),
        ("edit (conflict 2).txt", "second", SyncState::Dirty),
    ] {
        let entry = engine.lookup("root", name).unwrap().unwrap();
        assert_eq!(entry.item_id, item_id);
        assert_eq!(entry.sync_state, state);
    }
    assert_eq!(engine.list_children("root").unwrap().len(), 3);
}
