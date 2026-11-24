use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use onedrive_core::{
    Clock, CloudDrive, CloudError, DeltaPage, DeltaRequest, Engine, FakeCloudDrive, ItemKind,
    RemoteItem, SystemClock,
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
    sleeps: tokio::sync::mpsc::UnboundedSender<std::time::Duration>,
}

#[async_trait::async_trait]
impl onedrive_core::Clock for FakeClock {
    fn now(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    async fn sleep(&self, duration: std::time::Duration) {
        self.sleeps.send(self.now() + duration).unwrap();
        tokio::time::sleep(duration).await;
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_survives_cancellation_and_restart_before_retrying_the_same_link() {
    use std::time::Duration;

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("a", Some("root"), "a.txt", ItemKind::File));
    struct TimedDelta {
        drive: Arc<FakeCloudDrive>,
        clock: Arc<FakeClock>,
        requests: std::sync::Mutex<Vec<(DeltaRequest, Duration)>>,
    }

    #[async_trait::async_trait]
    impl CloudDrive for TimedDelta {
        async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
            self.requests
                .lock()
                .unwrap()
                .push((request.clone(), self.clock.now()));
            self.drive.delta(request).await
        }
    }

    let (sleeps, mut backoffs) = tokio::sync::mpsc::unbounded_channel();
    let clock = Arc::new(FakeClock {
        start: tokio::time::Instant::now(),
        sleeps,
    });
    let database = temp.path().join("metadata.db");
    let engine = Arc::new(Engine::open(&database, drive.clone(), clock.clone()).unwrap());
    let wait = Duration::from_millis(30_500);
    drive.fail_after(1, CloudError::Throttled { retry_after: wait });
    tokio::time::advance(Duration::from_micros(500)).await;

    let worker = tokio::spawn({
        let engine = engine.clone();
        async move { engine.sync().await }
    });
    assert_eq!(
        backoffs.recv().await,
        Some(wait + Duration::from_micros(500))
    );
    let requests = drive.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(requests[1], DeltaRequest::Link(_)));
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(engine);

    let elapsed = Duration::from_millis(10_250);
    tokio::time::advance(elapsed).await;
    let timed = Arc::new(TimedDelta {
        drive: drive.clone(),
        clock: clock.clone(),
        requests: std::sync::Mutex::new(Vec::new()),
    });
    let engine = Engine::open(&database, timed.clone(), clock).unwrap();
    engine.sync().await.unwrap();
    let deadline = wait + Duration::from_millis(1);
    assert_eq!(backoffs.try_recv().unwrap(), deadline);
    let retried = timed.requests.lock().unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].0, requests[1]);
    assert!(retried[0].1 >= deadline);
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
        let mut stub = item("stub", Some("root"), "unused", ItemKind::File);
        stub.name = None;
        drive.upsert(stub);
        drop(engine);

        for (page, winner) in [if deletion_first { "old" } else { "new" }, "new"]
            .into_iter()
            .enumerate()
        {
            let engine = open(&temp, &drive);
            drive.fail_after(1, CloudError::Unavailable);
            assert!(engine.sync().await.is_err());
            drop(engine);

            let engine = open(&temp, &drive);
            let requests = drive.requests().len();
            assert_eq!(
                engine.resolve_path("/report.txt").unwrap().unwrap().item_id,
                winner,
            );
            assert_directory_round_trips(
                &engine,
                "root",
                Path::new("/"),
                if deletion_first && page == 0 {
                    &["old", "sibling"]
                } else {
                    &["old", "new", "sibling"]
                },
            );
            let old = engine.item("old").unwrap().unwrap();
            assert_eq!(
                retained,
                onedrive_core::CatalogItem {
                    name: retained.name.clone(),
                    ..old
                }
            );
            assert_eq!(drive.requests().len(), requests);
        }
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        assert_directory_round_trips(&engine, "root", Path::new("/"), &["old", "new", "sibling"]);
        assert_eq!(
            engine.resolve_path("/report.txt").unwrap().unwrap().item_id,
            "new"
        );
        retained.name = "report (conflict 2).txt".into();
        retained.intended_name = retained.name.clone();
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
    drive.fail_after(3, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    drop(engine);

    let engine = open(&temp, &drive);
    assert_eq!(
        engine.resolve_path("/Folder.v1").unwrap().unwrap().item_id,
        "new"
    );
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["old", "new"]);
    let old_path = engine.path_for("old").unwrap().unwrap();
    assert_directory_round_trips(&engine, "old", &old_path, &["nested"]);
    assert_directory_round_trips(&engine, "nested", &old_path.join("Nested"), &["dirty"]);
    assert_eq!(
        engine
            .resolve_path(old_path.join("Nested/edit.txt"))
            .unwrap(),
        Some(dirty.clone())
    );

    engine.sync().await.unwrap();
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["old", "new"]);
    assert_eq!(
        engine
            .resolve_path("/Folder.v1/Nested/edit.txt")
            .unwrap()
            .unwrap()
            .item_id,
        "new-file"
    );
    old.name = "Folder.v1 (conflict)".into();
    old.intended_name = old.name.clone();
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

fn assert_directory_round_trips(engine: &Engine, parent: &str, path: &Path, expected: &[&str]) {
    let children = engine.list_children(parent).unwrap();
    assert_eq!(children.len(), expected.len());
    assert_eq!(
        children
            .iter()
            .map(|child| child.item_id.as_str())
            .collect::<HashSet<_>>(),
        expected.iter().copied().collect(),
    );
    let mut names = HashSet::new();
    for child in children {
        assert!(
            names.insert(child.name.clone()),
            "duplicate name: {}",
            child.name
        );
        let child_path = path.join(&child.name);
        assert_eq!(
            engine.lookup(parent, &child.name).unwrap().as_ref(),
            Some(&child)
        );
        assert_eq!(engine.item(&child.item_id).unwrap().as_ref(), Some(&child));
        assert_eq!(engine.getattr(child.inode).unwrap().as_ref(), Some(&child));
        assert_eq!(
            engine.path_for(&child.item_id).unwrap().as_ref(),
            Some(&child_path)
        );
        assert_eq!(
            engine.resolve_path(child_path).unwrap().as_ref(),
            Some(&child)
        );
    }
}

#[tokio::test]
async fn clean_swaps_assign_canonical_names_after_each_committed_page() {
    for (first, second, from, to) in [("a", "b", "a.txt", "b.txt"), ("b", "a", "b.txt", "a.txt")] {
        let temp = TempDir::new().unwrap();
        let drive = drive(1);
        drive.upsert(item(first, Some("root"), from, ItemKind::File));
        drive.upsert(item(second, Some("root"), to, ItemKind::File));
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        let second_inode = engine.item(second).unwrap().unwrap().inode;

        drive.upsert(item(first, Some("root"), to, ItemKind::File));
        drive.upsert(item(second, Some("root"), from, ItemKind::File));
        let mut stub = item("stub", Some("root"), "unused", ItemKind::File);
        stub.name = None;
        drive.upsert(stub);
        drop(engine);

        for expected_second_name in [None, Some(from)] {
            let engine = open(&temp, &drive);
            drive.fail_after(1, CloudError::Unavailable);
            assert!(engine.sync().await.is_err());
            drop(engine);
            let engine = open(&temp, &drive);
            let second_item = engine.item(second).unwrap().unwrap();
            assert_eq!(second_item.inode, second_inode);
            if let Some(name) = expected_second_name {
                assert_eq!(second_item.name, name);
            } else {
                assert_ne!(second_item.name, to);
                assert_ne!(second_item.name, from);
            }
            assert_eq!(engine.lookup("root", to).unwrap().unwrap().item_id, first);
            assert_directory_round_trips(&engine, "root", Path::new("/"), &["a", "b"]);
        }
        let engine = open(&temp, &drive);
        engine.sync().await.unwrap();
        assert_eq!(
            engine.lookup("root", from).unwrap().unwrap().item_id,
            second
        );
        assert_directory_round_trips(&engine, "root", Path::new("/"), &["a", "b"]);
    }
}

#[tokio::test]
async fn expired_fresh_continuations_stop_and_next_sync_starts_over() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use onedrive_core::{CloudDrive, DeltaPage, EngineError, PinState, SyncState};

    struct ExpiringContinuations {
        drive: Arc<FakeCloudDrive>,
        enabled: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CloudDrive for ExpiringContinuations {
        async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
            let expire =
                self.enabled.load(Ordering::Relaxed) && matches!(request, DeltaRequest::Link(_));
            let page = self.drive.delta(request).await;
            if expire {
                tokio::task::yield_now().await;
                Err(CloudError::ResyncRequired)
            } else {
                page
            }
        }
    }

    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("local", Some("root"), "local.txt", ItemKind::File));
    let cloud = Arc::new(ExpiringContinuations {
        drive: drive.clone(),
        enabled: AtomicBool::new(false),
    });
    let open = || {
        Engine::open(
            temp.path().join("metadata.db"),
            cloud.clone(),
            Arc::new(SystemClock),
        )
        .unwrap()
    };
    let engine = open();
    engine.sync().await.unwrap();
    engine.set_sync_state("local", SyncState::Dirty).unwrap();
    engine.set_pin_state("local", PinState::Pinned).unwrap();
    let retained = engine.item("local").unwrap();
    drive.delete("local");
    drive.upsert(item("new", Some("root"), "new.txt", ItemKind::File));

    let before = drive.requests().len();
    cloud.enabled.store(true, Ordering::Relaxed);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), engine.sync()).await,
        Ok(Err(EngineError::Cloud(CloudError::ResyncRequired))),
    ));
    let requests = drive.requests();
    assert!(matches!(
        &requests[before..],
        [
            DeltaRequest::Link(_),
            DeltaRequest::Initial,
            DeltaRequest::Link(_),
            DeltaRequest::Initial,
            DeltaRequest::Link(_)
        ]
    ));
    assert_eq!(engine.item("local").unwrap(), retained);

    drop(engine);
    cloud.enabled.store(false, Ordering::Relaxed);
    let engine = open();
    engine.sync().await.unwrap();
    assert_eq!(drive.requests()[requests.len()], DeltaRequest::Initial);
    assert_eq!(engine.item("local").unwrap(), retained);
    assert_eq!(
        engine.lookup("root", "new.txt").unwrap().unwrap().item_id,
        "new"
    );
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["local", "new"]);
}

#[test]
fn legacy_catalog_reopens_with_unique_names_and_preserved_local_state() {
    use onedrive_core::{PinState, SyncState};

    let temp = TempDir::new().unwrap();
    let db = rusqlite::Connection::open(temp.path().join("metadata.db")).unwrap();
    db.execute_batch(include_str!("../src/schema.sql")).unwrap();
    db.execute_batch(
        "INSERT INTO items (inode, item_id, parent_id, name, kind, size, mtime, etag, sync_state)
         VALUES (1, 'root', NULL, 'root', 1, 0, 0, NULL, 0),
                (2, 'old', 'root', 'report.txt', 0, 42, 123, 'old-version', 1),
                (3, 'new', 'root', 'report.txt', 0, 43, 456, 'new-version', 0);
         INSERT INTO pin_directives VALUES ('old', 1);",
    )
    .unwrap();
    drop(db);

    let drive = drive(1);
    let engine = open(&temp, &drive);
    assert_eq!(
        engine
            .lookup("root", "report.txt")
            .unwrap()
            .unwrap()
            .item_id,
        "new"
    );
    let old = engine.getattr(2).unwrap().unwrap();
    assert_eq!(
        (
            old.size,
            old.mtime,
            old.etag.as_deref(),
            old.sync_state,
            old.pin_state
        ),
        (
            42,
            123,
            Some("old-version"),
            SyncState::Dirty,
            PinState::Pinned
        ),
    );
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["old", "new"]);
    drop(engine);
    let engine = open(&temp, &drive);
    assert_eq!(engine.getattr(2).unwrap(), Some(old));
    assert!(drive.requests().is_empty());
}

#[tokio::test]
async fn remote_claim_can_take_over_a_temporary_alias() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    let name = format!("{}.txt", "é".repeat(125));
    drive.upsert(item("old", Some("root"), &name, ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();

    drive.upsert(item("new", Some("root"), &name, ItemKind::File));
    drive.delete("old");
    drive.fail_after(1, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    let alias = engine.item("old").unwrap().unwrap().name;
    assert!(alias.len() <= 255);
    assert!(alias.ends_with(".txt"));
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["old", "new"]);
    // The restarted scan can claim the alias before pruning the old item.
    drive.upsert(item("alias-claim", Some("root"), &alias, ItemKind::File));
    drive.expire_tokens();
    drive.fail_after(3, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    assert_eq!(
        engine.lookup("root", &alias).unwrap().unwrap().item_id,
        "alias-claim"
    );
    assert_ne!(engine.item("old").unwrap().unwrap().name, alias);
    assert_directory_round_trips(
        &engine,
        "root",
        Path::new("/"),
        &["old", "new", "alias-claim"],
    );
    engine.sync().await.unwrap();
    assert!(engine.item("old").unwrap().is_none());
    assert_directory_round_trips(&engine, "root", Path::new("/"), &["new", "alias-claim"]);
}

#[tokio::test]
async fn remote_reparent_preserves_a_queued_folder_path_across_pages_and_reopen() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("a", Some("root"), "A", ItemKind::Folder));
    drive.upsert(item("b", Some("a"), "B", ItemKind::Folder));
    drive.upsert(item("file", Some("b"), "file.txt", ItemKind::File));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let a = engine.item("a").unwrap().unwrap().inode;
    let b = engine.item("b").unwrap().unwrap().inode;
    engine.rename(a, "B", a, "Queued", true).unwrap();
    let queued = engine.pending_upload_paths().unwrap();
    let local_folder = engine.item("b").unwrap().unwrap();

    drive.upsert(item("b", Some("root"), "B", ItemKind::Folder));
    drive.upsert(item("a", Some("b"), "A", ItemKind::Folder));
    drive.fail_after(1, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert_eq!(engine.pending_upload_paths().unwrap(), queued);
    assert_eq!(
        engine.resolve_path("/A/Queued").unwrap(),
        Some(local_folder)
    );
    assert_eq!(
        engine
            .resolve_path("/A/Queued/file.txt")
            .unwrap()
            .unwrap()
            .item_id,
        "file"
    );

    // A later local move frees the held edge.
    engine.rename(a, "Queued", 1, "Queued", true).unwrap();
    engine.sync().await.unwrap();
    assert_eq!(engine.resolve_path("/Queued/A").unwrap().unwrap().inode, a);
    assert_eq!(engine.resolve_path("/Queued").unwrap().unwrap().inode, b);
    assert_eq!(
        engine.pending_upload_paths().unwrap()[0].0,
        Path::new("/Queued")
    );
}

#[tokio::test]
async fn reversed_remote_edges_are_retried_after_the_later_page() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("a", Some("root"), "A", ItemKind::Folder));
    drive.upsert(item("b", Some("a"), "B", ItemKind::Folder));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();

    drive.upsert(item("a", Some("b"), "A", ItemKind::Folder));
    drive.upsert(item("b", Some("root"), "B", ItemKind::Folder));
    drive.fail_after(1, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    assert!(engine.resolve_path("/A/B").unwrap().is_some());
    drop(engine);

    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    assert_eq!(engine.resolve_path("/B/A").unwrap().unwrap().item_id, "a");
    assert!(engine.resolve_path("/A").unwrap().is_none());
}

#[tokio::test]
async fn desktop_changes_report_alias_displacement_and_folder_renames() {
    let temp = TempDir::new().unwrap();
    let drive = drive(1);
    drive.upsert(item("old", Some("root"), "report.txt", ItemKind::File));
    drive.upsert(item("folder", Some("root"), "Folder", ItemKind::Folder));
    let engine = open(&temp, &drive);
    engine.sync().await.unwrap();
    let old = engine.item("old").unwrap().unwrap();
    let folder = engine.item("folder").unwrap().unwrap();
    engine
        .set_sync_state("old", onedrive_core::SyncState::Dirty)
        .unwrap();
    let cursor = engine.desktop_changes(None, None).unwrap();

    drive.upsert(item("new", Some("root"), "report.txt", ItemKind::File));
    drive.delete("old");
    drive.fail_after(1, CloudError::Unavailable);
    assert!(engine.sync().await.is_err());
    let changes = engine
        .desktop_changes(Some(&cursor.epoch), Some(cursor.revision))
        .unwrap();
    assert!(changes.inodes.unwrap().contains(&(old.inode, false)));
    assert_ne!(engine.item("old").unwrap().unwrap().name, "report.txt");

    engine.sync().await.unwrap();
    drive.upsert(item("folder", Some("root"), "Renamed", ItemKind::Folder));
    engine.sync().await.unwrap();
    let after_rename = engine
        .desktop_changes(Some(&changes.epoch), Some(changes.revision))
        .unwrap();
    assert!(after_rename.inodes.unwrap().contains(&(folder.inode, true)));
}
