use std::sync::Arc;

use rusqlite::Connection;

#[derive(Clone)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) enum CatalogInvalidation {
    Inode { inode: u64, content: bool },
    Entry { parent: u64, name: String },
}

pub(super) type Observer = dyn Fn(&[CatalogInvalidation]) + Send + Sync;
pub(crate) type CatalogObserver = Arc<Observer>;

pub(super) fn install(db: &Connection) -> rusqlite::Result<()> {
    // Capture changes at the catalog boundary, including rows pruned at the
    // end of a full enumeration and old names lost during rename or move.
    db.execute_batch(
        "CREATE TEMP TABLE changed_inodes (inode INTEGER, content INTEGER);
         CREATE TEMP TABLE changed_entries (parent_id TEXT, name TEXT);
         CREATE TEMP TRIGGER catalog_added AFTER INSERT ON main.items BEGIN
             INSERT INTO changed_entries VALUES (NEW.parent_id, coalesce(NEW.alias, NEW.name));
         END;
         CREATE TEMP TRIGGER catalog_removed AFTER DELETE ON main.items BEGIN
             INSERT INTO changed_inodes VALUES (OLD.inode, 1);
             INSERT INTO changed_entries VALUES (OLD.parent_id, coalesce(OLD.alias, OLD.name));
         END;
         CREATE TEMP TRIGGER catalog_attributes
         AFTER UPDATE OF parent_id, name, alias, kind, size, mtime, etag, ctag ON main.items
         WHEN OLD.parent_id IS NOT NEW.parent_id
              OR coalesce(OLD.alias, OLD.name) IS NOT coalesce(NEW.alias, NEW.name)
              OR OLD.kind IS NOT NEW.kind OR OLD.size IS NOT NEW.size
              OR OLD.mtime IS NOT NEW.mtime OR OLD.etag IS NOT NEW.etag
              OR OLD.ctag IS NOT NEW.ctag
         BEGIN
             INSERT INTO changed_inodes VALUES (
                 NEW.inode, OLD.kind IS NOT NEW.kind OR OLD.size IS NOT NEW.size
                            OR OLD.mtime IS NOT NEW.mtime OR OLD.ctag IS NOT NEW.ctag
             );
         END;
         CREATE TEMP TRIGGER catalog_moved AFTER UPDATE OF parent_id, name, alias ON main.items
         WHEN OLD.parent_id IS NOT NEW.parent_id
              OR coalesce(OLD.alias, OLD.name) IS NOT coalesce(NEW.alias, NEW.name)
         BEGIN
             INSERT INTO changed_entries VALUES (OLD.parent_id, coalesce(OLD.alias, OLD.name));
             INSERT INTO changed_entries VALUES (NEW.parent_id, coalesce(NEW.alias, NEW.name));
         END;",
    )
}

pub(super) fn collect(db: &Connection) -> rusqlite::Result<Vec<CatalogInvalidation>> {
    let mut inodes = db.prepare("SELECT inode, max(content) FROM changed_inodes GROUP BY inode")?;
    let mut changes = inodes
        .query_map([], |row| {
            Ok(CatalogInvalidation::Inode {
                inode: row.get(0)?,
                content: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut entries = db.prepare(
        "SELECT DISTINCT items.inode, changed_entries.name
         FROM changed_entries JOIN items ON items.item_id = changed_entries.parent_id",
    )?;
    for entry in entries.query_map([], |row| {
        Ok(CatalogInvalidation::Entry {
            parent: row.get(0)?,
            name: row.get(1)?,
        })
    })? {
        changes.push(entry?);
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{
        CloudError, Engine, EngineError, FakeCloudDrive, ItemKind, RemoteItem, SyncState,
        SystemClock,
    };

    #[tokio::test]
    async fn dirty_replacement_notifies_committed_alias_and_conflict_names() {
        let temp = tempfile::tempdir().unwrap();
        let drive = Arc::new(FakeCloudDrive::new(1));
        let root = RemoteItem {
            item_id: "root".into(),
            parent_id: None,
            name: Some("root".into()),
            kind: ItemKind::Folder,
            size: 0,
            mtime: 0,
            etag: None,
            ctag: None,
            quickxor_hash: None,
        };
        let old = RemoteItem {
            item_id: "old".into(),
            parent_id: Some("root".into()),
            name: Some("report.txt".into()),
            kind: ItemKind::File,
            ..root.clone()
        };
        drive.upsert(root);
        drive.upsert(old.clone());
        let engine = Arc::new(
            Engine::open(
                temp.path().join("catalog"),
                drive.clone(),
                Arc::new(SystemClock),
            )
            .unwrap(),
        );
        engine.sync().await.unwrap();
        engine.set_sync_state("old", SyncState::Dirty).unwrap();
        let old_inode = engine.item("old").unwrap().unwrap().inode;

        let notifications = Arc::new(Mutex::new(Vec::new()));
        let reported = notifications.clone();
        let catalog = Arc::downgrade(&engine);
        let observer: CatalogObserver = Arc::new(move |changes| {
            // Read the catalog after the transaction releases its lock.
            let catalog = catalog.upgrade().unwrap();
            let old = catalog.item("old").unwrap().unwrap();
            let replacement = catalog.lookup("root", "report.txt").unwrap().unwrap();
            reported
                .lock()
                .unwrap()
                .push((changes.to_vec(), old.name, replacement.item_id));
        });
        engine.observe_catalog(&observer);

        drive.upsert(RemoteItem {
            item_id: "replacement".into(),
            ..old
        });
        drive.delete("old");
        drive.fail_after(1, CloudError::Unavailable);
        assert!(matches!(
            engine.sync().await,
            Err(EngineError::Cloud(CloudError::Unavailable))
        ));
        let first_entries = engine.list_directory_entries("root").unwrap();
        let replacement_inode = engine.item("replacement").unwrap().unwrap().inode;
        assert_eq!(
            first_entries,
            [
                (old_inode, ItemKind::File, "report (pending).txt".into()),
                (replacement_inode, ItemKind::File, "report.txt".into()),
            ]
        );
        engine.sync().await.unwrap();

        let notifications = notifications.lock().unwrap();
        assert_eq!(notifications.len(), 2);
        for ((changes, visible, winner), (before, after)) in notifications.iter().zip([
            ("report.txt", "report (pending).txt"),
            ("report (pending).txt", "report (conflict).txt"),
        ]) {
            assert_eq!(visible, after);
            assert_eq!(winner, "replacement");
            assert_eq!(changes.len(), 3);
            assert!(changes.contains(&CatalogInvalidation::Inode {
                inode: old_inode,
                content: false
            }));
            for name in [before, after] {
                assert!(changes.contains(&CatalogInvalidation::Entry {
                    parent: 1,
                    name: name.into()
                }));
            }
        }
    }
}
