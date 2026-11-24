//! Bounded, transaction-local catalog change history for desktop status observers.

use rusqlite::{Connection, params};

use super::{Engine, EngineError};

const HISTORY: i64 = 512;

pub(super) fn install(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(&format!(
        "CREATE TEMP TABLE desktop_epoch AS SELECT lower(hex(randomblob(16))) AS token;
         CREATE TEMP TABLE desktop_history (
             revision INTEGER PRIMARY KEY, inode INTEGER NOT NULL, subtree INTEGER NOT NULL
         );
         CREATE TEMP VIEW desktop_change AS SELECT inode, subtree FROM desktop_history;
         CREATE TEMP TRIGGER desktop_record INSTEAD OF INSERT ON desktop_change
         WHEN NEW.inode IS NOT NULL BEGIN
             INSERT INTO desktop_history
                 VALUES ((SELECT coalesce(max(revision), 0) + 1 FROM desktop_history),
                         NEW.inode, NEW.subtree);
             DELETE FROM desktop_history WHERE revision = last_insert_rowid() - {HISTORY};
         END;
         CREATE TEMP TRIGGER desktop_pin_added AFTER INSERT ON main.pin_directives BEGIN
             INSERT INTO desktop_change SELECT inode, kind FROM items WHERE item_id = NEW.item_id;
         END;
         CREATE TEMP TRIGGER desktop_pin_changed AFTER UPDATE ON main.pin_directives
         WHEN OLD.pin_state IS NOT NEW.pin_state BEGIN
             INSERT INTO desktop_change SELECT inode, kind FROM items WHERE item_id = NEW.item_id;
         END;
         CREATE TEMP TRIGGER desktop_pin_removed AFTER DELETE ON main.pin_directives BEGIN
             INSERT INTO desktop_change SELECT inode, kind FROM items WHERE item_id = OLD.item_id;
         END;
         CREATE TEMP TRIGGER desktop_item_added AFTER INSERT ON main.items BEGIN
             INSERT INTO desktop_change SELECT inode, 0 FROM items WHERE item_id = NEW.parent_id;
         END;
         CREATE TEMP TRIGGER desktop_item_removed AFTER DELETE ON main.items BEGIN
             INSERT INTO desktop_change VALUES (OLD.inode, OLD.kind);
             INSERT INTO desktop_change SELECT inode, 0 FROM items WHERE item_id = OLD.parent_id;
         END;
         CREATE TEMP TRIGGER desktop_item_changed
         AFTER UPDATE OF sync_state, parent_id, size, kind, alias, name ON main.items
         WHEN (CASE OLD.sync_state WHEN {uploading} THEN {dirty} ELSE OLD.sync_state END)
                IS NOT (CASE NEW.sync_state WHEN {uploading} THEN {dirty} ELSE NEW.sync_state END)
              OR OLD.parent_id IS NOT NEW.parent_id OR OLD.size IS NOT NEW.size
              OR OLD.kind IS NOT NEW.kind
              OR coalesce(OLD.alias, OLD.name) IS NOT coalesce(NEW.alias, NEW.name)
         BEGIN
             INSERT INTO desktop_change VALUES (NEW.inode, NEW.kind AND (
                 OLD.parent_id IS NOT NEW.parent_id
                 OR coalesce(OLD.alias, OLD.name) IS NOT coalesce(NEW.alias, NEW.name)
             ));
             INSERT INTO desktop_change
                 SELECT inode, 0 FROM items WHERE item_id = OLD.parent_id
                 AND OLD.parent_id IS NOT NEW.parent_id;
         END;
         CREATE TEMP TRIGGER desktop_cache_added AFTER INSERT ON main.sparse_files BEGIN
             INSERT INTO desktop_change SELECT inode, 0 FROM items WHERE inode = NEW.inode AND size = 0;
         END;
         CREATE TEMP TRIGGER desktop_cache_removed AFTER DELETE ON main.sparse_files BEGIN
             INSERT INTO desktop_change VALUES (OLD.inode, 0);
         END;
         CREATE TEMP TRIGGER desktop_cache_block AFTER INSERT ON main.sparse_ranges BEGIN
             INSERT INTO desktop_change
                 SELECT inode, 0 FROM sparse_files JOIN items USING (inode)
                 WHERE generation = NEW.generation AND (NEW.block >= 0 OR size <= {header});
         END;",
        header = super::sparse_cache::HEADER_BYTES,
        uploading = super::SyncState::Uploading as i64,
        dirty = super::SyncState::Dirty as i64,
    ))
}

pub struct DesktopChanges {
    pub epoch: String,
    pub revision: i64,
    pub inodes: Option<Vec<(u64, bool)>>,
}

impl Engine {
    /// Return a cursor and changed inodes, marking changes that may affect a
    /// whole subtree. A missing list asks the observer to recheck its live items.
    pub fn desktop_changes(
        &self,
        epoch: Option<&str>,
        since: Option<i64>,
    ) -> Result<DesktopChanges, EngineError> {
        let db = self.db();
        let (current_epoch, revision): (String, i64) = db.query_row(
            "SELECT token, (SELECT coalesce(max(revision), 0) FROM desktop_history)
             FROM desktop_epoch",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut changes = DesktopChanges {
            epoch: current_epoch,
            revision,
            inodes: None,
        };
        let Some(since) = since.filter(|since| {
            epoch == Some(changes.epoch.as_str())
                && (revision.saturating_sub(HISTORY).max(0)..=revision).contains(since)
        }) else {
            return Ok(changes);
        };
        if since == revision {
            changes.inodes = Some(Vec::new());
            return Ok(changes);
        }
        // A file's hydration can change the aggregate state of any ancestor.
        // UNION deduplicates the shared parent chains and handles catalog cycles.
        let mut select = db.prepare_cached(
            "WITH RECURSIVE affected(inode, subtree) AS (
                 SELECT inode, subtree FROM desktop_history WHERE revision > ?1
                 UNION
                 SELECT parent.inode, 0 FROM affected
                     JOIN items AS child USING (inode)
                     JOIN items AS parent ON parent.item_id = child.parent_id
             )
             SELECT inode, max(subtree) FROM affected GROUP BY inode ORDER BY inode LIMIT ?2",
        )?;
        let inodes = select
            .query_map(params![since, HISTORY + 1], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        changes.inodes = (inodes.len() <= HISTORY as usize).then_some(inodes);
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{FakeCloudDrive, ItemKind, RemoteItem, SyncState, SystemClock};

    use super::*;

    #[tokio::test]
    async fn reports_state_changes_and_recovers_from_a_stale_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let cloud = Arc::new(FakeCloudDrive::new(10));
        let root = RemoteItem {
            item_id: "root".into(),
            parent_id: None,
            name: Some("root".into()),
            kind: ItemKind::Folder,
            size: 0,
            mtime: 1,
            etag: None,
            ctag: None,
            quickxor_hash: None,
        };
        cloud.upsert(root.clone());
        cloud.upsert(RemoteItem {
            item_id: "file".into(),
            parent_id: Some(root.item_id.clone()),
            name: Some("file".into()),
            kind: ItemKind::File,
            size: 1,
            ..root
        });
        let engine =
            Engine::open(temp.path().join("catalog"), cloud, Arc::new(SystemClock)).unwrap();
        engine.sync().await.unwrap();
        let inode = engine.item("file").unwrap().unwrap().inode;
        let initial = engine.desktop_changes(None, None).unwrap();
        assert!(initial.inodes.is_none());
        let read = |revision| {
            engine
                .desktop_changes(Some(&initial.epoch), Some(revision))
                .unwrap()
        };
        engine.set_sync_state("file", SyncState::Dirty).unwrap();
        let dirty = read(initial.revision);
        assert_eq!(dirty.inodes, Some(vec![(1, false), (inode, false)]));
        engine.set_sync_state("file", SyncState::Uploading).unwrap();
        assert_eq!(read(dirty.revision).inodes, Some(Vec::new()));
        engine.set_sync_state("file", SyncState::Conflict).unwrap();
        assert_eq!(
            read(dirty.revision).inodes,
            Some(vec![(1, false), (inode, false)])
        );

        for _ in 0..HISTORY {
            engine.set_sync_state("file", SyncState::Clean).unwrap();
            engine.set_sync_state("file", SyncState::Dirty).unwrap();
        }
        assert!(read(initial.revision).inodes.is_none());
        let recent = read(initial.revision).revision;
        assert_eq!(
            read(recent - 1).inodes,
            Some(vec![(1, false), (inode, false)])
        );
        assert!(
            engine
                .desktop_changes(Some("old daemon"), Some(recent))
                .unwrap()
                .inodes
                .is_none()
        );
    }
}
