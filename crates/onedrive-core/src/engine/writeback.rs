use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use tokio::sync::Notify;

use super::{
    CatalogItem, Engine, EngineError, ItemKind, SyncState, item_by_id, item_by_inode, lookup,
};
use crate::{
    CloudError, DeltaChange, DeltaCursor, DeltaRequest, RemoteItem, UploadSource, UploadTarget,
};

pub(super) struct Writeback {
    root: PathBuf,
    changed: Arc<Notify>,
    uploading: tokio::sync::Mutex<()>,
    open_files: Mutex<HashMap<u64, usize>>,
}

impl Writeback {
    pub(super) fn staging_directory(database: &Path) -> PathBuf {
        database.with_added_extension("staging")
    }

    pub(super) fn open(database: &Path, db: &mut Connection) -> Result<Self, EngineError> {
        let root = Self::staging_directory(database);
        fs::create_dir_all(&root)?;
        // Persist the directory entry before any transaction can reference it.
        File::open(
            root.parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let referenced = tx
            .prepare("SELECT stage FROM writeback_queue WHERE stage IS NOT NULL UNION SELECT stage FROM writeback_attempts WHERE stage IS NOT NULL")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<HashSet<_>, _>>()?;
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("stage-") && !referenced.contains(name))
            {
                fs::remove_file(entry.path())?;
            }
        }
        tx.commit()?;
        Ok(Self {
            root,
            changed: Arc::new(Notify::new()),
            uploading: tokio::sync::Mutex::new(()),
            open_files: Mutex::default(),
        })
    }

    fn stage(
        &self,
        previous: Option<&str>,
        prefix: &[u8],
        size: u64,
        write: Option<(u64, &[u8])>,
    ) -> Result<tempfile::NamedTempFile, EngineError> {
        let mut file = tempfile::Builder::new()
            .prefix("stage-")
            .tempfile_in(&self.root)?;
        if let Some(previous) = previous {
            let old = File::open(self.root.join(previous))?;
            if rustix::fs::ioctl_ficlone(file.as_file(), &old).is_err() {
                io::copy(&mut old.take(size), &mut file)?;
            }
        } else {
            file.write_all(prefix)?;
        }
        file.as_file().set_len(size)?;
        if let Some((offset, bytes)) = write {
            file.as_file().write_all_at(bytes, offset)?;
        }
        file.as_file().sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(file)
    }

    fn retire(&self, name: Option<&str>) {
        if let Some(name) = name {
            // A failed cleanup can be retried on the next open.
            let _ = fs::remove_file(self.root.join(name));
        }
    }
}

#[derive(Debug)]
struct Job {
    generation: i64,
    remote_id: Option<String>,
    remote_parent: Option<String>,
    remote_name: Option<String>,
    stage: Option<String>,
    deleted: bool,
    conflict: bool,
}

fn job(db: &Connection, inode: u64) -> rusqlite::Result<Option<Job>> {
    db.query_row(
        "SELECT generation, remote_id, remote_parent, remote_name, stage, deleted, conflict
         FROM writeback_queue WHERE inode = ?1",
        [inode],
        |row| {
            Ok(Job {
                generation: row.get(0)?,
                remote_id: row.get(1)?,
                remote_parent: row.get(2)?,
                remote_name: row.get(3)?,
                stage: row.get(4)?,
                deleted: row.get(5)?,
                conflict: row.get(6)?,
            })
        },
    )
    .optional()
}

fn same_content_version(before: &CatalogItem, after: &CatalogItem) -> bool {
    before.item_id == after.item_id
        && before.size == after.size
        && before.etag == after.etag
        && before.ctag == after.ctag
        && before.mtime == after.mtime
}

fn enqueue_existing(tx: &Transaction<'_>, item: &CatalogItem) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO writeback_queue (inode, remote_id, remote_parent, remote_name, conflict)
         VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(inode) DO NOTHING",
        params![
            item.inode,
            item.item_id,
            item.parent_id,
            item.intended_name,
            item.sync_state == SyncState::Conflict
        ],
    )?;
    Ok(())
}

fn validate_name(parent: &CatalogItem, name: &str, kind: ItemKind) -> Result<(), EngineError> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        return Err(EngineError::InvalidMutation);
    }
    if parent.inode == 1
        && kind == ItemKind::Folder
        && (name.starts_with(".Trash-") || name == ".Trash")
    {
        return Err(EngineError::TrashProhibited);
    }
    Ok(())
}

fn directory(db: &Connection, inode: u64) -> Result<CatalogItem, EngineError> {
    let item = item_by_inode(db, inode)?.ok_or(EngineError::NotFound)?;
    if item.kind != ItemKind::Folder {
        return Err(EngineError::NotDirectory);
    }
    if job(db, inode)?.is_some_and(|job| job.deleted) {
        return Err(EngineError::NotFound);
    }
    Ok(item)
}

/// Run while holding the catalog transaction that retires cached bytes.
pub(super) fn refuse_purge(item_id: &str, state: SyncState) -> Result<(), EngineError> {
    match state {
        SyncState::Dirty | SyncState::Uploading => Err(EngineError::PendingUpload {
            item_id: item_id.into(),
        }),
        SyncState::Conflict => Err(EngineError::UnresolvedConflict {
            item_id: item_id.into(),
        }),
        SyncState::Clean => Ok(()),
    }
}

impl Engine {
    pub fn create(
        &self,
        parent: u64,
        name: &str,
        kind: ItemKind,
    ) -> Result<CatalogItem, EngineError> {
        self.create_with_mode(parent, name, kind, None)
    }

    pub(crate) fn create_with_mode(
        &self,
        parent: u64,
        name: &str,
        kind: ItemKind,
        mode: Option<u16>,
    ) -> Result<CatalogItem, EngineError> {
        self.create_entry(parent, name, kind, mode, false)
    }

    pub fn create_and_open(
        &self,
        parent: u64,
        name: &str,
        mode: Option<u16>,
    ) -> Result<CatalogItem, EngineError> {
        self.create_entry(parent, name, ItemKind::File, mode, true)
    }

    fn create_entry(
        &self,
        parent: u64,
        name: &str,
        kind: ItemKind,
        mode: Option<u16>,
        open: bool,
    ) -> Result<CatalogItem, EngineError> {
        let item = self.update_catalog_after(
            |tx| {
                let parent = directory(tx, parent)?;
                validate_name(&parent, name, kind)?;
                if lookup(tx, &parent.item_id, name)?.is_some() {
                    return Err(EngineError::AlreadyExists);
                }
                tx.execute(
                    "INSERT INTO items (item_id, parent_id, name, kind, size, mtime, sync_state)
             VALUES ('local:' || lower(hex(randomblob(16))), ?1, ?2, ?3, 0, ?4, ?5)",
                    params![
                        parent.item_id,
                        name,
                        kind == ItemKind::Folder,
                        self.write_time(),
                        SyncState::Dirty as i64
                    ],
                )?;
                let inode = tx.last_insert_rowid();
                tx.execute("INSERT INTO writeback_queue (inode) VALUES (?1)", [inode])?;
                if let Some(mode) = mode {
                    tx.execute(
                        "INSERT INTO local_attributes (inode, mode) VALUES (?1, ?2)",
                        params![inode, mode],
                    )?;
                }
                item_by_inode(tx, inode as u64)?.ok_or(EngineError::NotFound)
            },
            |item| {
                if open {
                    self.register_open(item.inode);
                }
            },
        )?;
        self.writeback.changed.notify_one();
        Ok(item)
    }

    pub(crate) fn set_local_attributes(
        &self,
        inode: u64,
        mode: Option<u16>,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> Result<CatalogItem, EngineError> {
        let item = self.update_catalog(|tx| {
            if item_by_inode(tx, inode)?.is_none() {
                return Err(EngineError::NotFound);
            }
            tx.execute(
                "INSERT INTO local_attributes (inode, mode, atime, mtime) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(inode) DO UPDATE SET
                mode = coalesce(excluded.mode, local_attributes.mode),
                atime = coalesce(excluded.atime, local_attributes.atime),
                mtime = coalesce(excluded.mtime, local_attributes.mtime)",
                params![inode, mode, atime, mtime],
            )?;
            let item = item_by_inode(tx, inode)?.ok_or(EngineError::NotFound)?;
            Ok(item)
        })?;
        Ok(item)
    }

    fn write_time(&self) -> i64 {
        i64::try_from(self.clock.now().as_secs()).unwrap_or(i64::MAX)
    }

    pub async fn write(
        &self,
        inode: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
    ) -> Result<(), EngineError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.edit(inode, Some((offset, bytes, append)), None)
            .await
            .map(|_| ())
    }

    pub async fn truncate(&self, inode: u64, size: u64) -> Result<CatalogItem, EngineError> {
        self.edit(inode, None, Some(size)).await
    }

    async fn edit(
        &self,
        inode: u64,
        write: Option<(u64, &[u8], bool)>,
        truncate: Option<u64>,
    ) -> Result<CatalogItem, EngineError> {
        loop {
            let (before, previous) = {
                let db = self.db();
                (
                    item_by_inode(&db, inode)?.ok_or(EngineError::NotFound)?,
                    job(&db, inode)?,
                )
            };
            if before.kind == ItemKind::Folder {
                return Err(EngineError::IsDirectory);
            }
            let write_at = write
                .map(|(offset, bytes, append)| (if append { before.size } else { offset }, bytes));
            let size = if let Some((offset, bytes)) = write_at {
                before.size.max(
                    offset
                        .checked_add(bytes.len() as u64)
                        .ok_or(EngineError::InvalidMutation)?,
                )
            } else {
                truncate.expect("edit is a write or truncate")
            };
            if size > i64::MAX as u64 {
                return Err(EngineError::InvalidMutation);
            }
            let stage = previous.as_ref().and_then(|job| job.stage.as_deref());
            let overwritten = write_at
                .is_some_and(|(offset, bytes)| offset == 0 && bytes.len() as u64 >= before.size);
            let keep = before.size.min(size);
            let needs_base = stage.is_none() && keep != 0 && !overwritten;
            let prefix = if needs_base {
                let read = self
                    .read(
                        inode,
                        0,
                        keep.try_into().map_err(|_| EngineError::InvalidMutation)?,
                    )
                    .await?;
                Some(read)
            } else {
                None
            };

            let Some(current) = self.update_catalog(|tx| {
                let current = item_by_inode(tx, inode)?.ok_or(EngineError::NotFound)?;
                let current_job = job(tx, inode)?;
                if !same_content_version(&before, &current)
                    || previous.as_ref().map(|job| job.generation)
                        != current_job.as_ref().map(|job| job.generation)
                {
                    return Ok(None);
                }
                if prefix
                    .as_ref()
                    .is_some_and(|bytes| bytes.len() as u64 != keep)
                {
                    return Err(EngineError::InvalidRange);
                }
                enqueue_existing(tx, &current)?;
                let file = self.writeback.stage(
                    stage,
                    prefix.as_deref().unwrap_or_default(),
                    size,
                    write_at,
                )?;
                let name = file.path().file_name().unwrap().to_str().unwrap();
                tx.execute(
                    "UPDATE writeback_queue SET stage = ?2, generation = generation + 1 WHERE inode = ?1",
                    params![inode, name],
                )?;
                tx.execute(
                    "UPDATE items SET size = ?2, mtime = ?3, last_accessed_at = ?4,
                     sync_state = CASE WHEN sync_state = 3 THEN 3 ELSE 1 END WHERE inode = ?1",
                    params![inode, size, self.write_time(), self.access_time()],
                )?;
                tx.execute(
                    "UPDATE local_attributes SET mtime = NULL WHERE inode = ?1",
                    [inode],
                )?;
                tx.execute("DELETE FROM sparse_files WHERE inode = ?1", [inode])?;
                let current = item_by_inode(tx, inode)?.ok_or(EngineError::NotFound)?;
                // Disarm automatic unlink before committing a reference to this file.
                let (_file, _path) = file.keep().map_err(|err| err.error)?;
                tx.execute("INSERT INTO changed_inodes VALUES (?1, 1)", [inode])?;
                Ok(Some(current))
            })? else { continue };
            if let Some(stage) = stage {
                self.retire_unused_stage(stage)?;
            }
            let _ = self.reclaim_sparse_cache();
            self.writeback.changed.notify_one();
            return Ok(current);
        }
    }

    fn retire_unused_stage(&self, stage: &str) -> Result<(), EngineError> {
        let db = self.db();
        let referenced: bool = db.query_row(
            "SELECT EXISTS (SELECT 1 FROM writeback_queue WHERE stage = ?1)
                 OR EXISTS (SELECT 1 FROM writeback_attempts WHERE stage = ?1)",
            [stage],
            |row| row.get(0),
        )?;
        if !referenced {
            self.writeback.retire(Some(stage));
        }
        Ok(())
    }

    pub(super) fn open_stage(
        &self,
        db: &Connection,
        inode: u64,
    ) -> Result<Option<File>, EngineError> {
        let stage = db
            .query_row(
                "SELECT stage FROM writeback_queue WHERE inode = ?1",
                [inode],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        Ok(stage
            .map(|name| File::open(self.writeback.root.join(name)))
            .transpose()?)
    }

    /// Refuse Purge while there is local work queued for this item.
    pub fn check_purge(&self, item_id: &str) -> Result<(), EngineError> {
        let db = self.db();
        let item = item_by_id(&db, item_id)?.ok_or(EngineError::NotFound)?;
        refuse_purge(item_id, item.sync_state)
    }
}

impl Engine {
    pub fn unlink(&self, parent: u64, name: &str, kind: ItemKind) -> Result<(), EngineError> {
        self.update_catalog(|tx| {
            let parent = directory(tx, parent)?;
            let item = lookup(tx, &parent.item_id, name)?.ok_or(EngineError::NotFound)?;
            check_removable(tx, &item, kind)?;
            queue_delete(tx, &item)?;
            let mut affected = crate::namespace::AffectedNames::default();
            affected.record_item(tx, &item.item_id)?;
            affected.reconcile(tx, false)?;
            Ok(())
        })?;
        self.writeback.changed.notify_one();
        Ok(())
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        no_replace: bool,
    ) -> Result<(), EngineError> {
        self.update_catalog(|tx| {
            let parent = directory(tx, parent)?;
            let new_parent = directory(tx, new_parent)?;
            let item = lookup(tx, &parent.item_id, name)?.ok_or(EngineError::NotFound)?;
            validate_name(&new_parent, new_name, item.kind)?;
            if (parent.inode, name) == (new_parent.inode, new_name) {
                return Ok(());
            }
            if item.kind == ItemKind::Folder {
                let mut ancestor = Some(new_parent.clone());
                let mut seen = HashSet::new();
                while let Some(folder) = ancestor {
                    if folder.inode == item.inode || !seen.insert(folder.inode) {
                        return Err(EngineError::InvalidMutation);
                    }
                    ancestor = folder
                        .parent_id
                        .as_deref()
                        .map(|id| item_by_id(tx, id))
                        .transpose()?
                        .flatten();
                }
            }

            let mut affected = crate::namespace::AffectedNames::default();
            affected.record_item(tx, &item.item_id)?;
            affected.insert(Some(new_parent.item_id.clone()), new_name.to_owned());
            if let Some(replaced) = lookup(tx, &new_parent.item_id, new_name)? {
                if no_replace {
                    return Err(EngineError::AlreadyExists);
                }
                check_removable(tx, &replaced, item.kind)?;
                queue_delete(tx, &replaced)?;
                affected.record_item(tx, &replaced.item_id)?;
                if job(tx, replaced.inode)?.is_some_and(|job| job.remote_id.is_some()) {
                    tx.execute(
                        "INSERT INTO writeback_replacements VALUES (?1, ?2, ?3)",
                        params![replaced.inode, item.inode, replaced.etag],
                    )?;
                }
            }
            enqueue_existing(tx, &item)?;
            tx.execute(
                "UPDATE items SET parent_id = ?2, name = ?3, alias = NULL, sync_state = CASE WHEN sync_state = 3 THEN 3 ELSE 1 END WHERE inode = ?1",
                params![item.inode, new_parent.item_id, new_name],
            )?;
            tx.execute(
                "UPDATE writeback_queue SET generation = generation + 1 WHERE inode = ?1",
                [item.inode],
            )?;
            affected.reconcile(tx, false)?;
            Ok(())
        })?;
        self.writeback.changed.notify_one();
        Ok(())
    }

    pub(crate) fn open_file(&self, inode: u64) -> Result<(), EngineError> {
        let db = self.db();
        let item = item_by_inode(&db, inode)?.ok_or(EngineError::NotFound)?;
        if item.kind != ItemKind::File {
            return Err(EngineError::IsDirectory);
        }
        if job(&db, inode)?.is_some_and(|job| job.deleted) {
            return Err(EngineError::NotFound);
        }
        self.register_open(inode);
        Ok(())
    }

    fn register_open(&self, inode: u64) {
        *self
            .writeback
            .open_files
            .lock()
            .unwrap()
            .entry(inode)
            .or_default() += 1;
    }

    pub fn release_file(&self, inode: u64) {
        let mut files = self.writeback.open_files.lock().unwrap();
        if let Some(count) = files.get_mut(&inode) {
            *count -= 1;
            if *count == 0 {
                files.remove(&inode);
            }
        }
        self.writeback.changed.notify_one();
    }

    /// Expose queued catalog entries, including deletions hidden from the mount.
    pub fn pending_uploads(&self) -> Result<Vec<CatalogItem>, EngineError> {
        queued_items(&self.db())
    }

    /// Resolve pending Upload paths from the same catalog snapshot as their states.
    pub fn pending_upload_paths(&self) -> Result<Vec<(PathBuf, SyncState)>, EngineError> {
        let db = self.db();
        let tx = db.unchecked_transaction()?;
        queued_items(&tx)?
            .into_iter()
            .map(|item| {
                let path = super::path_for(&tx, &item.item_id)?.ok_or(EngineError::NotFound)?;
                Ok((path, item.sync_state))
            })
            .collect()
    }

    /// Drain currently runnable work. The catalog lock is released during all cloud calls.
    pub async fn drain_uploads(&self) -> Result<usize, EngineError> {
        let _uploading = self.writeback.uploading.lock().await;
        let mut completed = 0;
        let mut skipped = HashSet::new();
        let mut first_error = None;
        loop {
            // Serialize each upload acknowledgment with delta so it can't apply a
            // stale snapshot. Other delta work can proceed between uploads.
            let _sync = self.syncing.lock().await;
            let Some(mut work) = self.next_upload(&skipped)? else {
                break;
            };
            let result = self.upload(&mut work).await;
            let conflict = matches!(&result, Err(EngineError::Cloud(CloudError::Conflict)))
                && matches!(
                    work.action,
                    Action::Upload | Action::CreateFile | Action::CreateFolder | Action::Move
                );
            let missing_update = matches!(&result, Err(EngineError::Cloud(CloudError::NotFound)))
                && matches!(work.action, Action::Upload);
            if result.is_ok() && !matches!(work.action, Action::CreateFolder | Action::CreateFile) {
                completed += 1;
            }
            let result = if conflict || missing_update {
                self.preserve_conflict(&work).await
            } else {
                result
            };
            match result {
                Ok(()) => {}
                Err(
                    error @ EngineError::Cloud(
                        CloudError::Throttled { .. } | CloudError::Authentication,
                    ),
                ) => return Err(error),
                Err(error @ (EngineError::Cloud(_) | EngineError::CacheIo(_))) => {
                    skipped.insert(work.item.inode);
                    first_error.get_or_insert(error);
                }
                Err(error) => return Err(error),
            }
        }
        first_error.map_or(Ok(completed), Err)
    }

    /// Run retries while the caller retains the Engine. This task holds no strong
    /// Engine reference while idle, so dropping the mount can stop it.
    pub async fn run_upload_worker(engine: Weak<Self>) {
        loop {
            let Some(active) = engine.upgrade() else {
                return;
            };
            let changed = active.writeback.changed.clone();
            match active.drain_uploads().await {
                Ok(_) => {
                    drop(active);
                    // The timeout also notices a dropped Engine without a notification.
                    let _ = tokio::time::timeout(Duration::from_secs(1), changed.notified()).await;
                }
                Err(error) => {
                    let wait = match error {
                        EngineError::Cloud(CloudError::Throttled { retry_after }) => retry_after,
                        _ => Duration::from_secs(1),
                    };
                    drop(active);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    fn next_upload(&self, skipped: &HashSet<u64>) -> Result<Option<Work>, EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let open_files = self.writeback.open_files.lock().unwrap();
        let inodes = tx
            .prepare("SELECT inode FROM writeback_queue ORDER BY deleted DESC, sequence")?
            .query_map([], |row| row.get::<_, u64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut blocked_moves = HashMap::new();
        for inode in inodes.into_iter().filter(|inode| !skipped.contains(inode)) {
            let item = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
            let queued = job(&tx, inode)?.ok_or(EngineError::NotFound)?;
            if !queued.deleted
                && tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM writeback_replacements WHERE source_inode = ?1)",
                    [inode],
                    |row| row.get::<_, bool>(0),
                )?
            {
                continue;
            }
            let saved = attempt(&tx, inode)?;
            let action = if let Some(saved) = &saved {
                if saved.remote_id.is_some() {
                    Action::Upload
                } else if item.kind == ItemKind::Folder {
                    Action::CreateFolder
                } else {
                    Action::CreateFile
                }
            } else if queued.deleted {
                // Keep the remote file available until the last POSIX handle closes.
                if open_files.contains_key(&inode) {
                    continue;
                }
                if item.kind == ItemKind::Folder
                    && tx.query_row(
                        "SELECT EXISTS (SELECT 1 FROM writeback_queue WHERE remote_parent = ?1)",
                        [&item.item_id],
                        |row| row.get::<_, bool>(0),
                    )?
                {
                    // A child moved out locally may still be under this folder in OneDrive.
                    continue;
                }
                Action::Delete
            } else {
                let parent = item
                    .parent_id
                    .as_deref()
                    .ok_or(EngineError::InvalidMutation)?;
                let parent = item_by_id(&tx, parent)?.ok_or(EngineError::NotFound)?;
                if job(&tx, parent.inode)?.is_some_and(|job| job.remote_id.is_none()) {
                    continue;
                }
                let path_changed = queued.remote_parent != item.parent_id
                    || queued.remote_name.as_deref() != Some(&item.intended_name);
                if queued.remote_id.is_none() || (queued.stage.is_none() && path_changed) {
                    let occupied = tx
                        .query_row(
                            "SELECT inode FROM writeback_queue
                         WHERE inode <> ?1 AND remote_parent = ?2 AND remote_name = ?3
                         ORDER BY sequence LIMIT 1",
                            params![inode, item.parent_id, item.intended_name],
                            |row| row.get::<_, u64>(0),
                        )
                        .optional()?;
                    if let Some(other) = occupied {
                        if queued.remote_id.is_some() {
                            blocked_moves.insert(inode, other);
                        }
                        continue;
                    }
                }
                match (&queued.remote_id, item.kind, &queued.stage) {
                    (None, ItemKind::Folder, _) => Action::CreateFolder,
                    (None, ItemKind::File, _) => Action::CreateFile,
                    (Some(_), ItemKind::File, Some(_)) => Action::Upload,
                    (Some(_), _, _) if path_changed => Action::Move,
                    _ => Action::Acknowledge,
                }
            };
            let replay = saved.is_some();
            let attempt = if saved.is_none()
                && matches!(
                    action,
                    Action::Upload | Action::CreateFile | Action::CreateFolder
                ) {
                let parent = item
                    .parent_id
                    .as_deref()
                    .ok_or(EngineError::InvalidMutation)?;
                tx.execute(
                    "INSERT INTO writeback_attempts (inode, generation, remote_id, parent_id, name, etag, stage, size)
                     VALUES (?1, ?2, ?3, ?4,
                         CASE WHEN ?3 IS NULL THEN 'onedrive-rs-upload-' || lower(hex(randomblob(16))) ELSE ?5 END,
                         ?6, ?7, ?8)",
                    params![inode, queued.generation, queued.remote_id, parent, item.intended_name, item.etag, queued.stage, item.size],
                )?;
                attempt(&tx, inode)?
            } else {
                saved
            };
            tx.commit()?;
            return Ok(Some(Work {
                item,
                queued,
                action,
                stage: None,
                attempt,
                replay,
            }));
        }
        if let Some(inode) = rename_cycle(&blocked_moves) {
            let item = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
            let queued = job(&tx, inode)?.ok_or(EngineError::NotFound)?;
            return Ok(Some(Work {
                item,
                queued,
                action: Action::TemporaryMove,
                stage: None,
                attempt: None,
                replay: false,
            }));
        }
        Ok(None)
    }

    async fn upload(&self, work: &mut Work) -> Result<(), EngineError> {
        let parent = work
            .item
            .parent_id
            .as_deref()
            .ok_or(EngineError::InvalidMutation)?;
        let name = &work.item.intended_name;
        let cloud_id = work.queued.remote_id.as_deref();
        let remote = match &work.action {
            Action::Delete => {
                if let Some(id) = cloud_id {
                    let replacement = self.db().query_row(
                        "SELECT source_inode, etag FROM writeback_replacements WHERE target_inode = ?1",
                        [work.item.inode],
                        |row| Ok((row.get::<_, Option<u64>>(0)?, row.get::<_, Option<String>>(1)?)),
                    ).optional()?;
                    let result = match &replacement {
                        Some((_, Some(etag))) => self.cloud.delete_if_match(id, etag).await,
                        Some((_, None)) => Err(CloudError::Conflict),
                        None => self.cloud.delete_item(id).await,
                    };
                    match result {
                        Ok(()) | Err(CloudError::NotFound) => {}
                        Err(CloudError::Conflict) if replacement.is_some() => {
                            if let Some(remote) = self
                                .cloud
                                .upload_destination(UploadTarget::Existing { item_id: id })
                                .await?
                            {
                                self.preserve_replacement(
                                    work,
                                    replacement.and_then(|(inode, _)| inode),
                                    remote,
                                )?;
                                return Ok(());
                            }
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                self.finish_delete(work)?;
                return Ok(());
            }
            Action::CreateFolder | Action::CreateFile | Action::Upload => {
                work.stage = work
                    .attempt
                    .as_ref()
                    .and_then(|attempt| attempt.stage.as_deref())
                    .map(|name| File::open(self.writeback.root.join(name)))
                    .transpose()?;
                self.dispatch_attempt(work).await?
            }
            Action::Move => {
                let item_id = cloud_id.ok_or(EngineError::InvalidMutation)?;
                match self.cloud.move_item(item_id, parent, name).await {
                    Err(CloudError::Conflict) => match self
                        .cloud
                        .upload_destination(UploadTarget::Existing { item_id })
                        .await?
                    {
                        Some(remote)
                            if remote.parent_id.as_deref() == Some(parent)
                                && remote.name.as_deref() == Some(name) =>
                        {
                            remote
                        }
                        _ => return Err(CloudError::Conflict.into()),
                    },
                    result => result?,
                }
            }
            Action::TemporaryMove => self.move_to_temporary_name(work).await?,
            Action::Acknowledge => return self.acknowledge(work, None),
        };
        self.acknowledge(work, Some(remote))
    }

    async fn dispatch_attempt(&self, work: &Work) -> Result<RemoteItem, EngineError> {
        let attempt = work.attempt.as_ref().ok_or(EngineError::InvalidMutation)?;
        let target = if let Some(id) = &attempt.remote_id {
            UploadTarget::Existing { item_id: id }
        } else {
            UploadTarget::New {
                parent_id: &attempt.parent_id,
                name: &attempt.name,
            }
        };
        if matches!(work.action, Action::CreateFolder) {
            if work.replay
                && let Some(remote) = self.cloud.upload_destination(target).await?
            {
                return if remote.kind == ItemKind::Folder && remote.etag.is_some() {
                    Ok(remote)
                } else {
                    Err(CloudError::Conflict.into())
                };
            }
            return Ok(self
                .cloud
                .create_folder(&attempt.parent_id, &attempt.name)
                .await?);
        }
        let size = usize::try_from(attempt.size).map_err(|_| EngineError::InvalidMutation)?;
        let source = match &work.stage {
            Some(file) => UploadSource::File { file, size },
            None => UploadSource::Bytes(&[]),
        };
        let etag = if attempt.remote_id.is_some() {
            Some(attempt.etag.as_deref().ok_or(CloudError::Conflict)?)
        } else {
            None
        };
        if work.replay
            && let Some(remote) = self.cloud.confirm_upload(target, source, etag).await?
        {
            return Ok(remote);
        }
        Ok(self.cloud.upload_from(target, source, etag).await?)
    }

    async fn move_to_temporary_name(&self, work: &Work) -> Result<RemoteItem, EngineError> {
        let id = work
            .queued
            .remote_id
            .as_deref()
            .ok_or(EngineError::InvalidMutation)?;
        let parent = work
            .queued
            .remote_parent
            .as_deref()
            .ok_or(EngineError::InvalidMutation)?;
        for attempt in 0..16 {
            let name = format!("onedrive-rs-move-{}-{attempt}", work.item.inode);
            if self.lookup(parent, &name)?.is_some() {
                continue;
            }
            match self.cloud.move_item(id, parent, &name).await {
                Ok(mut moved) => {
                    moved.parent_id = Some(parent.into());
                    moved.name = Some(name);
                    return Ok(moved);
                }
                Err(CloudError::Conflict) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(CloudError::Conflict.into())
    }

    fn preserve_replacement(
        &self,
        work: &Work,
        source_inode: Option<u64>,
        remote: RemoteItem,
    ) -> Result<(), EngineError> {
        self.update_catalog(|tx| {
            let target = work.item.inode;
            let mut affected = crate::namespace::AffectedNames::default();
            affected.record_item(tx, &work.item.item_id)?;
            if let Some(name) = &remote.name {
                affected.insert(remote.parent_id.clone(), name.clone());
            }
            if let Some(inode) = source_inode
                && let Some(source) = item_by_inode(tx, inode)?
                && !source.unlinked
                && source.parent_id == work.item.parent_id
                && source.intended_name == work.item.intended_name
            {
                let parent = source.parent_id.as_deref().ok_or(EngineError::InvalidMutation)?;
                let name = conflict_name(tx, parent, &source, self.write_time(), &HashMap::new())?;
                tx.execute(
                    "UPDATE items SET name = ?2, alias = NULL, sync_state = 3 WHERE inode = ?1",
                    params![inode, name],
                )?;
                tx.execute(
                    "UPDATE writeback_queue SET generation = generation + 1, conflict = 1 WHERE inode = ?1", [inode],
                )?;
            }
            // A target that had its own pending edit keeps those staged bytes.
            let queued = job(tx, target)?.ok_or(EngineError::NotFound)?;
            if queued.stage.is_some() {
                tx.execute("UPDATE writeback_queue SET deleted = 0 WHERE inode = ?1", [target])?;
                tx.execute("DELETE FROM writeback_replacements WHERE target_inode = ?1", [target])?;
            } else {
                tx.execute("DELETE FROM writeback_queue WHERE inode = ?1", [target])?;
                tx.execute("UPDATE items SET sync_state = 0 WHERE inode = ?1", [target])?;
                let revision = tx.query_row("SELECT revision FROM delta_checkpoint", [], |row| row.get(0))?;
                super::upsert_remote_item(tx, remote, revision, 0)?;
            }
            tx.execute("INSERT INTO changed_entries VALUES (?1, ?2)", params![work.item.parent_id, work.item.name])?;
            affected.reconcile(tx, false)?;
            Ok(())
        })
    }

    fn finish_delete(&self, work: &Work) -> Result<(), EngineError> {
        let stage = self.update_catalog(|tx| {
            let stage = job(tx, work.item.inode)?.and_then(|job| job.stage);
            if let Some(id) = work.queued.remote_id.as_deref() {
                tx.execute(
                    "INSERT OR IGNORE INTO writeback_recycled (item_id) VALUES (?1)",
                    [id],
                )?;
            }
            tx.execute("DELETE FROM items WHERE inode = ?1", [work.item.inode])?;
            Ok(stage)
        })?;
        self.writeback.retire(stage.as_deref());
        self.reclaim_sparse_cache()?;
        Ok(())
    }

    fn acknowledge(&self, work: &Work, remote: Option<RemoteItem>) -> Result<(), EngineError> {
        let cache_lock = work
            .stage
            .as_ref()
            .map(|_| self.cache.lock_disk())
            .transpose()
            .ok()
            .flatten();
        let stage = self.update_catalog(|tx| {
            let inode = work.item.inode;
            let current = item_by_inode(tx, inode)?.ok_or(EngineError::NotFound)?;
            let queued = job(tx, inode)?.ok_or(EngineError::NotFound)?;
            let sent_content = matches!(work.action, Action::CreateFile | Action::Upload);
            let attempted_stage = work.attempt.as_ref().and_then(|attempt| attempt.stage.as_deref());
            let retire = sent_content && queued.stage.as_deref() == attempted_stage;
            clear_attempt(tx, work)?;
            if retire {
                tx.execute(
                    "UPDATE writeback_queue SET stage = NULL WHERE inode = ?1",
                    [inode],
                )?;
            }
            let completed = if let Some(remote) = remote {
                let (parent, name) = (remote.parent_id.as_deref(), remote.name.as_deref());
                if matches!(work.action, Action::CreateFolder | Action::CreateFile) {
                    tx.execute(
                        "INSERT INTO writeback_temporary_names VALUES (?1, ?2)",
                        params![name, remote.item_id],
                    )?;
                }
                if matches!(work.action, Action::Upload) && let Some(attempt) = &work.attempt {
                    tx.execute(
                        "UPDATE writeback_replacements SET etag = ?3 WHERE target_inode = ?1 AND etag = ?2",
                        params![inode, attempt.etag, remote.etag],
                    )?;
                }
                tx.execute("INSERT OR IGNORE INTO full_sync_seen SELECT ?1 WHERE EXISTS (
                    SELECT 1 FROM delta_checkpoint WHERE full_scan AND link IS NOT NULL
                )", [&remote.item_id])?;
                tx.execute(
                    "UPDATE writeback_queue SET remote_id = ?2, remote_parent = ?3, remote_name = ?4 WHERE inode = ?1",
                    params![inode, remote.item_id, parent, name],
                )?;
                // Only a content acknowledgment advances the base of a concurrent edit.
                // If a pure move changed just the eTag, its cTag still identifies the bytes.
                let preserve_bytes = matches!(work.action, Action::Move | Action::TemporaryMove)
                    && current.ctag.is_some() && current.ctag == remote.ctag && current.size == remote.size;
                if preserve_bytes {
                    tx.execute("INSERT INTO sparse_preserve_on_move VALUES (?1)", [inode])?;
                }
                let accept_etag = sent_content
                    || queued.stage.is_none()
                    || (current.ctag.is_some() && current.ctag == remote.ctag);
                tx.execute(
                    "UPDATE items SET item_id = ?2, remote_missing = 0,
                     etag = CASE WHEN ?3 THEN ?4 ELSE etag END, ctag = ?5, quickxor_hash = ?6
                     WHERE inode = ?1",
                    params![
                        inode,
                        remote.item_id,
                        accept_etag,
                        remote.etag,
                        remote.ctag,
                        remote.quickxor_hash
                    ],
                )?;
                if current.item_id != remote.item_id {
                    tx.execute(
                        "UPDATE items SET parent_id = ?2 WHERE parent_id = ?1",
                        params![current.item_id, remote.item_id],
                    )?;
                    tx.execute(
                        "UPDATE pin_directives SET item_id = ?2 WHERE item_id = ?1",
                        params![current.item_id, remote.item_id],
                    )?;
                }
                let follow_remote_path = matches!(work.action, Action::Upload)
                    && current.parent_id == queued.remote_parent
                    && Some(current.intended_name.as_str()) == queued.remote_name.as_deref();
                if follow_remote_path && let Some(name) = name {
                    let mut affected = crate::namespace::AffectedNames::default();
                    affected.record_item(tx, &remote.item_id)?;
                    affected.insert(remote.parent_id.clone(), name.to_owned());
                    tx.execute("UPDATE items SET parent_id = ?2, name = ?3, alias = CASE
                        WHEN parent_id IS ?2 AND name = ?3 THEN alias END WHERE inode = ?1",
                        params![inode, parent, name])?;
                    affected.reconcile(tx, false)?;
                }
                if queued.stage.is_none() || retire {
                    tx.execute(
                        "UPDATE items SET size = ?2, mtime = ?3 WHERE inode = ?1",
                        params![
                            inode,
                            remote.size,
                            remote.mtime
                        ],
                    )?;
                }
                if preserve_bytes {
                    tx.execute("DELETE FROM sparse_preserve_on_move WHERE inode = ?1", [inode])?;
                }
                (retire || queued.stage.is_none())
                    && (follow_remote_path || (current.parent_id.as_deref() == parent
                        && Some(current.intended_name.as_str()) == name))
            } else {
                queued.generation == work.queued.generation
            };
            if retire && !queued.deleted && let (Some(directory), Some(source)) = (&cache_lock, &work.stage) {
                tx.execute_batch("SAVEPOINT cache_uploaded_content")?;
                if self.cache.retain_upload(directory, tx, inode, current.size, source).is_err() {
                    tx.execute_batch("ROLLBACK TO cache_uploaded_content")?;
                }
                tx.execute_batch("RELEASE cache_uploaded_content")?;
            }
            if completed && !queued.deleted {
                tx.execute("DELETE FROM writeback_queue WHERE inode = ?1", [inode])?;
                tx.execute(
                    "UPDATE items SET sync_state = ?2 WHERE inode = ?1",
                    params![
                        inode,
                        if queued.conflict {
                            SyncState::Conflict
                        } else {
                            SyncState::Clean
                        } as i64
                    ],
                )?;
            }
            Ok(attempted_stage.map(str::to_owned))
        })?;
        drop(cache_lock);
        if let Some(stage) = stage {
            self.retire_unused_stage(&stage)?;
        }
        self.reclaim_sparse_cache()?;
        Ok(())
    }
}

fn queued_items(db: &Connection) -> Result<Vec<CatalogItem>, EngineError> {
    let mut query = db.prepare_cached(&format!(
        "SELECT {} FROM items WHERE inode IN (SELECT inode FROM writeback_queue) ORDER BY inode",
        super::ITEM_COLUMNS,
    ))?;
    Ok(query
        .query_map([], super::catalog_item)?
        .collect::<Result<_, _>>()?)
}

enum Action {
    Delete,
    CreateFolder,
    CreateFile,
    Upload,
    Move,
    TemporaryMove,
    Acknowledge,
}

struct Work {
    item: CatalogItem,
    queued: Job,
    action: Action,
    stage: Option<File>,
    attempt: Option<Attempt>,
    replay: bool,
}

struct Attempt {
    generation: i64,
    remote_id: Option<String>,
    parent_id: String,
    name: String,
    etag: Option<String>,
    stage: Option<String>,
    size: u64,
}

fn attempt(db: &Connection, inode: u64) -> rusqlite::Result<Option<Attempt>> {
    db.query_row(
        "SELECT generation, remote_id, parent_id, name, etag, stage, size
         FROM writeback_attempts WHERE inode = ?1",
        [inode],
        |row| {
            Ok(Attempt {
                generation: row.get(0)?,
                remote_id: row.get(1)?,
                parent_id: row.get(2)?,
                name: row.get(3)?,
                etag: row.get(4)?,
                stage: row.get(5)?,
                size: row.get(6)?,
            })
        },
    )
    .optional()
}

fn clear_attempt(tx: &Transaction<'_>, work: &Work) -> Result<(), EngineError> {
    if let Some(attempt) = &work.attempt
        && tx.execute(
            "DELETE FROM writeback_attempts WHERE inode = ?1 AND generation = ?2",
            params![work.item.inode, attempt.generation],
        )? != 1
    {
        return Err(EngineError::InvalidMutation);
    }
    Ok(())
}

fn rename_cycle(blocked: &HashMap<u64, u64>) -> Option<u64> {
    let mut processed = HashSet::new();
    for &start in blocked.keys() {
        let mut visited = HashSet::new();
        let mut current = start;
        while let Some(&next) = blocked.get(&current) {
            if processed.contains(&current) {
                break;
            }
            if !visited.insert(current) {
                return Some(current);
            }
            current = next;
        }
        processed.extend(visited);
    }
    None
}

fn check_removable(db: &Connection, item: &CatalogItem, kind: ItemKind) -> Result<(), EngineError> {
    if item.kind != kind {
        return Err(if item.kind == ItemKind::Folder {
            EngineError::IsDirectory
        } else {
            EngineError::NotDirectory
        });
    }
    if item.kind == ItemKind::Folder {
        let nonempty = db.query_row(
            "SELECT EXISTS (SELECT 1 FROM items WHERE parent_id = ?1
             AND inode NOT IN (SELECT inode FROM writeback_queue WHERE deleted))",
            [&item.item_id],
            |row| row.get::<_, bool>(0),
        )?;
        if nonempty {
            return Err(EngineError::NotEmpty);
        }
    }
    Ok(())
}

fn queue_delete(tx: &Transaction<'_>, item: &CatalogItem) -> rusqlite::Result<()> {
    enqueue_existing(tx, item)?;
    tx.execute(
        "INSERT INTO changed_entries VALUES (?1, ?2)",
        params![item.parent_id, item.name],
    )?;
    tx.execute("INSERT INTO changed_inodes VALUES (?1, 0)", [item.inode])?;
    tx.execute(
        "UPDATE writeback_queue SET deleted = 1, generation = generation + 1 WHERE inode = ?1",
        [item.inode],
    )?;
    tx.execute(
        "UPDATE items SET sync_state = 1 WHERE inode = ?1",
        [item.inode],
    )?;
    Ok(())
}

impl Engine {
    async fn preserve_conflict(&self, work: &Work) -> Result<(), EngineError> {
        let mut request = DeltaRequest::Initial;
        let mut remote = HashMap::new();
        loop {
            let page = self.cloud.delta(request).await?;
            for change in page.changes {
                match change {
                    DeltaChange::Upsert(item) if item.name.is_some() => {
                        remote.insert(item.item_id.clone(), item);
                    }
                    DeltaChange::Delete(id) => {
                        remote.remove(&id);
                    }
                    _ => {}
                }
            }
            match page.cursor {
                DeltaCursor::Next(link) => request = DeltaRequest::Link(link),
                DeltaCursor::Complete(_) => break,
            }
        }
        self.update_catalog(|tx| {
            let item = item_by_inode(tx, work.item.inode)?.ok_or(EngineError::NotFound)?;
            let queued = job(tx, item.inode)?.ok_or(EngineError::NotFound)?;
            clear_attempt(tx, work)?;
            if queued.deleted {
                return Ok(());
            }
            let parent = item
                .parent_id
                .as_deref()
                .ok_or(EngineError::InvalidMutation)?;
            let mut affected = crate::namespace::AffectedNames::default();
            affected.record_item(tx, &item.item_id)?;
            let name = conflict_name(tx, parent, &item, self.write_time(), &remote)?;
            let original_id = queued.remote_id.as_deref();
            // A stale conditional upload forks the item; a destination-name
            // collision can keep the current item and use a different destination.
            if matches!(work.action, Action::Upload) {
                tx.execute(
                    "UPDATE items SET item_id = 'local:' || lower(hex(randomblob(16))),
                     etag = NULL, ctag = NULL, quickxor_hash = NULL, remote_missing = 0 WHERE inode = ?1",
                    [item.inode],
                )?;
                tx.execute(
                    "UPDATE writeback_queue SET remote_id = NULL, remote_parent = NULL, remote_name = NULL WHERE inode = ?1",
                    [item.inode],
                )?;
            }
            tx.execute(
                "UPDATE items SET name = ?2, alias = NULL, sync_state = 3 WHERE inode = ?1",
                params![item.inode, name],
            )?;
            tx.execute(
                "UPDATE writeback_queue SET generation = generation + 1, conflict = 1 WHERE inode = ?1",
                [item.inode],
            )?;
            // Restore the original at its current cloud path. Existing open handles
            // keep the inode associated with the local bytes.
            for original in remote.into_values().filter(|entry| {
                (matches!(work.action, Action::Upload) && Some(entry.item_id.as_str()) == original_id)
                    || (entry.parent_id.as_deref() == Some(parent)
                        && entry.name.as_deref() == Some(&item.intended_name))
            }) {
                affected.record_item(tx, &original.item_id)?;
                if let Some(name) = &original.name {
                    affected.insert(original.parent_id.clone(), name.clone());
                }
                let revision = tx.query_row("SELECT revision FROM delta_checkpoint", [], |row| row.get(0))?;
                super::upsert_remote_item(tx, original, revision, 0)?;
            }
            affected.reconcile(tx, false)?;
            Ok(())
        })?;
        if let Some(stage) = work
            .attempt
            .as_ref()
            .and_then(|attempt| attempt.stage.as_deref())
        {
            self.retire_unused_stage(stage)?;
        }
        Ok(())
    }
}

fn conflict_name(
    db: &Connection,
    parent: &str,
    item: &CatalogItem,
    seconds: i64,
    remote: &HashMap<String, RemoteItem>,
) -> Result<String, EngineError> {
    let name = item.intended_name.as_str();
    let (stem, extension) = Path::new(name)
        .extension()
        .filter(|_| item.kind == ItemKind::File)
        .and_then(|extension| extension.to_str())
        .map_or((name, ""), |extension| {
            name.split_at(name.len() - extension.len() - 1)
        });
    let date: String = db.query_row("SELECT date(?1, 'unixepoch')", [seconds], |row| row.get(0))?;
    for number in 1.. {
        let label = if number == 1 {
            format!("conflict {date}")
        } else {
            format!("conflict {date} {number}")
        };
        let suffix = format!(" ({label})");
        let extension = truncate_name(extension, 255 - suffix.len());
        let stem = truncate_name(stem, 255 - suffix.len() - extension.len());
        let candidate = format!("{stem}{suffix}{extension}");
        if lookup(db, parent, &candidate)?.is_none()
            && !remote.values().any(|entry| {
                entry.parent_id.as_deref() == Some(parent)
                    && entry.name.as_deref() == Some(&candidate)
            })
        {
            return Ok(candidate);
        }
    }
    unreachable!()
}

fn truncate_name(name: &str, max_bytes: usize) -> &str {
    &name[..name.floor_char_boundary(max_bytes.min(name.len()))]
}
