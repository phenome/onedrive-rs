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
use crate::{CloudError, DeltaChange, DeltaCursor, DeltaRequest, RemoteItem, UploadTarget};

pub(super) struct Writeback {
    root: PathBuf,
    changed: Arc<Notify>,
    open_files: Mutex<HashMap<u64, usize>>,
}

impl Writeback {
    pub(super) fn open(database: &Path, db: &mut Connection) -> Result<Self, EngineError> {
        let mut root = database.as_os_str().to_owned();
        root.push(".staging");
        let root = PathBuf::from(root);
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
            .prepare("SELECT stage FROM writeback_queue WHERE stage IS NOT NULL")?
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
            io::copy(&mut old.take(size), &mut file)?;
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

fn enqueue_existing(tx: &Transaction<'_>, item: &CatalogItem) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO writeback_queue (inode, remote_id, remote_parent, remote_name, conflict)
         VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(inode) DO NOTHING",
        params![
            item.inode,
            item.item_id,
            item.parent_id,
            item.name,
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

/// The Purge preflight must run under the same catalog lock as cache removal.
pub(super) fn refuse_dirty(state: SyncState) -> Result<(), EngineError> {
    match state {
        SyncState::Dirty | SyncState::Uploading => Err(EngineError::Dirty),
        _ => Ok(()),
    }
}

impl Engine {
    pub fn create(
        &self,
        parent: u64,
        name: &str,
        kind: ItemKind,
    ) -> Result<CatalogItem, EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent = directory(&tx, parent)?;
        validate_name(&parent, name, kind)?;
        if lookup(&tx, &parent.item_id, name)?.is_some() {
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
        let item = item_by_inode(&tx, inode as u64)?.ok_or(EngineError::NotFound)?;
        tx.commit()?;
        self.writeback.changed.notify_one();
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

            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
            let current_job = job(&tx, inode)?;
            if before != current
                || previous.as_ref().map(|job| job.generation)
                    != current_job.as_ref().map(|job| job.generation)
            {
                continue;
            }
            if prefix
                .as_ref()
                .is_some_and(|bytes| bytes.len() as u64 != keep)
            {
                return Err(EngineError::InvalidRange);
            }
            enqueue_existing(&tx, &current)?;
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
                "UPDATE items SET size = ?2, mtime = ?3, sync_state = CASE WHEN sync_state = 3 THEN 3 ELSE 1 END WHERE inode = ?1",
                params![inode, size, self.write_time()],
            )?;
            let current = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
            // Disarm automatic unlink before committing a reference to this file.
            let (_file, _path) = file.keep().map_err(|err| err.error)?;
            tx.commit()?;
            self.writeback.retire(stage);
            self.writeback.changed.notify_one();
            return Ok(current);
        }
    }

    pub(super) fn has_staged(&self, db: &Connection, inode: u64) -> Result<bool, EngineError> {
        Ok(job(db, inode)?.is_some_and(|job| job.stage.is_some()))
    }

    pub(super) fn read_staged(
        &self,
        db: &Connection,
        inode: u64,
        offset: u64,
        output: &mut [u8],
    ) -> Result<bool, EngineError> {
        let Some(stage) = job(db, inode)?.and_then(|job| job.stage) else {
            return Ok(false);
        };
        File::open(self.writeback.root.join(stage))?.read_exact_at(output, offset)?;
        Ok(true)
    }

    /// Refuse Purge while there is local work queued for this item.
    pub fn check_purge(&self, item_id: &str) -> Result<(), EngineError> {
        let db = self.db();
        let item = item_by_id(&db, item_id)?.ok_or(EngineError::NotFound)?;
        refuse_dirty(item.sync_state)?;
        if job(&db, item.inode)?.is_some() {
            return Err(EngineError::Dirty);
        }
        Ok(())
    }
}

impl Engine {
    pub fn unlink(&self, parent: u64, name: &str, kind: ItemKind) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent = directory(&tx, parent)?;
        let item = lookup(&tx, &parent.item_id, name)?.ok_or(EngineError::NotFound)?;
        check_removable(&tx, &item, kind)?;
        queue_delete(&tx, &item)?;
        tx.commit()?;
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
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent = directory(&tx, parent)?;
        let new_parent = directory(&tx, new_parent)?;
        let item = lookup(&tx, &parent.item_id, name)?.ok_or(EngineError::NotFound)?;
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
                    .map(|id| item_by_id(&tx, id))
                    .transpose()?
                    .flatten();
            }
        }

        if let Some(replaced) = lookup(&tx, &new_parent.item_id, new_name)? {
            if no_replace {
                return Err(EngineError::AlreadyExists);
            }
            check_removable(&tx, &replaced, item.kind)?;
            queue_delete(&tx, &replaced)?;
        }
        enqueue_existing(&tx, &item)?;
        tx.execute(
            "UPDATE items SET parent_id = ?2, name = ?3, sync_state = CASE WHEN sync_state = 3 THEN 3 ELSE 1 END WHERE inode = ?1",
            params![item.inode, new_parent.item_id, new_name],
        )?;
        tx.execute(
            "UPDATE writeback_queue SET generation = generation + 1 WHERE inode = ?1",
            [item.inode],
        )?;
        tx.commit()?;
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
        *self
            .writeback
            .open_files
            .lock()
            .unwrap()
            .entry(inode)
            .or_default() += 1;
        Ok(())
    }

    pub(crate) fn release_file(&self, inode: u64) {
        let mut files = self.writeback.open_files.lock().unwrap();
        if let Some(count) = files.get_mut(&inode) {
            *count -= 1;
            if *count == 0 {
                files.remove(&inode);
            }
        }
        self.writeback.changed.notify_one();
    }

    /// Drain currently runnable work. The catalog lock is released during all cloud calls.
    pub async fn drain_uploads(&self) -> Result<usize, EngineError> {
        // Delta must not apply a snapshot taken before an upload acknowledgment.
        let _sync = self.syncing.lock().await;
        let mut completed = 0;
        while let Some(work) = self.next_upload()? {
            let result = self.upload(&work).await;
            match result {
                Ok(()) => completed += 1,
                Err(EngineError::Cloud(CloudError::Conflict | CloudError::NotFound))
                    if matches!(work.action, Action::Upload(_) | Action::CreateFile(_)) =>
                {
                    self.preserve_conflict(&work).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(completed)
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

    fn next_upload(&self) -> Result<Option<Work>, EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let open_files = self.writeback.open_files.lock().unwrap();
        let inodes = tx
            .prepare("SELECT inode FROM writeback_queue ORDER BY deleted DESC, sequence")?
            .query_map([], |row| row.get::<_, u64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for inode in inodes {
            let item = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
            let queued = job(&tx, inode)?.ok_or(EngineError::NotFound)?;
            let action = if queued.deleted {
                // Keep the remote file available until the last POSIX handle closes.
                if open_files.contains_key(&inode) {
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
                    || queued.remote_name.as_deref() != Some(&item.name);
                if queued.remote_id.is_none() || (queued.stage.is_none() && path_changed) {
                    let occupied: bool = tx.query_row(
                        "SELECT EXISTS (SELECT 1 FROM writeback_queue
                          WHERE inode <> ?1 AND remote_parent = ?2 AND remote_name = ?3)",
                        params![inode, item.parent_id, item.name],
                        |row| row.get(0),
                    )?;
                    if occupied {
                        continue;
                    }
                }
                match (&queued.remote_id, item.kind, &queued.stage) {
                    (None, ItemKind::Folder, _) => Action::CreateFolder,
                    (None, ItemKind::File, _) => Action::CreateFile(self.staged_bytes(&queued)?),
                    (Some(_), ItemKind::File, Some(_)) => {
                        Action::Upload(self.staged_bytes(&queued)?)
                    }
                    (Some(_), _, _) if path_changed => Action::Move,
                    _ => Action::Acknowledge,
                }
            };
            tx.commit()?;
            return Ok(Some(Work {
                item,
                queued,
                action,
            }));
        }
        Ok(None)
    }

    fn staged_bytes(&self, queued: &Job) -> Result<Vec<u8>, EngineError> {
        queued.stage.as_ref().map_or(Ok(Vec::new()), |name| {
            Ok(fs::read(self.writeback.root.join(name))?)
        })
    }

    async fn upload(&self, work: &Work) -> Result<(), EngineError> {
        let parent = work
            .item
            .parent_id
            .as_deref()
            .ok_or(EngineError::InvalidMutation)?;
        let name = &work.item.name;
        let cloud_id = work.queued.remote_id.as_deref();
        let remote = match &work.action {
            Action::Delete => {
                if let Some(id) = cloud_id {
                    match self.cloud.delete_item(id).await {
                        Ok(()) | Err(CloudError::NotFound) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                self.finish_delete(work)?;
                return Ok(());
            }
            Action::CreateFolder => self.cloud.create_folder(parent, name).await?,
            Action::CreateFile(bytes) => {
                self.cloud
                    .upload(
                        UploadTarget::New {
                            parent_id: parent,
                            name,
                        },
                        bytes,
                        None,
                    )
                    .await?
            }
            Action::Upload(bytes) => {
                let etag = work.item.etag.as_deref().ok_or(CloudError::Conflict)?;
                let item_id = cloud_id.ok_or(EngineError::InvalidMutation)?;
                self.cloud
                    .upload(UploadTarget::Existing { item_id }, bytes, Some(etag))
                    .await?
            }
            Action::Move => {
                self.cloud
                    .move_item(cloud_id.ok_or(EngineError::InvalidMutation)?, parent, name)
                    .await?
            }
            Action::Acknowledge => return self.acknowledge(work, None),
        };
        self.acknowledge(work, Some(remote))
    }

    fn finish_delete(&self, work: &Work) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stage = job(&tx, work.item.inode)?.and_then(|job| job.stage);
        tx.execute("DELETE FROM items WHERE inode = ?1", [work.item.inode])?;
        tx.commit()?;
        self.writeback.retire(stage.as_deref());
        self.cache.reclaim(&db)?;
        Ok(())
    }

    fn acknowledge(&self, work: &Work, remote: Option<RemoteItem>) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let inode = work.item.inode;
        let current = item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
        let queued = job(&tx, inode)?.ok_or(EngineError::NotFound)?;
        let sent_content = matches!(work.action, Action::CreateFile(_) | Action::Upload(_));
        let retire = sent_content && queued.stage == work.queued.stage;
        if retire {
            tx.execute(
                "UPDATE writeback_queue SET stage = NULL WHERE inode = ?1",
                [inode],
            )?;
        }
        let completed = if let Some(remote) = remote {
            let submitted_path = !matches!(work.action, Action::Upload(_));
            let parent = if submitted_path {
                work.item.parent_id.as_deref()
            } else {
                work.queued.remote_parent.as_deref()
            };
            let name = if submitted_path {
                Some(work.item.name.as_str())
            } else {
                work.queued.remote_name.as_deref()
            };
            tx.execute(
                "UPDATE writeback_queue SET remote_id = ?2, remote_parent = ?3, remote_name = ?4 WHERE inode = ?1",
                params![inode, remote.item_id, parent, name],
            )?;
            // Only a content acknowledgment advances the base of a concurrent edit.
            // If a pure move changed just the eTag, its cTag still identifies the bytes.
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
            if queued.stage.is_none() || retire {
                tx.execute(
                    "UPDATE items SET size = ?2, mtime = ?3 WHERE inode = ?1",
                    params![inode, remote.size, remote.mtime],
                )?;
            }
            (retire || queued.stage.is_none())
                && current.parent_id.as_deref() == parent
                && Some(current.name.as_str()) == name
        } else {
            queued.generation == work.queued.generation
        };
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
        tx.commit()?;
        if retire {
            self.writeback.retire(queued.stage.as_deref());
        }
        self.cache.reclaim(&db)?;
        Ok(())
    }
}

enum Action {
    Delete,
    CreateFolder,
    CreateFile(Vec<u8>),
    Upload(Vec<u8>),
    Move,
    Acknowledge,
}

struct Work {
    item: CatalogItem,
    queued: Job,
    action: Action,
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
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = item_by_inode(&tx, work.item.inode)?.ok_or(EngineError::NotFound)?;
        let queued = job(&tx, item.inode)?.ok_or(EngineError::NotFound)?;
        if queued.deleted {
            return Ok(());
        }
        let parent = item
            .parent_id
            .as_deref()
            .ok_or(EngineError::InvalidMutation)?;
        let name = conflict_name(&tx, parent, &item.name, self.write_time(), &remote)?;
        let original_id = queued.remote_id.as_deref();
        tx.execute(
            "UPDATE items SET item_id = 'local:' || lower(hex(randomblob(16))), name = ?2,
             etag = NULL, ctag = NULL, quickxor_hash = NULL, sync_state = 3, remote_missing = 0
             WHERE inode = ?1",
            params![item.inode, name],
        )?;
        tx.execute(
            "UPDATE writeback_queue SET remote_id = NULL, remote_parent = NULL, remote_name = NULL,
             generation = generation + 1, conflict = 1 WHERE inode = ?1",
            [item.inode],
        )?;
        // Restore the original at its current cloud path. Existing open handles
        // keep the inode associated with the local bytes.
        for original in remote.into_values().filter(|entry| {
            Some(entry.item_id.as_str()) == original_id
                || (entry.parent_id.as_deref() == Some(parent)
                    && entry.name.as_deref() == Some(&item.name))
        }) {
            let existing = lookup(
                &tx,
                original.parent_id.as_deref().unwrap_or_default(),
                original.name.as_deref().unwrap_or_default(),
            )?;
            if existing.is_none() || existing.is_some_and(|local| local.item_id == original.item_id)
            {
                super::upsert_remote_item(&tx, original)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn conflict_name(
    db: &Connection,
    parent: &str,
    name: &str,
    seconds: i64,
    remote: &HashMap<String, RemoteItem>,
) -> Result<String, EngineError> {
    let (stem, extension) = Path::new(name)
        .extension()
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
        let candidate = format!("{stem} ({label}){extension}");
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
