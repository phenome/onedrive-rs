use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use thiserror::Error;

use crate::namespace::AffectedNames;
use crate::{
    Clock, CloudDrive, CloudError, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest, ItemKind,
    RemoteItem,
};

// Recover within the same call when both the saved cursor and the first
// replacement continuation expire, while keeping repeated expiry bounded.
const MAX_FULL_SYNC_RESTARTS: usize = 2;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("metadata catalog error")]
    Catalog(#[from] rusqlite::Error),
    #[error("metadata catalog requires SQLite WAL mode")]
    WalUnavailable,
    #[error("another engine advanced the delta checkpoint")]
    SyncSuperseded,
    #[error(transparent)]
    Cloud(#[from] CloudError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogItem {
    pub inode: u64,
    pub item_id: String,
    pub parent_id: Option<String>,
    /// The name exposed by the local namespace, including any temporary alias.
    pub name: String,
    pub kind: ItemKind,
    pub size: u64,
    pub mtime: i64,
    pub etag: Option<String>,
    pub ctag: Option<String>,
    pub quickxor_hash: Option<String>,
    pub last_accessed_at: Option<i64>,
    pub pin_state: PinState,
    pub sync_state: SyncState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PinState {
    None = 0,
    Pinned = 1,
    UnpinOverride = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncState {
    Clean = 0,
    Dirty = 1,
    Uploading = 2,
    Conflict = 3,
}

impl FromSql for PinState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let integer = value.as_i64()?;
        [Self::None, Self::Pinned, Self::UnpinOverride]
            .into_iter()
            .find(|state| *state as i64 == integer)
            .ok_or(FromSqlError::OutOfRange(integer))
    }
}

impl FromSql for SyncState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let integer = value.as_i64()?;
        [Self::Clean, Self::Dirty, Self::Uploading, Self::Conflict]
            .into_iter()
            .find(|state| *state as i64 == integer)
            .ok_or(FromSqlError::OutOfRange(integer))
    }
}

pub struct Engine {
    db: Mutex<Connection>,
    cloud: Arc<dyn CloudDrive>,
    clock: Arc<dyn Clock>,
    syncing: tokio::sync::Mutex<()>,
}

const ITEM_COLUMNS: &str =
    "inode, item_id, parent_id, coalesce(alias, name), kind, size, mtime, etag, ctag, quickxor_hash, last_accessed_at, sync_state,
     (SELECT pin_state FROM pin_directives WHERE item_id = items.item_id)";

impl Engine {
    pub fn open(
        database: impl AsRef<Path>,
        cloud: Arc<dyn CloudDrive>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        let mut db = Connection::open(database)?;
        let journal: String = db.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !journal.eq_ignore_ascii_case("wal") {
            return Err(EngineError::WalUnavailable);
        }
        db.pragma_update(None, "synchronous", "FULL")?;
        let transaction = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(include_str!("schema.sql"))?;
        crate::namespace::migrate(&transaction)?;
        transaction.commit()?;
        Ok(Self {
            db: Mutex::new(db),
            cloud,
            clock,
            syncing: tokio::sync::Mutex::new(()),
        })
    }

    fn db(&self) -> MutexGuard<'_, Connection> {
        self.db.lock().expect("catalog lock poisoned")
    }

    pub async fn sync(&self) -> Result<(), EngineError> {
        let _sync = self.syncing.lock().await;
        let mut restarts = 0;
        loop {
            let (link, revision) = self.ready_checkpoint().await?;
            let request = link.map_or(DeltaRequest::Initial, DeltaRequest::Link);
            let can_resync = request != DeltaRequest::Initial;
            let page = match self.cloud.delta(request).await {
                Ok(page) => page,
                Err(CloudError::Throttled { retry_after }) => {
                    self.record_retry_after(retry_after)?;
                    self.clock.sleep(retry_after).await;
                    continue;
                }
                Err(CloudError::ResyncRequired) if can_resync => {
                    // Discard expired progress even when this invocation is out of retries.
                    self.restart_full_sync(revision)?;
                    if restarts == MAX_FULL_SYNC_RESTARTS {
                        return Err(CloudError::ResyncRequired.into());
                    }
                    restarts += 1;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if self.commit_page(page, revision)? {
                return Ok(());
            }
        }
    }

    async fn ready_checkpoint(&self) -> Result<(Option<String>, i64), EngineError> {
        loop {
            let (link, revision, retry_at_ms) = self.db().query_row(
                "SELECT link, revision, retry_at_ms FROM delta_checkpoint WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let retry_at = Duration::from_millis(retry_at_ms);
            let remaining = retry_at.saturating_sub(self.clock.now());
            if remaining.is_zero() {
                return Ok((link, revision));
            }
            self.clock.sleep(remaining).await;
        }
    }

    fn record_retry_after(&self, delay: Duration) -> Result<(), EngineError> {
        // Round up so sub-millisecond persistence never shortens Retry-After.
        let retry_at = self.clock.now().saturating_add(delay);
        let milliseconds = retry_at.as_nanos().div_ceil(1_000_000);
        let milliseconds = i64::try_from(milliseconds).unwrap_or(i64::MAX);
        self.db().execute(
            "UPDATE delta_checkpoint SET retry_at_ms = max(retry_at_ms, ?1)",
            [milliseconds],
        )?;
        Ok(())
    }

    fn commit_page(&self, page: DeltaPage, revision: i64) -> Result<bool, EngineError> {
        let mut db = self.db();
        let transaction = db.transaction()?;
        let (link, complete) = match page.cursor {
            DeltaCursor::Next(link) => (link, false),
            DeltaCursor::Complete(link) => (link, true),
        };
        advance_checkpoint(&transaction, revision, Some(&link))?;
        let full_scan = transaction.query_row(
            "SELECT full_scan FROM delta_checkpoint WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let mut affected = AffectedNames::default();
        for (position, change) in page.changes.into_iter().enumerate() {
            apply_change(
                &transaction,
                change,
                full_scan,
                &mut affected,
                revision + 1,
                position,
            )?;
        }
        if complete {
            reconcile_catalog(&transaction, full_scan, &mut affected)?;
            transaction.execute("UPDATE delta_checkpoint SET full_scan = 0", [])?;
        }
        affected.reconcile(&transaction, complete)?;
        transaction.commit()?;
        Ok(complete)
    }

    fn restart_full_sync(&self, revision: i64) -> Result<(), EngineError> {
        let mut db = self.db();
        let transaction = db.transaction()?;
        advance_checkpoint(&transaction, revision, None)?;
        let mut affected = AffectedNames::default();
        affected.extend(
            &transaction,
            "SELECT parent_id, name
            FROM delta_deletions JOIN items USING (item_id)",
        )?;
        transaction.execute_batch(
            "UPDATE delta_checkpoint SET full_scan = 1;
             DELETE FROM delta_deletions;
             DELETE FROM full_sync_seen;",
        )?;
        affected.reconcile(&transaction, false)?;
        transaction.commit()?;
        Ok(())
    }

    /// Store the explicit directive. Effective sticky pin policy belongs in Engine.
    pub fn set_pin_state(&self, item_id: &str, pin: PinState) -> Result<bool, EngineError> {
        let db = self.db();
        let changed = match pin {
            PinState::None => {
                db.execute("DELETE FROM pin_directives WHERE item_id = ?1", [item_id])?
            }
            _ => db.execute(
                "INSERT INTO pin_directives (item_id, pin_state)
                 SELECT ?1, ?2 WHERE EXISTS (SELECT 1 FROM items WHERE item_id = ?1)
                 ON CONFLICT(item_id) DO UPDATE SET pin_state = excluded.pin_state",
                params![item_id, pin as i64],
            )?,
        };
        Ok(changed != 0)
    }

    pub fn pin_state(&self, item_id: &str) -> Result<PinState, EngineError> {
        let value = self
            .db()
            .query_row(
                "SELECT pin_state FROM pin_directives WHERE item_id = ?1",
                [item_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value.unwrap_or(PinState::None))
    }

    pub fn set_sync_state(&self, item_id: &str, state: SyncState) -> Result<bool, EngineError> {
        Ok(self.db().execute(
            "UPDATE items SET sync_state = ?2 WHERE item_id = ?1",
            params![item_id, state as i64],
        )? != 0)
    }

    pub fn root(&self) -> Result<Option<CatalogItem>, EngineError> {
        Ok(root(&self.db())?)
    }

    pub fn lookup(&self, parent_id: &str, name: &str) -> Result<Option<CatalogItem>, EngineError> {
        Ok(lookup(&self.db(), parent_id, name)?)
    }

    pub fn getattr(&self, inode: u64) -> Result<Option<CatalogItem>, EngineError> {
        Ok(self
            .db()
            .prepare_cached(&format!(
                "SELECT {ITEM_COLUMNS} FROM items WHERE inode = ?1"
            ))?
            .query_row([inode], catalog_item)
            .optional()?)
    }

    pub fn item(&self, item_id: &str) -> Result<Option<CatalogItem>, EngineError> {
        Ok(item_by_id(&self.db(), item_id)?)
    }

    pub fn list_children(&self, parent_id: &str) -> Result<Vec<CatalogItem>, EngineError> {
        Ok(self
            .db()
            .prepare_cached(&format!(
                "SELECT {ITEM_COLUMNS} FROM items WHERE parent_id = ?1 ORDER BY coalesce(alias, name), inode"
            ))?
            .query_map([parent_id], catalog_item)?
            .collect::<Result<_, _>>()?)
    }

    pub fn resolve_path(&self, path: impl AsRef<Path>) -> Result<Option<CatalogItem>, EngineError> {
        let db = self.db();
        let snapshot = db.unchecked_transaction()?;
        let mut components = path.as_ref().components();
        if components.next() != Some(Component::RootDir) {
            return Ok(None);
        }
        let mut current = root(&snapshot)?;
        for component in components {
            let (Component::Normal(name), Some(parent)) = (component, current) else {
                return Ok(None);
            };
            let Some(name) = name.to_str() else {
                return Ok(None);
            };
            if parent.kind != ItemKind::Folder {
                return Ok(None);
            }
            current = lookup(&snapshot, &parent.item_id, name)?;
        }
        Ok(current)
    }

    pub fn path_for(&self, item_id: &str) -> Result<Option<PathBuf>, EngineError> {
        let db = self.db();
        let snapshot = db.unchecked_transaction()?;
        let mut current = item_by_id(&snapshot, item_id)?;
        let mut seen = HashSet::new();
        let mut names = Vec::new();
        while let Some(item) = current {
            let Some(parent_id) = item.parent_id else {
                if item.kind != ItemKind::Folder {
                    return Ok(None);
                }
                let mut path = PathBuf::from("/");
                path.extend(names.into_iter().rev());
                return Ok(Some(path));
            };
            if !seen.insert(item.item_id) {
                return Ok(None);
            }
            names.push(item.name);
            current =
                item_by_id(&snapshot, &parent_id)?.filter(|parent| parent.kind == ItemKind::Folder);
        }
        Ok(None)
    }
}

fn root(db: &Connection) -> rusqlite::Result<Option<CatalogItem>> {
    db.prepare_cached(&format!(
        "SELECT {ITEM_COLUMNS} FROM items WHERE parent_id IS NULL AND kind = 1"
    ))?
    .query_row([], catalog_item)
    .optional()
}

fn lookup(db: &Connection, parent_id: &str, name: &str) -> rusqlite::Result<Option<CatalogItem>> {
    db.prepare_cached(&format!(
        "SELECT {ITEM_COLUMNS} FROM items WHERE parent_id = ?1 AND coalesce(alias, name) = ?2"
    ))?
    .query_row(params![parent_id, name], catalog_item)
    .optional()
}

fn item_by_id(db: &Connection, item_id: &str) -> rusqlite::Result<Option<CatalogItem>> {
    db.prepare_cached(&format!(
        "SELECT {ITEM_COLUMNS} FROM items WHERE item_id = ?1"
    ))?
    .query_row([item_id], catalog_item)
    .optional()
}

fn catalog_item(row: &Row<'_>) -> rusqlite::Result<CatalogItem> {
    Ok(CatalogItem {
        inode: row.get(0)?,
        item_id: row.get(1)?,
        parent_id: row.get(2)?,
        name: row.get(3)?,
        kind: if row.get::<_, bool>(4)? {
            ItemKind::Folder
        } else {
            ItemKind::File
        },
        size: row.get(5)?,
        mtime: row.get(6)?,
        etag: row.get(7)?,
        ctag: row.get(8)?,
        quickxor_hash: row.get(9)?,
        last_accessed_at: row.get(10)?,
        sync_state: row.get(11)?,
        pin_state: row
            .get::<_, Option<PinState>>(12)?
            .unwrap_or(PinState::None),
    })
}

fn advance_checkpoint(
    transaction: &Transaction<'_>,
    revision: i64,
    link: Option<&str>,
) -> Result<(), EngineError> {
    if transaction.execute(
        "UPDATE delta_checkpoint SET link = ?1, revision = revision + 1
         WHERE singleton = 1 AND revision = ?2",
        params![link, revision],
    )? == 0
    {
        return Err(EngineError::SyncSuperseded);
    }
    Ok(())
}

fn apply_change(
    transaction: &Transaction<'_>,
    change: DeltaChange,
    full_scan: bool,
    affected: &mut AffectedNames,
    revision: i64,
    position: usize,
) -> rusqlite::Result<()> {
    match change {
        DeltaChange::Delete(item_id) => {
            affected.record_item(transaction, &item_id)?;
            transaction
                .prepare_cached("INSERT OR IGNORE INTO delta_deletions VALUES (?1)")?
                .execute([item_id])?;
        }
        DeltaChange::Upsert(item) => {
            affected.record_item(transaction, &item.item_id)?;
            record_sighting(transaction, &item.item_id, full_scan)?;
            if let Some(name) = &item.name {
                affected.insert(item.parent_id.clone(), name.clone());
                upsert_remote_item(transaction, item, revision, position)?;
            }
        }
    }
    Ok(())
}

fn upsert_remote_item(
    transaction: &Transaction<'_>,
    item: RemoteItem,
    revision: i64,
    position: usize,
) -> rusqlite::Result<()> {
    transaction
        .prepare_cached(
            "INSERT INTO items (inode, item_id, parent_id, name, kind, size, mtime, etag, ctag,
                            quickxor_hash, claim_revision, claim_position)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(item_id) DO UPDATE SET
             alias = CASE WHEN items.parent_id IS excluded.parent_id AND items.name = excluded.name
                          THEN items.alias END,
             parent_id = excluded.parent_id, name = excluded.name, kind = excluded.kind,
             size = excluded.size, mtime = excluded.mtime, etag = excluded.etag,
             ctag = excluded.ctag, quickxor_hash = excluded.quickxor_hash,
             claim_revision = excluded.claim_revision, claim_position = excluded.claim_position
         WHERE items.sync_state = 0",
        )?
        .execute(params![
            reserved_inode(&item),
            item.item_id,
            item.parent_id,
            item.name,
            item.kind == ItemKind::Folder,
            item.size,
            item.mtime,
            item.etag,
            item.ctag,
            item.quickxor_hash,
            revision,
            position
        ])?;
    Ok(())
}

fn reserved_inode(item: &RemoteItem) -> Option<u64> {
    (item.parent_id.is_none() && item.kind == ItemKind::Folder).then_some(1)
}

// Even a nameless stub establishes presence in a full enumeration.
fn record_sighting(
    transaction: &Transaction<'_>,
    item_id: &str,
    full_scan: bool,
) -> rusqlite::Result<()> {
    transaction
        .prepare_cached("DELETE FROM delta_deletions WHERE item_id = ?1")?
        .execute([item_id])?;
    transaction
        .prepare_cached(
            "UPDATE items SET remote_missing = 0 WHERE item_id = ?1 AND remote_missing = 1",
        )?
        .execute([item_id])?;
    if full_scan {
        transaction
            .prepare_cached("INSERT OR IGNORE INTO full_sync_seen VALUES (?1)")?
            .execute([item_id])?;
    }
    Ok(())
}

fn reconcile_catalog(
    transaction: &Transaction<'_>,
    full_scan: bool,
    affected: &mut AffectedNames,
) -> rusqlite::Result<()> {
    // Wait for the final page so later moves can remove children from deleted folders.
    let mut mark_missing = transaction.prepare(
        "WITH RECURSIVE deleted(item_id) AS (
             SELECT item_id FROM delta_deletions
             UNION
             SELECT i.item_id FROM items AS i JOIN deleted AS d ON i.parent_id = d.item_id
         )
         UPDATE items SET remote_missing = 1
         WHERE remote_missing = 0 AND item_id IN (
             SELECT item_id FROM deleted
             UNION
             SELECT item_id FROM items WHERE ?1 AND item_id NOT IN full_sync_seen
         )
         RETURNING parent_id, name",
    )?;
    affected.read(&mut mark_missing.query([full_scan])?)?;
    prune_remote_deletions(transaction, affected)?;
    transaction.execute_batch("DELETE FROM delta_deletions; DELETE FROM full_sync_seen;")
}

fn prune_remote_deletions(
    transaction: &Transaction<'_>,
    affected: &mut AffectedNames,
) -> rusqlite::Result<()> {
    // Retain the parent chain needed to reach local changes. Persist the absence
    // so a later sync can remove the chain after the local changes are resolved.
    let mut prune = transaction.prepare(
        "WITH RECURSIVE protected(item_id, parent_id) AS (
             SELECT item_id, parent_id FROM items WHERE sync_state <> 0
             UNION
             SELECT i.item_id, i.parent_id FROM items AS i JOIN protected AS p ON i.item_id = p.parent_id
         )
         DELETE FROM items WHERE remote_missing = 1
           AND item_id NOT IN (SELECT item_id FROM protected)
         RETURNING parent_id, name",
    )?;
    affected.read(&mut prune.query([])?)
}
