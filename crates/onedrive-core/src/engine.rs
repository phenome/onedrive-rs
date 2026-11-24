use std::collections::HashSet;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use thiserror::Error;

use crate::namespace::AffectedNames;
use crate::{
    Clock, CloudDrive, CloudError, Config, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest,
    ItemKind, RemoteItem,
};

mod catalog_invalidation;
mod desktop_changes;
pub use desktop_changes::DesktopChanges;
mod relocation;
mod retention;
mod sparse_cache;
mod thumbnails;
mod writeback;

pub(crate) use catalog_invalidation::{CatalogInvalidation, CatalogObserver};
pub use retention::CacheStatus;

use sparse_cache::SparseCache;
use thumbnails::ThumbnailCache;
pub(crate) use thumbnails::default_thumbnail_root;

// Recover within the same call when both the saved cursor and the first
// replacement continuation expire, while keeping repeated expiry bounded.
const MAX_FULL_SYNC_RESTARTS: usize = 2;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("metadata catalog error")]
    Catalog(#[from] rusqlite::Error),
    #[error("metadata catalog requires SQLite WAL mode")]
    WalUnavailable,
    #[error("could not checkpoint the catalog for relocation")]
    CatalogRelocation,
    #[error("another engine advanced the delta checkpoint")]
    SyncSuperseded,
    #[error("item does not exist")]
    NotFound,
    #[error("cannot purge {item_id}: pending upload")]
    PendingUpload { item_id: String },
    #[error("cannot purge {item_id}: unresolved conflict")]
    UnresolvedConflict { item_id: String },
    #[error("cache hydration paused to preserve the free-disk floor")]
    LowDisk,
    #[error("cannot read a directory as a file")]
    IsDirectory,
    #[error("item is not a directory")]
    NotDirectory,
    #[error("directory is not empty")]
    NotEmpty,
    #[error("name already exists")]
    AlreadyExists,
    #[error("invalid file name or operation")]
    InvalidMutation,
    #[error("top-level .Trash directories are prohibited")]
    TrashProhibited,
    #[error("sparse cache I/O error")]
    CacheIo(#[from] io::Error),
    #[error("thumbnail cache I/O error")]
    ThumbnailIo(#[source] io::Error),
    #[error("invalid cloud thumbnail")]
    InvalidThumbnail(#[from] image::ImageError),
    #[error("could not encode thumbnail PNG")]
    ThumbnailEncoding(#[from] png::EncodingError),
    #[error("original cloud version unavailable; staged edits are retained")]
    OriginalVersionUnavailable,
    #[error("cloud range did not match the catalog file size")]
    InvalidRange,
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
    /// Milliseconds since the Unix epoch.
    pub last_accessed_at: Option<i64>,
    pub pin_state: PinState,
    pub sync_state: SyncState,
    pub intended_name: String,
    pub mode: Option<u16>,
    pub local_atime: Option<i64>,
    pub local_mtime: Option<i64>,
    pub unlinked: bool,
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
    cache: SparseCache,
    thumbnails: ThumbnailCache,
    cloud: Arc<dyn CloudDrive>,
    clock: Arc<dyn Clock>,
    syncing: tokio::sync::Mutex<()>,
    config: Config,
    hydrating: tokio::sync::Mutex<()>,
    hydration_paused_low_disk: AtomicBool,
    writeback: writeback::Writeback,
    observers: Mutex<Vec<Weak<catalog_invalidation::Observer>>>,
}

const ITEM_COLUMNS: &str =
    "inode, item_id, parent_id, coalesce(alias, name), kind, size, mtime, etag, ctag, quickxor_hash, last_accessed_at, sync_state,
     (SELECT pin_state FROM pin_directives WHERE item_id = items.item_id),
     (SELECT mode FROM local_attributes WHERE inode = items.inode),
     (SELECT atime FROM local_attributes WHERE inode = items.inode),
     (SELECT mtime FROM local_attributes WHERE inode = items.inode),
     EXISTS (SELECT 1 FROM writeback_queue WHERE inode = items.inode AND deleted), name";

impl Engine {
    pub fn open(
        database: impl AsRef<Path>,
        cloud: Arc<dyn CloudDrive>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        let database = database.as_ref();
        let mut cache = database.as_os_str().to_owned();
        cache.push(".cache");
        Self::open_with_cache(database, PathBuf::from(cache), cloud, clock)
    }

    pub fn open_with_cache(
        database: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
        cloud: Arc<dyn CloudDrive>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        Self::open_with_config(database, cache_dir, cloud, clock, Config::default())
    }

    /// Prepare an offline catalog for an atomic filesystem move.
    /// The caller holds exclusive access to both catalog locations.
    pub fn checkpoint_for_relocation(database: &Path) -> Result<(), EngineError> {
        let db =
            Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        // Exiting WAL mode folds committed frames into the main database.
        let mode: String = db.query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))?;
        if mode != "delete" {
            return Err(EngineError::CatalogRelocation);
        }
        Ok(())
    }

    pub fn open_with_config(
        database: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
        cloud: Arc<dyn CloudDrive>,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Result<Self, EngineError> {
        let mut db = Connection::open(database.as_ref())?;
        db.pragma_update(None, "foreign_keys", true)?;
        let journal: String = db.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !journal.eq_ignore_ascii_case("wal") {
            return Err(EngineError::WalUnavailable);
        }
        db.pragma_update(None, "synchronous", "FULL")?;
        let transaction = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(include_str!("schema.sql"))?;
        crate::namespace::migrate(&transaction)?;
        sparse_cache::retire_stale_etag_ranges(&transaction)?;
        retention::initialize_cached_access_times(&transaction, clock.as_ref())?;
        transaction.commit()?;
        catalog_invalidation::install(&db)?;
        desktop_changes::install(&db)?;
        let cache = SparseCache::open(cache_dir.as_ref(), &db)?;
        let writeback = writeback::Writeback::open(database.as_ref(), &mut db)?;
        let engine = Self {
            cache,
            writeback,
            thumbnails: ThumbnailCache::default(),
            db: Mutex::new(db),
            cloud,
            clock,
            config,
            syncing: tokio::sync::Mutex::new(()),
            hydrating: tokio::sync::Mutex::new(()),
            hydration_paused_low_disk: AtomicBool::new(false),
            observers: Mutex::default(),
        };
        engine.recover_cached_generations()?;
        Ok(engine)
    }

    fn db(&self) -> MutexGuard<'_, Connection> {
        self.db.lock().expect("catalog lock poisoned")
    }

    pub async fn sync(&self) -> Result<(), EngineError> {
        self.sync_catalog().await?;
        self.hydrate_pending_pins().await
    }

    /// Commit remote metadata independently of potentially slow content transfers.
    pub async fn sync_catalog(&self) -> Result<(), EngineError> {
        let _sync = self.syncing.lock().await;
        // Age-based eviction also runs when the cloud is unavailable.
        self.evict()?;
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
                return self.evict();
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
        let complete = self.update_catalog(|transaction| {
            let (link, complete) = match page.cursor {
                DeltaCursor::Next(link) => (link, false),
                DeltaCursor::Complete(link) => (link, true),
            };
            transaction.execute(
                "DELETE FROM writeback_recycled WHERE EXISTS (
                    SELECT 1 FROM delta_checkpoint WHERE singleton = 1 AND full_scan AND link IS NULL
                )", [],
            )?;
            advance_checkpoint(transaction, revision, Some(&link))?;
            let full_scan = transaction.query_row(
                "SELECT full_scan FROM delta_checkpoint WHERE singleton = 1",
                [],
                |row| row.get(0),
            )?;
            let mut affected = AffectedNames::default();
            for (position, change) in page.changes.into_iter().enumerate() {
                apply_change(
                    transaction,
                    change,
                    full_scan,
                    &mut affected,
                    revision + 1,
                    position,
                )?;
            }
            retry_deferred_parents(transaction, &mut affected)?;
            if complete {
                reconcile_catalog(transaction, full_scan, &mut affected)?;
                transaction.execute("UPDATE delta_checkpoint SET full_scan = 0", [])?;
            }
            affected.reconcile(transaction, complete)?;
            Ok(complete)
        })?;
        self.reclaim_sparse_cache()?;
        Ok(complete)
    }

    fn update_catalog<T>(
        &self,
        update: impl FnOnce(&Transaction<'_>) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        self.update_catalog_after(update, |_| {})
    }

    fn update_catalog_after<T>(
        &self,
        update: impl FnOnce(&Transaction<'_>) -> Result<T, EngineError>,
        committed: impl FnOnce(&T),
    ) -> Result<T, EngineError> {
        let mut db = self.db();
        let transaction = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DELETE FROM changed_inodes", [])?;
        transaction.execute("DELETE FROM changed_entries", [])?;
        let result = update(&transaction)?;
        let observers: Vec<_> = {
            let mut observers = self.observers.lock().unwrap();
            observers.retain(|observer| observer.strong_count() > 0);
            observers.iter().filter_map(Weak::upgrade).collect()
        };
        let invalidations = if observers.is_empty() {
            Vec::new()
        } else {
            catalog_invalidation::collect(&transaction)?
        };
        transaction.commit()?;
        committed(&result);
        drop(db);
        for observer in observers {
            observer(&invalidations);
        }
        Ok(result)
    }

    pub(crate) fn observe_catalog(&self, observer: &CatalogObserver) {
        self.observers
            .lock()
            .unwrap()
            .push(Arc::downgrade(observer));
    }

    fn restart_full_sync(&self, revision: i64) -> Result<(), EngineError> {
        self.update_catalog(|transaction| {
            advance_checkpoint(transaction, revision, None)?;
            let mut affected = AffectedNames::default();
            affected.extend(
                transaction,
                "SELECT parent_id, name
                FROM delta_deletions JOIN items USING (item_id)",
            )?;
            transaction.execute_batch(
                "UPDATE delta_checkpoint SET full_scan = 1;
                 DELETE FROM delta_deletions;
                 DELETE FROM full_sync_seen;",
            )?;
            affected.reconcile(transaction, false)?;
            Ok(())
        })
    }

    /// Store a directive without fetching or removing content. Use pin, unpin, or purge for user actions.
    pub fn set_pin_state(&self, item_id: &str, pin: PinState) -> Result<bool, EngineError> {
        let db = self.db();
        Ok(write_pin(&db, item_id, pin)?)
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

    pub fn pending_upload_count(&self) -> Result<u64, EngineError> {
        Ok(self
            .db()
            .query_row("SELECT count(*) FROM writeback_queue", [], |row| row.get(0))?)
    }

    pub fn root(&self) -> Result<Option<CatalogItem>, EngineError> {
        Ok(root(&self.db())?)
    }

    pub fn lookup(&self, parent_id: &str, name: &str) -> Result<Option<CatalogItem>, EngineError> {
        Ok(lookup(&self.db(), parent_id, name)?)
    }

    pub fn getattr(&self, inode: u64) -> Result<Option<CatalogItem>, EngineError> {
        Ok(item_by_inode(&self.db(), inode)?)
    }

    pub fn item(&self, item_id: &str) -> Result<Option<CatalogItem>, EngineError> {
        Ok(item_by_id(&self.db(), item_id)?)
    }

    pub fn list_children(&self, parent_id: &str) -> Result<Vec<CatalogItem>, EngineError> {
        Ok(list_children(&self.db(), parent_id)?)
    }

    pub(crate) fn list_directory_entries(
        &self,
        parent_id: &str,
    ) -> Result<Vec<(u64, ItemKind, String)>, EngineError> {
        Ok(self
            .db()
            .prepare_cached(
                "SELECT inode, kind, coalesce(alias, name)
                 FROM items WHERE parent_id = ?1 AND inode NOT IN (SELECT inode FROM writeback_queue WHERE deleted) ORDER BY coalesce(alias, name), inode",
            )?
            .query_map([parent_id], |row| {
                Ok((row.get(0)?, item_kind(row, 1)?, row.get(2)?))
            })?
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
        path_for(&snapshot, item_id)
    }
}

fn list_children(db: &Connection, parent_id: &str) -> rusqlite::Result<Vec<CatalogItem>> {
    db.prepare_cached(&format!(
        "SELECT {ITEM_COLUMNS} FROM items WHERE parent_id = ?1 AND inode NOT IN (SELECT inode FROM writeback_queue WHERE deleted) ORDER BY coalesce(alias, name), inode"
    ))?
    .query_map([parent_id], catalog_item)?
    .collect()
}

fn path_for(db: &Connection, item_id: &str) -> Result<Option<PathBuf>, EngineError> {
    let mut current = item_by_id(db, item_id)?;
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
        current = item_by_id(db, &parent_id)?.filter(|parent| parent.kind == ItemKind::Folder);
    }
    Ok(None)
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
        "SELECT {ITEM_COLUMNS} FROM items WHERE parent_id = ?1 AND coalesce(alias, name) = ?2
         AND inode NOT IN (SELECT inode FROM writeback_queue WHERE deleted)"
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

fn item_by_inode(db: &Connection, inode: u64) -> rusqlite::Result<Option<CatalogItem>> {
    db.prepare_cached(&format!(
        "SELECT {ITEM_COLUMNS} FROM items WHERE inode = ?1"
    ))?
    .query_row([inode], catalog_item)
    .optional()
}

fn item_kind(row: &Row<'_>, index: usize) -> rusqlite::Result<ItemKind> {
    Ok(if row.get::<_, bool>(index)? {
        ItemKind::Folder
    } else {
        ItemKind::File
    })
}

fn catalog_item(row: &Row<'_>) -> rusqlite::Result<CatalogItem> {
    Ok(CatalogItem {
        inode: row.get(0)?,
        item_id: row.get(1)?,
        parent_id: row.get(2)?,
        name: row.get(3)?,
        kind: item_kind(row, 4)?,
        size: row.get(5)?,
        mtime: row.get(6)?,
        etag: row.get(7)?,
        ctag: row.get(8)?,
        quickxor_hash: row.get(9)?,
        last_accessed_at: row.get(10)?,
        sync_state: row.get(11)?,
        intended_name: row.get(17)?,
        mode: row.get(13)?,
        local_atime: row.get(14)?,
        local_mtime: row.get(15)?,
        unlinked: row.get(16)?,

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
            transaction.execute(
                "DELETE FROM writeback_recycled WHERE item_id = ?1",
                [&item_id],
            )?;
            transaction
                .prepare_cached("INSERT OR IGNORE INTO delta_deletions VALUES (?1)")?
                .execute([item_id])?;
        }
        DeltaChange::Upsert(item) => {
            affected.record_item(transaction, &item.item_id)?;
            record_sighting(transaction, &item.item_id, full_scan)?;
            let suppressed = transaction.query_row(
                "SELECT EXISTS (SELECT 1 FROM writeback_recycled WHERE item_id = ?1)
                     OR EXISTS (SELECT 1 FROM writeback_attempts WHERE remote_id IS NULL AND parent_id = ?2 AND name = ?3)
                     OR EXISTS (SELECT 1 FROM writeback_temporary_names WHERE item_id = ?1 AND name = ?3)",
                params![item.item_id, item.parent_id, item.name],
                |row| row.get::<_, bool>(0),
            )?;
            if !suppressed && item.name.is_some() {
                upsert_remote_item(transaction, item, revision, position, affected)?;
            }
        }
    }
    Ok(())
}

fn upsert_remote_item(
    transaction: &Transaction<'_>,
    mut item: RemoteItem,
    revision: i64,
    position: usize,
    affected: &mut AffectedNames,
) -> rusqlite::Result<()> {
    let inode = reserved_inode(&item);
    let deferred_parent = defer_cyclic_remote_parent(transaction, &mut item)?;
    transaction.prepare_cached(
        "DELETE FROM deferred_remote_parents WHERE inode = (SELECT inode FROM items WHERE item_id = ?1)",
    )?.execute([&item.item_id])?;
    let updated = transaction
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
         WHERE items.sync_state IN (0, 3)
           AND items.inode NOT IN (SELECT inode FROM writeback_queue)",
        )?
        .execute(params![
            inode,
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
    if updated != 0 {
        if let Some(name) = item.name {
            affected.insert(item.parent_id, name);
        }
        if let Some(parent) = deferred_parent {
            transaction
                .prepare_cached(
                    "INSERT INTO deferred_remote_parents
                 SELECT inode, ?2 FROM items WHERE item_id = ?1",
                )?
                .execute(params![item.item_id, parent])?;
        }
    }
    Ok(())
}

fn defer_cyclic_remote_parent(
    db: &Connection,
    item: &mut RemoteItem,
) -> rusqlite::Result<Option<String>> {
    if !parent_would_cycle(db, &item.item_id, item.parent_id.as_deref())? {
        return Ok(None);
    }
    let previous_parent = db
        .query_row(
            "SELECT parent_id FROM items WHERE item_id = ?1",
            [&item.item_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    // Keep the previous edge so a queued descendant keeps its parent chain.
    // An item without a usable previous edge can attach directly to the root.
    let fallback = if previous_parent.is_some()
        && !parent_would_cycle(db, &item.item_id, previous_parent.as_deref())?
    {
        previous_parent
    } else {
        db.query_row("SELECT item_id FROM items WHERE inode = 1", [], |row| {
            row.get(0)
        })
        .optional()?
    };
    Ok(std::mem::replace(&mut item.parent_id, fallback))
}

fn parent_would_cycle(
    db: &Connection,
    item_id: &str,
    parent: Option<&str>,
) -> rusqlite::Result<bool> {
    db.prepare_cached(
        "WITH RECURSIVE ancestors(item_id) AS (
             SELECT ?2 WHERE ?2 IS NOT NULL
             UNION
             SELECT items.parent_id FROM items JOIN ancestors USING (item_id)
             WHERE items.parent_id IS NOT NULL AND ancestors.item_id <> ?1
         )
         SELECT EXISTS (SELECT 1 FROM ancestors WHERE item_id = ?1)",
    )?
    .query_row(params![item_id, parent], |row| row.get(0))
}

fn retry_deferred_parents(
    db: &Transaction<'_>,
    affected: &mut AffectedNames,
) -> rusqlite::Result<()> {
    let mut select = db.prepare_cached(
        "SELECT items.inode, items.item_id, deferred.parent_id, items.name
         FROM deferred_remote_parents AS deferred JOIN items USING (inode)
         JOIN items AS parent ON parent.item_id = deferred.parent_id
         WHERE items.sync_state IN (0, 3) AND parent.kind = 1 AND parent.remote_missing = 0
           AND NOT EXISTS (SELECT 1 FROM writeback_queue WHERE inode = items.inode)
         ORDER BY items.claim_revision, items.claim_position, items.inode",
    )?;
    // One reparent can release an edge considered earlier in this pass.
    loop {
        let pending = select
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut progressed = false;
        for (inode, item_id, parent, name) in pending {
            if parent_would_cycle(db, &item_id, Some(&parent))? {
                continue;
            }
            affected.record_item(db, &item_id)?;
            db.execute(
                "UPDATE items SET parent_id = ?2, alias = NULL WHERE inode = ?1",
                params![inode, parent],
            )?;
            db.execute(
                "DELETE FROM deferred_remote_parents WHERE inode = ?1",
                [inode],
            )?;
            affected.insert(Some(parent), name);
            progressed = true;
        }
        if !progressed {
            return Ok(());
        }
    }
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

fn write_pin(db: &Connection, item_id: &str, pin: PinState) -> rusqlite::Result<bool> {
    let changed = match pin {
        PinState::None => db.execute("DELETE FROM pin_directives WHERE item_id = ?1", [item_id])?,
        _ => db.execute(
            "INSERT INTO pin_directives (item_id, pin_state)
             SELECT ?1, ?2 WHERE EXISTS (SELECT 1 FROM items WHERE item_id = ?1)
             ON CONFLICT(item_id) DO UPDATE SET pin_state = excluded.pin_state",
            params![item_id, pin as i64],
        )?,
    };
    Ok(changed != 0)
}
