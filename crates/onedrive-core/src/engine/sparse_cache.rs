use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::Mutex as AsyncMutex;

use super::{CatalogItem, Engine, EngineError, ItemKind};

pub(super) const HEADER_BYTES: u64 = 64 * 1024;
pub(super) const STREAM_BYTES: u64 = 2 * 1024 * 1024;
const HEADER_BLOCK: i64 = -1;
#[derive(Default)]
struct RangeLocks(HashMap<(i64, u64), Weak<AsyncMutex<()>>>);

pub(super) struct SparseCache {
    root: PathBuf,
    locks: Mutex<RangeLocks>,
}

impl SparseCache {
    pub(super) fn open(root: &Path, db: &Connection) -> Result<Self, EngineError> {
        fs::create_dir_all(root)?;
        let cache = Self {
            root: root.into(),
            locks: Mutex::default(),
        };
        cache.reclaim(db)?;
        Ok(cache)
    }

    fn path(&self, generation: i64) -> PathBuf {
        self.root.join(format!("{generation}.sparse"))
    }

    pub(super) fn reclaim(&self, db: &Connection) -> Result<(), EngineError> {
        let mut select = db.prepare_cached("SELECT generation FROM sparse_retired")?;
        let retired = select.query_map([], |row| row.get(0))?;
        for generation in retired {
            let generation = generation?;
            if let Err(error) = fs::remove_file(self.path(generation))
                && error.kind() != io::ErrorKind::NotFound
            {
                return Err(error.into());
            }
            db.execute(
                "DELETE FROM sparse_retired WHERE generation = ?1",
                [generation],
            )?;
        }
        Ok(())
    }

    fn lock(&self, generation: i64, stream_offset: u64) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        let key = (generation, stream_offset);
        if let Some(lock) = locks.0.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        locks.0.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(AsyncMutex::new(()));
        locks.0.insert(key, Arc::downgrade(&lock));
        lock
    }
}

impl Engine {
    pub fn cache_capacity(&self) -> Result<rustix::fs::StatVfs, EngineError> {
        rustix::fs::statvfs(&self.cache.root).map_err(|err| io::Error::from(err).into())
    }

    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, EngineError> {
        loop {
            let (item, generation) = {
                let mut db = self.db();
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let item = super::item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
                if item.kind == ItemKind::Folder {
                    return Err(EngineError::IsDirectory);
                }
                if offset >= item.size || length == 0 {
                    return Ok(Vec::new());
                }
                let generation = allocate_generation(&tx, inode)?;
                tx.commit()?;
                (item, generation)
            };

            let end = offset.saturating_add(length as u64).min(item.size);
            let mut result = vec![0; (end - offset) as usize];
            let header_sniff = offset == 0 && length as u64 <= HEADER_BYTES;
            if let Some(downloaded) = self
                .read_generation(&item, generation, offset, header_sniff, &mut result, false)
                .await?
            {
                if downloaded {
                    self.evict()?;
                }
                return Ok(result);
            }
        }
    }

    pub(super) async fn read_generation(
        &self,
        item: &CatalogItem,
        generation: i64,
        start: u64,
        header_sniff: bool,
        output: &mut [u8],
        pin_hydration: bool,
    ) -> Result<Option<bool>, EngineError> {
        let mut downloaded = false;
        let end = start + output.len() as u64;
        let mut offset = start;
        while offset < end {
            let stream_offset = offset / STREAM_BYTES * STREAM_BYTES;
            // The two requests that can cover byte zero share a lock.
            let lock = self.cache.lock(generation, stream_offset);
            let _guard = lock.lock().await;
            {
                let db = self.db();
                if !generation_is_current(&db, item, generation, pin_hydration)? {
                    return Ok(None);
                }
                let cached_end = if block_cached(&db, generation, stream_offset as i64)? {
                    Some(stream_offset + STREAM_BYTES)
                } else if offset < HEADER_BYTES && block_cached(&db, generation, HEADER_BLOCK)? {
                    Some(HEADER_BYTES)
                } else {
                    None
                };
                if let Some(cached_end) = cached_end {
                    let until = end.min(cached_end);
                    let slice = &mut output[(offset - start) as usize..(until - start) as usize];
                    File::open(self.cache.path(generation))?.read_exact_at(slice, offset)?;
                    offset = until;
                    continue;
                }
            }

            let (block, fetch_offset, length) = range_for_read(stream_offset, header_sniff);
            let expected = length.min(item.size - fetch_offset) as usize;
            if pin_hydration {
                self.check_hydration_space(expected as u64)?;
            }
            let bytes = self
                .cloud
                .read_range(&item.item_id, fetch_offset, length as usize)
                .await;
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if !generation_is_current(&tx, item, generation, pin_hydration)? {
                return Ok(None);
            }
            // Another Engine may have filled the range while the request was in flight.
            if block_cached(&tx, generation, block)? {
                continue;
            }
            let bytes = bytes?;
            if pin_hydration {
                self.check_hydration_space(expected as u64)?;
            }
            if bytes.len() != expected {
                return Err(EngineError::InvalidRange);
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(self.cache.path(generation))?;
            file.write_all_at(&bytes, fetch_offset)?;
            file.sync_data()?;
            File::open(&self.cache.root)?.sync_all()?;
            tx.execute(
                "INSERT OR IGNORE INTO sparse_ranges VALUES (?1, ?2)",
                params![generation, block],
            )?;
            self.touch_generation(&tx, generation)?;
            tx.commit()?;
            downloaded = true;
        }
        let db = self.db();
        Ok(self
            .touch_generation(&db, generation)?
            .then_some(downloaded))
    }

    fn touch_generation(&self, db: &Connection, generation: i64) -> rusqlite::Result<bool> {
        Ok(db.execute(
            "UPDATE items SET last_accessed_at = max(coalesce(last_accessed_at, 0), ?2)
             WHERE inode = (SELECT inode FROM sparse_files WHERE generation = ?1)",
            params![generation, self.access_time()],
        )? != 0)
    }
}

fn range_for_read(stream_offset: u64, header_sniff: bool) -> (i64, u64, u64) {
    if header_sniff {
        (HEADER_BLOCK, 0, HEADER_BYTES)
    } else {
        (stream_offset as i64, stream_offset, STREAM_BYTES)
    }
}

pub(super) fn generation_for(db: &Connection, inode: u64) -> rusqlite::Result<Option<i64>> {
    db.query_row(
        "SELECT generation FROM sparse_files WHERE inode = ?1",
        [inode],
        |row| row.get(0),
    )
    .optional()
}

fn block_cached(db: &Connection, generation: i64, block: i64) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS (SELECT 1 FROM sparse_ranges WHERE generation = ?1 AND block = ?2)",
        params![generation, block],
        |row| row.get(0),
    )
}

// Uses the current item's row as i. A generation with no chunks is complete
// only for an empty file that has explicitly been hydrated.
pub(super) const HAS_COMPLETE_CONTENT: &str = "EXISTS (
    SELECT 1 FROM sparse_files f WHERE f.inode = i.inode AND (
        (i.size <= ?2 AND EXISTS (
            SELECT 1 FROM sparse_ranges WHERE generation = f.generation AND block = -1
        ))
        OR (SELECT count(*) FROM sparse_ranges WHERE generation = f.generation AND block >= 0)
            = (i.size / ?3) + (i.size % ?3 <> 0)
    )
)";

pub(super) fn is_hydrated(db: &Connection, item: &CatalogItem) -> rusqlite::Result<bool> {
    db.prepare_cached(&format!(
        "SELECT {HAS_COMPLETE_CONTENT} FROM items i WHERE inode = ?1 AND kind = 0",
    ))?
    .query_row(params![item.inode, HEADER_BYTES, STREAM_BYTES], |row| {
        row.get(0)
    })
}

pub(super) fn allocate_generation(db: &Connection, inode: u64) -> rusqlite::Result<i64> {
    db.execute(
        "INSERT INTO sparse_files (inode) VALUES (?1) ON CONFLICT(inode) DO NOTHING",
        [inode],
    )?;
    Ok(generation_for(db, inode)?.expect("cache generation was allocated"))
}

fn generation_is_current(
    db: &Connection,
    item: &CatalogItem,
    generation: i64,
    pin_hydration: bool,
) -> rusqlite::Result<bool> {
    if generation_for(db, item.inode)? != Some(generation) {
        return Ok(false);
    }
    if !pin_hydration {
        return Ok(true);
    }
    let clean = db
        .query_row(
            "SELECT sync_state = 0 AND remote_missing = 0 FROM items WHERE inode = ?1",
            [item.inode],
            |row| row.get::<_, bool>(0),
        )
        .optional()?
        .unwrap_or(false);
    Ok(clean && super::retention::effective_pin(db, &item.item_id)? == super::PinState::Pinned)
}
