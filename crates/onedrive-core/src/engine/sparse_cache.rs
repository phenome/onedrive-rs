use std::collections::HashMap;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::UNIX_EPOCH;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use rustix::fs::{FlockOperation, Mode, OFlags, flock};
use tokio::sync::Mutex as AsyncMutex;

use super::{CatalogItem, Engine, EngineError, ItemKind};

pub(super) const HEADER_BYTES: u64 = 64 * 1024;
pub(super) const STREAM_BYTES: u64 = 2 * 1024 * 1024;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadKind {
    Header,
    Stream,
    Pin,
}

#[derive(PartialEq, Eq)]
pub(super) enum ReadOutcome {
    Retired,
    Cached,
    Downloaded,
}

#[derive(Clone, Copy)]
enum Block {
    Header,
    Stream(u64),
}

impl Block {
    fn key(self) -> i64 {
        match self {
            Self::Header => -1,
            Self::Stream(offset) => offset as i64,
        }
    }

    fn offset(self) -> u64 {
        match self {
            Self::Header => 0,
            Self::Stream(offset) => offset,
        }
    }

    fn capacity(self) -> u64 {
        match self {
            Self::Header => HEADER_BYTES,
            Self::Stream(_) => STREAM_BYTES,
        }
    }

    fn from_key(key: i64) -> Self {
        if key == Self::Header.key() {
            Self::Header
        } else {
            Self::Stream(key as u64)
        }
    }
}

#[derive(Default)]
struct RangeLocks(HashMap<(i64, u64), Weak<AsyncMutex<()>>>);

pub(super) struct SparseCache {
    root: PathBuf,
    locks: Mutex<RangeLocks>,
    #[cfg(test)]
    before_flush: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl SparseCache {
    pub(super) fn open(root: &Path, db: &Connection) -> Result<Self, EngineError> {
        let namespace: String =
            db.query_row("SELECT name FROM sparse_namespace", [], |row| row.get(0))?;
        let root = root.join(namespace);

        let cache = Self {
            root,
            locks: Mutex::default(),
            #[cfg(test)]
            before_flush: Mutex::default(),
        };
        let disk = cache.lock_disk()?;
        let retired = retired_generations(db)?;
        for generation in retired {
            if cache.remove_generation(generation) {
                disk.sync_all()?;
                forget_retired(db, generation)?;
            }
        }
        let mut select = db.prepare("SELECT generation, block FROM sparse_pending")?;
        let pending = select
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (generation, block) in pending {
            cache.remove_pending(generation, Block::from_key(block))?;
            forget_pending(db, generation, Block::from_key(block))?;
        }
        Ok(cache)
    }

    fn ensure_root(&self) -> io::Result<()> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.root)
    }

    fn path(&self, generation: i64) -> PathBuf {
        self.root.join(format!("{generation}.sparse"))
    }

    fn block_path(&self, generation: i64, block: Block) -> PathBuf {
        self.path(generation).join(block.key().to_string())
    }

    fn staging_path(&self, generation: i64, block: Block) -> PathBuf {
        self.path(generation)
            .join(format!("{}.pending", block.key()))
    }

    fn remove_pending(&self, generation: i64, block: Block) -> io::Result<()> {
        let mut removed = false;
        for path in [
            self.staging_path(generation, block),
            self.block_path(generation, block),
        ] {
            match fs::remove_file(path) {
                Ok(()) => removed = true,
                Err(err) if err.kind() == io::ErrorKind::NotFound => (),
                Err(err) => return Err(err),
            }
        }
        if removed {
            File::open(self.path(generation))?.sync_all()?;
        }
        Ok(())
    }

    // Independent engines opening the same catalog coordinate publication and
    // reclamation. Always acquire this lock before consulting the catalog.
    pub(super) fn lock_disk(&self) -> io::Result<File> {
        self.ensure_root()?;
        let directory = File::open(&self.root)?;
        flock(&directory, FlockOperation::LockExclusive)?;
        Ok(directory)
    }

    fn remove_generation(&self, generation: i64) -> bool {
        let removed = fs::remove_dir_all(self.path(generation));
        removed.is_ok() || removed.is_err_and(|err| err.kind() == io::ErrorKind::NotFound)
    }

    pub(super) fn retain_upload(
        &self,
        directory: &File,
        db: &Connection,
        inode: u64,
        size: u64,
        source: &File,
    ) -> Result<(), EngineError> {
        db.execute("DELETE FROM sparse_files WHERE inode = ?1", [inode])?;
        db.execute("INSERT INTO sparse_files (inode) VALUES (?1)", [inode])?;
        let generation = db.last_insert_rowid();
        // A rolled-back transaction can leave an unpublished directory.
        if !self.remove_generation(generation) {
            return Err(io::Error::other("could not clear unpublished cache generation").into());
        }
        // Keep a warm prefix without expanding a large sparse stage on disk
        // or holding the catalog lock across a full-file copy.
        for offset in (0..size.min(4 * STREAM_BYTES)).step_by(STREAM_BYTES as usize) {
            let block = Block::Stream(offset);
            let mut bytes = vec![0; (size - offset).min(STREAM_BYTES) as usize];
            source.read_exact_at(&mut bytes, offset)?;
            let stamp = self.write_block(directory, generation, block, &bytes)?;
            db.execute(
                "INSERT INTO sparse_ranges VALUES (?1, ?2)",
                params![generation, block.key()],
            )?;
            db.execute(
                "INSERT INTO sparse_range_files VALUES (?1, ?2, ?3)",
                params![generation, block.key(), stamp],
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

    fn open_block(&self, generation: i64, block: Block, stamp: &[u8]) -> io::Result<Option<File>> {
        let path = self.block_path(generation, block);
        // An unexpected FIFO must not block an Engine request.
        let fd = match rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP) => {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        let file = File::from(fd);
        Ok(matches_stamp(&file, stamp)?.then_some(file))
    }

    fn write_block(
        &self,
        directory: &File,
        generation: i64,
        block: Block,
        bytes: &[u8],
    ) -> io::Result<Vec<u8>> {
        let path = self.path(generation);
        fs::create_dir_all(&path)?;
        let staging = self.staging_path(generation, block);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        file.write_all(bytes)?;
        // Any later write or truncate changes this timestamp even on
        // filesystems whose clock cannot distinguish successive writes.
        file.set_times(FileTimes::new().set_modified(UNIX_EPOCH))?;
        #[cfg(test)]
        if let Some(pause) = self.before_flush.lock().unwrap().take() {
            pause();
        }
        file.sync_all()?;
        fs::rename(&staging, self.block_path(generation, block))?;
        File::open(path)?.sync_all()?;
        directory.sync_all()?;
        let metadata = file.metadata()?;
        if metadata.size() != bytes.len() as u64 || metadata.modified()? != UNIX_EPOCH {
            return Err(io::Error::other(
                "unpublished range changed during installation",
            ));
        }
        Ok(file_stamp(&metadata))
    }
}

impl Engine {
    pub fn cache_capacity(&self) -> Result<rustix::fs::StatVfs, EngineError> {
        self.cache.ensure_root()?;
        rustix::fs::statvfs(&self.cache.root).map_err(|err| io::Error::from(err).into())
    }

    pub(super) fn reclaim_sparse_cache(&self) -> Result<(), EngineError> {
        let retired = retired_generations(&self.db())?;
        if retired.is_empty() {
            return Ok(());
        }
        let disk = self.cache.lock_disk()?;
        for generation in retired {
            if self.cache.remove_generation(generation) {
                disk.sync_all()?;
                forget_retired(&self.db(), generation)?;
            }
        }
        Ok(())
    }

    fn retire_sparse_generation(&self, generation: i64) -> Result<(), EngineError> {
        self.db().execute(
            "DELETE FROM sparse_files WHERE generation = ?1",
            [generation],
        )?;
        self.reclaim_sparse_cache()
    }

    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, EngineError> {
        loop {
            let (item, stage, generation) = {
                let mut db = self.db();
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let item = super::item_by_inode(&tx, inode)?.ok_or(EngineError::NotFound)?;
                if item.kind == ItemKind::Folder {
                    return Err(EngineError::IsDirectory);
                }
                if offset >= item.size || length == 0 {
                    return Ok(Vec::new());
                }
                let stage = self.open_stage(&tx, inode)?;
                let generation = if stage.is_none() {
                    Some(allocate_generation(&tx, inode)?)
                } else {
                    None
                };
                tx.commit()?;
                (item, stage, generation)
            };

            let end = offset.saturating_add(length as u64).min(item.size);
            let mut result = vec![0; (end - offset) as usize];
            if let Some(stage) = stage {
                stage.read(self, offset, &mut result).await?;
                return Ok(result);
            }
            let generation = generation.expect("clean read has a cache generation");
            let kind = if offset == 0 && length as u64 <= HEADER_BYTES {
                ReadKind::Header
            } else {
                ReadKind::Stream
            };
            match self
                .read_generation(&item, generation, offset, &mut result, kind)
                .await
            {
                Ok(ReadOutcome::Retired) => continue,
                Ok(ReadOutcome::Cached) => return Ok(result),
                Ok(ReadOutcome::Downloaded) => {
                    self.evict()?;
                    return Ok(result);
                }
                Err(error) => {
                    // Earlier blocks may have been cached before the read failed.
                    let _ = self.evict();
                    return Err(error);
                }
            }
        }
    }

    pub(super) fn read_verified_cache(
        &self,
        generation: i64,
        etag: Option<&str>,
        ctag: Option<&str>,
        offset: u64,
        output: &mut [u8],
    ) -> Result<usize, EngineError> {
        let stream = Block::Stream(offset / STREAM_BYTES * STREAM_BYTES);
        let db = self.db();
        for block in [stream, Block::Header] {
            if offset >= block.offset() + block.capacity() {
                continue;
            }
            let verified = db.query_row(
                "SELECT EXISTS (SELECT 1 FROM sparse_range_versions
                    WHERE generation = ?1 AND block = ?2
                    AND ((?4 IS NOT NULL AND ctag IS ?4)
                         OR (?4 IS NULL AND ?3 IS NOT NULL AND etag IS ?3)))",
                params![generation, block.key(), etag, ctag],
                |row| row.get::<_, bool>(0),
            )?;
            if verified
                && let Some(stamp) = block_stamp(&db, generation, block)?
                && let Some(file) = self.cache.open_block(generation, block, &stamp)?
            {
                let start = offset - block.offset();
                let length =
                    (file.metadata()?.len().saturating_sub(start) as usize).min(output.len());
                if length > 0
                    && file.read_exact_at(&mut output[..length], start).is_ok()
                    && matches_stamp(&file, &stamp)?
                {
                    return Ok(length);
                }
            }
        }
        Ok(0)
    }

    pub(super) async fn read_generation(
        &self,
        item: &CatalogItem,
        generation: i64,
        start: u64,
        output: &mut [u8],
        kind: ReadKind,
    ) -> Result<ReadOutcome, EngineError> {
        let mut downloaded = false;
        let end = start + output.len() as u64;
        let mut offset = start;
        while offset < end {
            let stream_offset = offset / STREAM_BYTES * STREAM_BYTES;
            // The two requests that can cover byte zero share a lock.
            let lock = self.cache.lock(generation, stream_offset);
            let _guard = lock.lock().await;
            let stream = Block::Stream(stream_offset);
            let cached = {
                let db = self.db();
                if !generation_is_current(&db, item, generation, kind)? {
                    return Ok(ReadOutcome::Retired);
                }
                let mut cached = block_stamp(&db, generation, stream)?.map(|stamp| (stream, stamp));
                if cached.is_none() && offset < HEADER_BYTES {
                    cached = block_stamp(&db, generation, Block::Header)?
                        .map(|stamp| (Block::Header, stamp));
                }
                cached
            };
            if let Some((block, stamp)) = cached {
                let until = end.min(block.offset() + block.capacity());
                let slice = &mut output[(offset - start) as usize..(until - start) as usize];
                if let Some(file) = self.cache.open_block(generation, block, &stamp)? {
                    match file.read_exact_at(slice, offset - block.offset()) {
                        Ok(()) if matches_stamp(&file, &stamp)? => {
                            offset = until;
                            continue;
                        }
                        Ok(()) => (),
                        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => (),
                        Err(err) => return Err(err.into()),
                    }
                }
                self.retire_sparse_generation(generation)?;
                return Ok(ReadOutcome::Retired);
            }

            let block = if kind == ReadKind::Header
                || (kind == ReadKind::Pin && item.size <= HEADER_BYTES)
            {
                Block::Header
            } else {
                stream
            };
            if kind == ReadKind::Pin {
                self.check_hydration_space(block.capacity().min(item.size - block.offset()))?;
            }
            let offset = block.offset();
            let length = block.capacity() as usize;
            let versioned = if item.etag.is_some() || item.ctag.is_some() {
                self.cloud
                    .read_range_at_version(
                        &item.item_id,
                        item.etag.as_deref(),
                        item.ctag.as_deref(),
                        offset,
                        length,
                    )
                    .await
            } else {
                Err(crate::CloudError::Unsupported)
            };
            let bytes = match versioned {
                Err(crate::CloudError::Unsupported) => self
                    .cloud
                    .read_range(&item.item_id, offset, length)
                    .await
                    .map(|bytes| (bytes, false)),
                result => result.map(|bytes| (bytes, true)),
            };
            match self.publish_block(item, generation, block, kind, bytes)? {
                ReadOutcome::Retired => return Ok(ReadOutcome::Retired),
                ReadOutcome::Downloaded => downloaded = true,
                ReadOutcome::Cached => (),
            }
        }
        let db = self.db();
        if !generation_is_current(&db, item, generation, kind)?
            || !self.touch_generation(&db, generation)?
        {
            return Ok(ReadOutcome::Retired);
        }
        Ok(if downloaded {
            ReadOutcome::Downloaded
        } else {
            ReadOutcome::Cached
        })
    }

    fn publish_block(
        &self,
        item: &CatalogItem,
        generation: i64,
        block: Block,
        kind: ReadKind,
        bytes: Result<(Vec<u8>, bool), crate::CloudError>,
    ) -> Result<ReadOutcome, EngineError> {
        let directory = self.cache.lock_disk()?;
        let (bytes, verified) = {
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if !generation_is_current(&tx, item, generation, kind)? {
                return Ok(ReadOutcome::Retired);
            }
            if block_stamp(&tx, generation, block)?.is_some() {
                return Ok(ReadOutcome::Cached);
            }
            let (bytes, verified) = bytes?;
            let expected = block.capacity().min(item.size - block.offset()) as usize;
            if kind == ReadKind::Pin {
                self.check_hydration_space(expected as u64)?;
            }
            if bytes.len() != expected {
                return Err(EngineError::InvalidRange);
            }
            // Record cleanup before creating anything on disk. A crash between
            // file installation and SQLite publication leaves this entry.
            tx.execute(
                "INSERT OR IGNORE INTO sparse_pending VALUES (?1, ?2)",
                params![generation, block.key()],
            )?;
            tx.commit()?;
            (bytes, verified)
        };
        let result = (|| {
            self.cache.remove_pending(generation, block)?;
            let stamp = self
                .cache
                .write_block(&directory, generation, block, &bytes)?;
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if !generation_is_current(&tx, item, generation, kind)? {
                return Ok(ReadOutcome::Retired);
            }
            tx.execute(
                "INSERT INTO sparse_ranges VALUES (?1, ?2)",
                params![generation, block.key()],
            )?;
            tx.execute(
                "INSERT INTO sparse_range_files VALUES (?1, ?2, ?3)",
                params![generation, block.key(), stamp],
            )?;
            if verified {
                tx.execute(
                    "INSERT INTO sparse_range_versions VALUES (?1, ?2, ?3, ?4)",
                    params![generation, block.key(), item.etag, item.ctag],
                )?;
            }
            self.touch_generation(&tx, generation)?;
            forget_pending(&tx, generation, block)?;
            tx.commit()?;
            Ok(ReadOutcome::Downloaded)
        })();
        if !matches!(result, Ok(ReadOutcome::Downloaded))
            && self.cache.remove_pending(generation, block).is_ok()
        {
            forget_pending(&self.db(), generation, block)?;
        }
        result
    }

    /// Status must remain available when damaged blocks cannot be opened.
    pub(super) fn invalid_cache_inodes(
        &self,
    ) -> Result<std::collections::HashSet<u64>, EngineError> {
        let blocks = {
            let db = self.db();
            db.prepare_cached(
                "SELECT f.inode, f.generation, r.block, r.stamp
                FROM sparse_files f JOIN sparse_range_files r USING (generation)",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, i64>(1)?,
                    Block::from_key(row.get(2)?),
                    row.get::<_, Vec<u8>>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut invalid = std::collections::HashSet::new();
        for (inode, generation, block, stamp) in blocks {
            if !invalid.contains(&inode) {
                let intact = match fs::symlink_metadata(self.cache.block_path(generation, block)) {
                    Ok(metadata) => metadata.is_file() && file_stamp(&metadata) == stamp,
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                        ) =>
                    {
                        false
                    }
                    Err(err) => return Err(err.into()),
                };
                if !intact {
                    invalid.insert(inode);
                }
            }
        }
        Ok(invalid)
    }

    pub(super) fn recover_cached_generations(&self) -> Result<(), EngineError> {
        let generations = {
            let db = self.db();
            db.prepare("SELECT inode, generation FROM sparse_files")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (inode, generation) in generations {
            self.validate_cached_generation(inode, generation)?;
        }
        Ok(())
    }

    pub(super) fn validate_cached_generation(
        &self,
        inode: u64,
        generation: i64,
    ) -> Result<bool, EngineError> {
        let ranges = {
            let db = self.db();
            let mut select = db.prepare_cached(
                "SELECT block, stamp FROM sparse_range_files WHERE generation = ?1",
            )?;
            select
                .query_map([generation], |row| {
                    Ok((Block::from_key(row.get(0)?), row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (block, stamp) in ranges {
            if self.cache.open_block(generation, block, &stamp)?.is_none() {
                self.retire_sparse_generation(generation)?;
                return Ok(false);
            }
        }
        Ok(generation_for(&self.db(), inode)? == Some(generation))
    }

    pub(super) fn touch_generation(
        &self,
        db: &Connection,
        generation: i64,
    ) -> rusqlite::Result<bool> {
        Ok(db.execute(
            "UPDATE items SET last_accessed_at = max(coalesce(last_accessed_at, 0), ?2)
             WHERE inode = (SELECT inode FROM sparse_files WHERE generation = ?1)",
            params![generation, self.access_time()],
        )? != 0)
    }
}

fn file_stamp(metadata: &fs::Metadata) -> Vec<u8> {
    [
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ]
    .into_iter()
    .flat_map(u64::to_le_bytes)
    .collect()
}

fn matches_stamp(file: &File, stamp: &[u8]) -> io::Result<bool> {
    let metadata = file.metadata()?;
    Ok(metadata.is_file() && file_stamp(&metadata) == stamp)
}

pub(super) fn generation_for(db: &Connection, inode: u64) -> rusqlite::Result<Option<i64>> {
    db.query_row(
        "SELECT generation FROM sparse_files WHERE inode = ?1",
        [inode],
        |row| row.get(0),
    )
    .optional()
}

fn block_stamp(
    db: &Connection,
    generation: i64,
    block: Block,
) -> rusqlite::Result<Option<Vec<u8>>> {
    db.query_row(
        "SELECT stamp FROM sparse_range_files WHERE generation = ?1 AND block = ?2",
        params![generation, block.key()],
        |row| row.get(0),
    )
    .optional()
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
    kind: ReadKind,
) -> rusqlite::Result<bool> {
    if generation_for(db, item.inode)? != Some(generation) {
        return Ok(false);
    }
    if kind != ReadKind::Pin {
        return db.query_row(
            "SELECT NOT EXISTS (SELECT 1 FROM writeback_queue WHERE inode = ?1 AND stage IS NOT NULL)",
            [item.inode],
            |row| row.get(0),
        );
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

fn forget_pending(db: &Connection, generation: i64, block: Block) -> rusqlite::Result<()> {
    db.execute(
        "DELETE FROM sparse_pending WHERE generation = ?1 AND block = ?2",
        params![generation, block.key()],
    )?;
    Ok(())
}

fn retired_generations(db: &Connection) -> rusqlite::Result<Vec<i64>> {
    db.prepare_cached("SELECT generation FROM sparse_retired")?
        .query_map([], |row| row.get(0))?
        .collect()
}

fn forget_retired(db: &Connection, generation: i64) -> rusqlite::Result<()> {
    db.execute(
        "DELETE FROM sparse_retired WHERE generation = ?1",
        [generation],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::{FakeCloudDrive, RemoteItem, SystemClock};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pin_rechecks_writeback_state_after_flushing() {
        let temp = tempfile::tempdir_in("/tmp").unwrap();
        let drive = Arc::new(FakeCloudDrive::new(100));
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
        drive.upsert(root.clone());
        drive.upsert(RemoteItem {
            item_id: "file".into(),
            parent_id: Some("root".into()),
            name: Some("file".into()),
            kind: ItemKind::File,
            size: 4096,
            ..root
        });
        drive.set_content("file", vec![13; 4096]);
        let engine = Arc::new(
            Engine::open_with_config(
                temp.path().join("db"),
                temp.path().join("cache"),
                drive,
                Arc::new(SystemClock),
                crate::Config {
                    min_free_disk_bytes: 0,
                    ..crate::Config::default()
                },
            )
            .unwrap(),
        );
        engine.sync().await.unwrap();

        let weak = Arc::downgrade(&engine);
        *engine.cache.before_flush.lock().unwrap() = Some(Box::new(move || {
            weak.upgrade()
                .unwrap()
                .set_sync_state("file", crate::SyncState::Dirty)
                .unwrap();
        }));

        engine.pin("file").await.unwrap();
        let item = engine.item("file").unwrap().unwrap();
        assert_eq!(item.sync_state, crate::SyncState::Dirty);
        assert_eq!(item.last_accessed_at, None);
        assert_eq!(engine.cache_status().unwrap().pinned_bytes, 0);
        let db = engine.db();
        for table in ["sparse_ranges", "sparse_pending"] {
            let rows: i64 = db
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 0, "{table}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn directory_reads_and_delta_commit_while_a_block_flush_is_suspended() {
        let timeout = Duration::from_secs(5);
        let temp = tempfile::tempdir_in("/tmp").unwrap();
        let drive = Arc::new(FakeCloudDrive::new(100));
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
        drive.upsert(root.clone());
        let mut remote = RemoteItem {
            item_id: "file".into(),
            parent_id: Some("root".into()),
            name: Some("file".into()),
            kind: ItemKind::File,
            size: 4096,
            ctag: Some("old".into()),
            ..root
        };
        drive.upsert(remote.clone());
        drive.set_content("file", vec![13; 4096]);
        let engine = Arc::new(
            Engine::open(temp.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap(),
        );
        engine.sync().await.unwrap();
        let inode = engine.item("file").unwrap().unwrap().inode;

        let (flushing, flushing_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        *engine.cache.before_flush.lock().unwrap() = Some(Box::new(move || {
            flushing.send(()).unwrap();
            release_rx.recv_timeout(timeout * 2).unwrap();
        }));
        let reader = tokio::spawn({
            let engine = engine.clone();
            async move { engine.read(inode, 0, 4096).await.unwrap() }
        });
        flushing_rx.recv_timeout(timeout).unwrap();

        remote.ctag = Some("new".into());
        drive.upsert(remote);
        drive.set_content("file", vec![79; 4096]);
        let sync = tokio::spawn({
            let engine = engine.clone();
            async move { engine.sync().await.unwrap() }
        });

        let (listed, listed_rx) = mpsc::channel();
        let probe = thread::spawn({
            let engine = engine.clone();
            move || {
                let start = Instant::now();
                while start.elapsed() < timeout {
                    let entries = engine.list_children("root").unwrap();
                    if entries[0].ctag.as_deref() == Some("new") {
                        let hit = engine.lookup("root", "file").unwrap().unwrap();
                        listed.send(hit.inode).unwrap();
                        return;
                    }
                    thread::yield_now();
                }
            }
        });

        let listed = listed_rx.recv_timeout(timeout);
        release.send(()).unwrap();
        probe.join().unwrap();
        assert_eq!(listed.unwrap(), inode);
        sync.await.unwrap();
        assert_eq!(reader.await.unwrap(), vec![79; 4096]);
        assert_eq!(drive.range_requests().len(), 2);
        assert_eq!(fs::read_dir(&engine.cache.root).unwrap().count(), 1);
    }
}
