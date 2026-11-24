use std::collections::HashMap;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::UNIX_EPOCH;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use rustix::fs::{FlockOperation, Mode, OFlags, flock};
use tokio::sync::Mutex as AsyncMutex;

use super::{CatalogItem, Engine, EngineError, ItemKind};

const HEADER_BYTES: u64 = 64 * 1024;
const STREAM_BYTES: u64 = 2 * 1024 * 1024;
const STATE: &str = "user.onedrive.state";
const PIN: &str = "user.onedrive.pin";
const ETAG: &str = "user.onedrive.etag";
const XATTR_NAMES: [&str; 3] = [STATE, PIN, ETAG];

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
        fs::create_dir_all(&root)?;
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
        for offset in (0..size).step_by(STREAM_BYTES as usize) {
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
    pub fn list_xattrs(&self, inode: u64) -> Result<&'static [&'static str], EngineError> {
        self.getattr(inode)?.ok_or(EngineError::NotFound)?;
        Ok(&XATTR_NAMES)
    }

    pub fn xattr(&self, inode: u64, name: &str) -> Result<Option<Vec<u8>>, EngineError> {
        let (item, generation) = {
            let db = self.db();
            let item = super::item_by_inode(&db, inode)?.ok_or(EngineError::NotFound)?;
            let generation = if name == STATE {
                generation_for(&db, inode)?
            } else {
                None
            };
            (item, generation)
        };
        let value = match name {
            STATE => match item.sync_state {
                super::SyncState::Conflict => "Conflict",
                super::SyncState::Dirty | super::SyncState::Uploading => "Dirty",
                super::SyncState::Clean if self.is_hydrated(&item, generation)? => "Hydrated",
                super::SyncState::Clean => "Cloud-only",
            },
            PIN => match item.pin_state {
                super::PinState::None => "none",
                super::PinState::Pinned => "pinned",
                super::PinState::UnpinOverride => "unpin_override",
            },
            ETAG => item.etag.as_deref().unwrap_or_default(),
            _ => return Ok(None),
        };
        Ok(Some(value.as_bytes().to_vec()))
    }

    pub fn cache_capacity(&self) -> Result<rustix::fs::StatVfs, EngineError> {
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
                if let Some(stage) = self.open_stage(&tx, inode)? {
                    let end = offset.saturating_add(length as u64).min(item.size);
                    let mut bytes = vec![0; (end - offset) as usize];
                    stage.read_exact_at(&mut bytes, offset)?;
                    return Ok(bytes);
                }
                tx.execute(
                    "INSERT INTO sparse_files (inode) VALUES (?1) ON CONFLICT(inode) DO NOTHING",
                    [inode],
                )?;
                let generation =
                    generation_for(&tx, inode)?.expect("cache generation was allocated");
                tx.commit()?;
                (item, generation)
            };

            let end = offset.saturating_add(length as u64).min(item.size);
            let mut result = vec![0; (end - offset) as usize];
            let header_sniff = offset == 0 && length as u64 <= HEADER_BYTES;
            if self
                .read_generation(&item, generation, offset, header_sniff, &mut result)
                .await?
            {
                return Ok(result);
            }
        }
    }

    async fn read_generation(
        &self,
        item: &CatalogItem,
        generation: i64,
        start: u64,
        header_sniff: bool,
        output: &mut [u8],
    ) -> Result<bool, EngineError> {
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
                if generation_for(&db, item.inode)? != Some(generation) {
                    return Ok(false);
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
                return Ok(false);
            }

            let block = if header_sniff { Block::Header } else { stream };
            let bytes = self
                .cloud
                .read_range(&item.item_id, block.offset(), block.capacity() as usize)
                .await;
            if !self.publish_block(item, generation, block, bytes)? {
                return Ok(false);
            }
        }
        Ok(generation_for(&self.db(), item.inode)? == Some(generation))
    }

    fn publish_block(
        &self,
        item: &CatalogItem,
        generation: i64,
        block: Block,
        bytes: Result<Vec<u8>, crate::CloudError>,
    ) -> Result<bool, EngineError> {
        let directory = self.cache.lock_disk()?;
        let bytes = {
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if generation_for(&tx, item.inode)? != Some(generation) {
                return Ok(false);
            }
            if block_stamp(&tx, generation, block)?.is_some() {
                return Ok(true);
            }
            let bytes = bytes?;
            let expected = block.capacity().min(item.size - block.offset()) as usize;
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
            bytes
        };
        let result = (|| {
            self.cache.remove_pending(generation, block)?;
            let stamp = self
                .cache
                .write_block(&directory, generation, block, &bytes)?;
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if generation_for(&tx, item.inode)? != Some(generation) {
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO sparse_ranges VALUES (?1, ?2)",
                params![generation, block.key()],
            )?;
            tx.execute(
                "INSERT INTO sparse_range_files VALUES (?1, ?2, ?3)",
                params![generation, block.key(), stamp],
            )?;
            forget_pending(&tx, generation, block)?;
            tx.commit()?;
            Ok(true)
        })();
        if !matches!(result, Ok(true)) && self.cache.remove_pending(generation, block).is_ok() {
            forget_pending(&self.db(), generation, block)?;
        }
        result
    }

    fn is_hydrated(
        &self,
        item: &CatalogItem,
        generation: Option<i64>,
    ) -> Result<bool, EngineError> {
        if item.kind != ItemKind::File {
            return Ok(false);
        }
        if item.size == 0 {
            return Ok(true);
        }
        let Some(generation) = generation else {
            return Ok(false);
        };
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
        let mut streams = 0;
        let mut full_header = false;
        for (block, stamp) in ranges {
            if self.cache.open_block(generation, block, &stamp)?.is_none() {
                self.retire_sparse_generation(generation)?;
                return Ok(false);
            }
            match block {
                Block::Stream(_) => streams += 1,
                Block::Header => full_header = item.size <= HEADER_BYTES,
            }
        }
        Ok((full_header || streams == item.size.div_ceil(STREAM_BYTES))
            && generation_for(&self.db(), item.inode)? == Some(generation))
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

fn generation_for(db: &Connection, inode: u64) -> rusqlite::Result<Option<i64>> {
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
