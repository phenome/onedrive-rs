use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, KernelConfig, LockOwner, MountOption, OpenFlags, Owner,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, Session, TimeOrNow, WriteFlags,
};
use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::engine::{CatalogInvalidation, CatalogObserver};
use crate::{CatalogItem, CloudError, Engine, EngineError, ItemKind};

type DirectoryEntry = (INodeNo, FileType, String);
type DirectorySnapshot = Arc<Mutex<Option<Vec<DirectoryEntry>>>>;

struct MountPaths {
    mountpoint: PathBuf,
    thumbnail_root: PathBuf,
}

pub struct FuseAdapter {
    engine: Arc<Engine>,
    runtime: Handle,
    shutdown: Option<watch::Receiver<()>>,
    directories: Mutex<HashMap<u64, DirectorySnapshot>>,
    paths: Option<MountPaths>,
    next_directory: AtomicU64,
    observer: Arc<OnceLock<CatalogObserver>>,
}

impl FuseAdapter {
    /// The caller keeps the Tokio runtime running for the lifetime of the mount.
    pub fn new(engine: Arc<Engine>, runtime: Handle) -> Self {
        Self {
            engine,
            runtime,
            shutdown: None,
            directories: Mutex::default(),
            paths: None,
            next_directory: AtomicU64::new(1),
            observer: Arc::default(),
        }
    }

    /// Closing the sender releases FUSE callbacks waiting on asynchronous work.
    pub fn with_shutdown(mut self, shutdown: watch::Receiver<()>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    fn run<T>(&self, work: impl Future<Output = Result<T, EngineError>>) -> Result<T, Errno> {
        self.runtime.block_on(async {
            if let Some(mut shutdown) = self.shutdown.clone() {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => Err(Errno::EINTR),
                    result = work => result.map_err(errno),
                }
            } else {
                work.await.map_err(errno)
            }
        })
    }

    pub fn mount(mut self, mountpoint: impl AsRef<Path>) -> io::Result<BackgroundSession> {
        let mountpoint = mountpoint.as_ref().canonicalize()?;
        self.paths = Some(MountPaths {
            mountpoint: mountpoint.clone(),
            thumbnail_root: crate::engine::default_thumbnail_root()?,
        });
        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName("onedrive-rs".into()),
            MountOption::Subtype("onedrive-rs".into()),
            MountOption::RW,
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
        ];
        config.n_threads = Some(4);
        let engine = self.engine.clone();
        let owner = self.observer.clone();
        let runtime = self.runtime.clone();
        let session = Session::new(self, mountpoint, &config)?;
        let notifier = session.notifier();
        let (send, mut changes) =
            tokio::sync::mpsc::unbounded_channel::<Vec<CatalogInvalidation>>();
        runtime.spawn_blocking(move || {
            while let Some(changes) = changes.blocking_recv() {
                for change in &changes {
                    let result = match change {
                        CatalogInvalidation::Inode { inode, content } => {
                            notifier.inval_inode(INodeNo(*inode), if *content { 0 } else { -1 }, 0)
                        }
                        CatalogInvalidation::Entry { parent, name } => {
                            notifier.inval_entry(INodeNo(*parent), OsStr::new(name))
                        }
                    };
                    if let Err(error) = result
                        && error.kind() != io::ErrorKind::NotFound
                        && error.raw_os_error() != Some(rustix::io::Errno::NODEV.raw_os_error())
                    {
                        log::warn!("FUSE cache invalidation failed: {error}");
                    }
                }
            }
        });
        let observer: CatalogObserver = Arc::new(move |changes| {
            let _ = send.send(changes.to_vec());
        });
        engine.observe_catalog(&observer);
        assert!(owner.set(observer).is_ok());
        session.spawn()
    }

    fn item(&self, ino: INodeNo) -> Result<CatalogItem, Errno> {
        self.engine
            .getattr(ino.0)
            .map_err(errno)?
            .ok_or(Errno::ENOENT)
    }

    fn directory(&self, ino: INodeNo) -> Result<CatalogItem, Errno> {
        let item = self.item(ino)?;
        if item.kind != ItemKind::Folder {
            return Err(Errno::ENOTDIR);
        }
        Ok(item)
    }

    fn directory_entries(
        &self,
        ino: INodeNo,
    ) -> Result<impl Iterator<Item = DirectoryEntry>, Errno> {
        let directory = self.directory(ino)?;
        let parent = match directory.parent_id {
            Some(id) => {
                self.engine
                    .item(&id)
                    .map_err(errno)?
                    .ok_or(Errno::ENOENT)?
                    .inode
            }
            None => ino.0,
        };
        let entries = match &self.paths {
            Some(paths) => self
                .run(self.engine.readdir(
                    &directory.item_id,
                    &paths.mountpoint,
                    &paths.thumbnail_root,
                ))
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| (entry.inode, entry.kind, entry.name))
                        .collect()
                })?,
            None => self
                .engine
                .list_directory_entries(&directory.item_id)
                .map_err(errno)?,
        };
        Ok([
            (ino, FileType::Directory, ".".into()),
            (INodeNo(parent), FileType::Directory, "..".into()),
        ]
        .into_iter()
        .chain(
            entries
                .into_iter()
                .map(|(inode, kind, name)| (INodeNo(inode), file_type(kind), name)),
        ))
    }
}

impl Filesystem for FuseAdapter {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        // fuser requires a positive byte limit. One byte rounds down to zero
        // pages on Linux, keeping cached mmap and MIME sniffing demand-driven.
        let _ = config.set_max_readahead(1);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let item = (|| {
            let name = name.to_str().ok_or(Errno::ENOENT)?;
            let parent = self.directory(parent)?;
            self.engine
                .lookup(&parent.item_id, name)
                .map_err(errno)?
                .ok_or(Errno::ENOENT)
        })();
        match item {
            Ok(item) => reply.entry(&Duration::ZERO, &attributes(&item), Generation(0)),
            Err(error) => reply.error(error),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.item(ino) {
            Ok(item) => reply.attr(&Duration::ZERO, &attributes(&item)),
            Err(error) => reply.error(error),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        if let Err(error) = self.directory(ino) {
            return reply.error(error);
        }
        let handle = self.next_directory.fetch_add(1, Ordering::Relaxed);
        self.directories
            .lock()
            .unwrap()
            .insert(handle, Arc::default());
        reply.opened(FileHandle(handle), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = self.directories.lock().unwrap().get(&fh.0).cloned();
        let Some(entries) = entries else {
            return reply.error(Errno::EBADF);
        };
        // Seed on the first read and preserve directory offsets across pages.
        let mut snapshot = entries.lock().unwrap();
        if snapshot.is_none() {
            *snapshot = match self.directory_entries(ino) {
                Ok(entries) => Some(entries.collect()),
                Err(error) => return reply.error(error),
            };
        }
        let entries = snapshot.as_ref().unwrap();
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        for (index, (inode, kind, name)) in entries.iter().enumerate().skip(offset) {
            if reply.add(*inode, (index + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.directories.lock().unwrap().remove(&fh.0);
        reply.ok();
    }

    fn open(
        &self,
        _req: &Request,
        ino: INodeNo,
        _flags: OpenFlags,
        _kill_suid_gid: bool,
        reply: ReplyOpen,
    ) {
        match self.engine.open_file(ino.0) {
            Ok(()) => reply.opened(FileHandle(ino.0), FopenFlags::FOPEN_KEEP_CACHE),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        _kill_suid_gid: bool,
        _owner: Owner,
        reply: ReplyCreate,
    ) {
        let result = (|| {
            let item = self
                .engine
                .create_and_open(parent.0, file_name(name)?, Some(permissions(mode & !umask)))
                .map_err(errno)?;
            Ok::<_, Errno>(item)
        })();
        match result {
            Ok(item) => reply.created(
                &Duration::ZERO,
                &attributes(&item),
                Generation(0),
                FileHandle(item.inode),
                FopenFlags::FOPEN_KEEP_CACHE,
            ),
            Err(error) => reply.error(error),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _owner: Owner,
        reply: ReplyEntry,
    ) {
        match file_name(name).and_then(|name| {
            self.engine
                .create_with_mode(
                    parent.0,
                    name,
                    ItemKind::Folder,
                    Some(permissions(mode & !umask)),
                )
                .map_err(errno)
        }) {
            Ok(item) => reply.entry(&Duration::ZERO, &attributes(&item), Generation(0)),
            Err(error) => reply.error(error),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let append = flags.0 & rustix::fs::OFlags::APPEND.bits() as i32 != 0;
        match self.run(self.engine.write(ino.0, offset, data, append)) {
            Ok(()) => reply.written(data.len() as u32),
            Err(error) => reply.error(error),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        _kill_suid_gid: bool,
        reply: ReplyAttr,
    ) {
        if uid.is_some_and(|uid| uid != rustix::process::geteuid().as_raw())
            || gid.is_some_and(|gid| gid != rustix::process::getegid().as_raw())
        {
            return reply.error(Errno::EPERM);
        }
        let result = (|| {
            let item = match size {
                Some(size) => self.run(self.engine.truncate(ino.0, size))?,
                None => self.item(ino)?,
            };
            if mode.is_some() || atime.is_some() || mtime.is_some() {
                return self
                    .engine
                    .set_local_attributes(
                        ino.0,
                        mode.map(permissions),
                        atime.map(set_time),
                        mtime.map(set_time),
                    )
                    .map_err(errno);
            }
            Ok::<_, Errno>(item)
        })();
        match result {
            Ok(item) => reply.attr(&Duration::ZERO, &attributes(&item)),
            Err(error) => reply.error(error),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        send_empty(
            reply,
            file_name(name).and_then(|name| {
                self.engine
                    .unlink(parent.0, name, ItemKind::File)
                    .map_err(errno)
            }),
        );
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        send_empty(
            reply,
            file_name(name).and_then(|name| {
                self.engine
                    .unlink(parent.0, name, ItemKind::Folder)
                    .map_err(errno)
            }),
        );
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        flags: RenameFlags,
        _owner: Option<Owner>,
        reply: ReplyEmpty,
    ) {
        let result = (|| {
            if !flags.difference(RenameFlags::RENAME_NOREPLACE).is_empty() {
                return Err(Errno::EINVAL);
            }
            self.engine
                .rename(
                    parent.0,
                    file_name(name)?,
                    new_parent.0,
                    file_name(new_name)?,
                    flags.contains(RenameFlags::RENAME_NOREPLACE),
                )
                .map_err(errno)
        })();
        send_empty(reply, result);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Engine acknowledges writes only after staging and catalog persistence.
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.engine.release_file(ino.0);
        reply.ok();
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.run(self.engine.read(ino.0, offset, size as usize)) {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(error),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Namespace mutations commit synchronously to the catalog.
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        match self.engine.cache_capacity() {
            Ok(space) => reply.statfs(
                space.f_blocks,
                space.f_bfree,
                space.f_bavail,
                space.f_files,
                space.f_ffree,
                space.f_bsize as u32,
                space.f_namemax as u32,
                space.f_frsize as u32,
            ),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let Some(name) = name.to_str() else {
            return reply.error(Errno::NO_XATTR);
        };
        match self.engine.xattr(ino.0, name) {
            Ok(Some(value)) => send_xattr(reply, size, &value),
            Ok(None) => reply.error(Errno::NO_XATTR),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.engine.list_xattrs(ino.0) {
            Ok(names) => {
                let mut packed = Vec::new();
                for name in names {
                    packed.extend_from_slice(name.as_bytes());
                    packed.push(0);
                }
                send_xattr(reply, size, &packed);
            }
            Err(error) => reply.error(errno(error)),
        }
    }
}

fn attributes(item: &CatalogItem) -> FileAttr {
    let mtime = unix_time(item.mtime);
    let directory = item.kind == ItemKind::Folder;
    FileAttr {
        ino: INodeNo(item.inode),
        size: item.size,
        blocks: item.size.div_ceil(512),
        atime: item.local_atime.map_or_else(
            || {
                item.last_accessed_at.map_or(mtime, |milliseconds| {
                    UNIX_EPOCH + Duration::from_millis(milliseconds.max(0) as u64)
                })
            },
            unix_time,
        ),
        mtime: item.local_mtime.map_or(mtime, unix_time),
        ctime: mtime,
        crtime: UNIX_EPOCH,
        kind: file_type(item.kind),
        perm: item.mode.unwrap_or(if directory { 0o755 } else { 0o644 }),
        nlink: if item.unlinked {
            0
        } else if directory {
            2
        } else {
            1
        },
        uid: rustix::process::geteuid().as_raw(),
        gid: rustix::process::getegid().as_raw(),
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn permissions(mode: u32) -> u16 {
    (mode & 0o777) as u16
}

fn set_time(time: TimeOrNow) -> i64 {
    let time = match time {
        TimeOrNow::SpecificTime(time) => time,
        TimeOrNow::Now => SystemTime::now(),
    };
    match time.duration_since(UNIX_EPOCH) {
        Ok(delta) => i64::try_from(delta.as_secs()).unwrap_or(i64::MAX),
        Err(delta) => -i64::try_from(delta.duration().as_secs()).unwrap_or(i64::MAX),
    }
}

fn unix_time(seconds: i64) -> SystemTime {
    let delta = Duration::from_secs(seconds.unsigned_abs());
    if seconds < 0 {
        UNIX_EPOCH - delta
    } else {
        UNIX_EPOCH + delta
    }
}

fn file_type(kind: ItemKind) -> FileType {
    match kind {
        ItemKind::File => FileType::RegularFile,
        ItemKind::Folder => FileType::Directory,
    }
}

fn send_xattr(reply: ReplyXattr, size: u32, bytes: &[u8]) {
    if size == 0 {
        reply.size(bytes.len() as u32);
    } else if (size as usize) < bytes.len() {
        reply.error(Errno::ERANGE);
    } else {
        reply.data(bytes);
    }
}

fn file_name(name: &OsStr) -> Result<&str, Errno> {
    name.to_str().ok_or(Errno::EINVAL)
}

fn send_empty(reply: ReplyEmpty, result: Result<(), Errno>) {
    match result {
        Ok(()) => reply.ok(),
        Err(error) => reply.error(error),
    }
}

fn errno(error: EngineError) -> Errno {
    match error {
        EngineError::NotFound => Errno::ENOENT,
        EngineError::IsDirectory => Errno::EISDIR,
        EngineError::CacheIo(error) => {
            Errno::from_i32(error.raw_os_error().unwrap_or(Errno::EIO.code()))
        }
        EngineError::NotDirectory => Errno::ENOTDIR,
        EngineError::NotEmpty => Errno::ENOTEMPTY,
        EngineError::AlreadyExists => Errno::EEXIST,
        EngineError::InvalidMutation => Errno::EINVAL,
        EngineError::TrashProhibited => Errno::EACCES,
        EngineError::Cloud(error) => cloud_errno(error),
        _ => Errno::EIO,
    }
}

fn cloud_errno(error: CloudError) -> Errno {
    match error {
        CloudError::NotFound => Errno::ENOENT,
        CloudError::Conflict => Errno::ESTALE,
        CloudError::Authentication => Errno::EACCES,
        CloudError::Unsupported => Errno::EOPNOTSUPP,
        CloudError::Throttled { .. } => Errno::EAGAIN,
        CloudError::InvalidResponse | CloudError::ResyncRequired | CloudError::Unavailable => {
            Errno::EIO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_all_cloud_errors_to_fuse_errno() {
        for (error, expected) in [
            (CloudError::NotFound, Errno::ENOENT),
            (CloudError::Conflict, Errno::ESTALE),
            (CloudError::Authentication, Errno::EACCES),
            (CloudError::Unsupported, Errno::EOPNOTSUPP),
            (
                CloudError::Throttled {
                    retry_after: Duration::from_secs(5),
                },
                Errno::EAGAIN,
            ),
            (CloudError::InvalidResponse, Errno::EIO),
            (CloudError::ResyncRequired, Errno::EIO),
            (CloudError::Unavailable, Errno::EIO),
        ] {
            assert_eq!(errno(EngineError::Cloud(error)), expected);
        }
    }
}
