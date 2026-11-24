use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenFlags, Owner, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use tokio::runtime::Handle;

use crate::{CatalogItem, Engine, EngineError, ItemKind};

type DirectoryEntry = (INodeNo, FileType, String);

pub struct FuseAdapter {
    engine: Arc<Engine>,
    runtime: Handle,
    directories: Mutex<HashMap<u64, Arc<[DirectoryEntry]>>>,
    next_directory: AtomicU64,
}

impl FuseAdapter {
    /// The caller keeps the Tokio runtime running for the lifetime of the mount.
    pub fn new(engine: Arc<Engine>, runtime: Handle) -> Self {
        Self {
            engine,
            runtime,
            directories: Mutex::default(),
            next_directory: AtomicU64::new(1),
        }
    }

    pub fn mount(self, mountpoint: impl AsRef<Path>) -> io::Result<BackgroundSession> {
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
        let engine = Arc::downgrade(&self.engine);
        let runtime = self.runtime.clone();
        let session = fuser::spawn_mount(self, mountpoint, &config)?;
        runtime.spawn(Engine::run_upload_worker(engine));
        Ok(session)
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
        let entries = self
            .engine
            .list_children(&directory.item_id)
            .map_err(errno)?;
        Ok([
            (ino, FileType::Directory, ".".into()),
            (INodeNo(parent), FileType::Directory, "..".into()),
        ]
        .into_iter()
        .chain(
            entries
                .into_iter()
                .map(|entry| (INodeNo(entry.inode), file_type(entry.kind), entry.name)),
        ))
    }
}

impl Filesystem for FuseAdapter {
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
        let entries = match self.directory_entries(ino) {
            Ok(entries) => entries.collect(),
            Err(error) => return reply.error(error),
        };
        // Directory offsets stay stable across paginated reads and concurrent delta updates.
        let handle = self.next_directory.fetch_add(1, Ordering::Relaxed);
        self.directories.lock().unwrap().insert(handle, entries);
        reply.opened(FileHandle(handle), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = self.directories.lock().unwrap().get(&fh.0).cloned();
        let Some(entries) = entries else {
            return reply.error(Errno::EBADF);
        };
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
            Ok(()) => reply.opened(FileHandle(ino.0), FopenFlags::FOPEN_DIRECT_IO),
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
                .create_with_mode(
                    parent.0,
                    file_name(name)?,
                    ItemKind::File,
                    Some(permissions(mode & !umask)),
                )
                .map_err(errno)?;
            self.engine.open_file(item.inode).map_err(errno)?;
            Ok::<_, Errno>(item)
        })();
        match result {
            Ok(item) => reply.created(
                &Duration::ZERO,
                &attributes(&item),
                Generation(0),
                FileHandle(item.inode),
                FopenFlags::FOPEN_DIRECT_IO,
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
        match self
            .runtime
            .block_on(self.engine.write(ino.0, offset, data, append))
        {
            Ok(()) => reply.written(data.len() as u32),
            Err(error) => reply.error(errno(error)),
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
                Some(size) => self
                    .runtime
                    .block_on(self.engine.truncate(ino.0, size))
                    .map_err(errno)?,
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
        match self
            .runtime
            .block_on(self.engine.read(ino.0, offset, size as usize))
        {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(errno(error)),
        }
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
        atime: item
            .local_atime
            .or(item.last_accessed_at)
            .map_or(mtime, unix_time),
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
        EngineError::NotDirectory => Errno::ENOTDIR,
        EngineError::NotEmpty => Errno::ENOTEMPTY,
        EngineError::AlreadyExists => Errno::EEXIST,
        EngineError::InvalidMutation => Errno::EINVAL,
        EngineError::TrashProhibited => Errno::EACCES,
        EngineError::CacheIo(error) => {
            Errno::from_i32(error.raw_os_error().unwrap_or(Errno::EIO.code()))
        }
        _ => Errno::EIO,
    }
}
