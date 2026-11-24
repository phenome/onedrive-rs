use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    BackgroundSession, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, LockOwner, MountOption, OpenAccMode, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyXattr, Request,
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
            MountOption::RO,
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
        ];
        config.n_threads = Some(4);
        fuser::spawn_mount(self, mountpoint, &config)
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
        flags: OpenFlags,
        _kill_suid_gid: bool,
        reply: ReplyOpen,
    ) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            return reply.error(Errno::EROFS);
        }
        match self.item(ino) {
            Ok(item) if item.kind == ItemKind::Folder => reply.error(Errno::EISDIR),
            // Preserve the application's read sizes and let the Engine own content freshness.
            Ok(_) => reply.opened(FileHandle(0), FopenFlags::FOPEN_DIRECT_IO),
            Err(error) => reply.error(error),
        }
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
        atime: item.last_accessed_at.map_or(mtime, unix_time),
        mtime,
        ctime: mtime,
        crtime: UNIX_EPOCH,
        kind: file_type(item.kind),
        perm: if directory { 0o555 } else { 0o444 },
        nlink: if directory { 2 } else { 1 },
        uid: rustix::process::geteuid().as_raw(),
        gid: rustix::process::getegid().as_raw(),
        rdev: 0,
        blksize: 4096,
        flags: 0,
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

fn errno(error: EngineError) -> Errno {
    match error {
        EngineError::NotFound => Errno::ENOENT,
        EngineError::IsDirectory => Errno::EISDIR,
        EngineError::CacheIo(error) => {
            Errno::from_i32(error.raw_os_error().unwrap_or(Errno::EIO.code()))
        }
        _ => Errno::EIO,
    }
}
