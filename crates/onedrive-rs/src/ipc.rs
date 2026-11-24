use std::ffi::OsString;
use std::fs::{self, File, Permissions};
use std::io::{self, BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rustix::fs::{
    AtFlags, CWD, FlockOperation, Mode, OFlags, ResolveFlags, flock, mkdirat, openat, openat2,
    unlinkat,
};
use rustix::io::Errno;
use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType, connect, socket_with};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener as AsyncListener, UnixStream as AsyncStream};
use tokio::time::timeout;

const MAX_REQUEST_BYTES: u64 = 64 * 1024;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);
const LONG_JOB_TIMEOUT: Duration = Duration::from_secs(300);

pub fn socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("onedrive-rs/daemon.sock")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status {},
    Pin { path: MountPath },
    Unpin { path: MountPath },
    Purge { path: MountPath },
    Sync {},
    Uploads {},
    Pause {},
    Resume {},
}

impl Request {
    pub(crate) fn is_long_running(&self) -> bool {
        matches!(self, Self::Pin { .. } | Self::Sync {})
    }
}

/// A canonical UTF-8 path in the OneDrive namespace. "/" is the drive root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct MountPath(String);

impl MountPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MountPath {
    type Error = &'static str;

    fn try_from(path: String) -> Result<Self, Self::Error> {
        if path == "/"
            || path.strip_prefix('/').is_some_and(|relative| {
                !relative.contains('\0')
                    && relative
                        .split('/')
                        .all(|part| !matches!(part, "" | "." | ".."))
            })
        {
            return Ok(Self(path));
        }
        Err("expected a canonical path starting at the mount root")
    }
}

/// Result payload for a successful status request.
#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub sync_state: String,
    pub unpinned_cache_bytes: u64,
    pub pinned_cache_bytes: u64,
    pub free_disk_floor_warning: bool,
    pub pending_upload_count: u64,
    pub paused: bool,
}

/// An entry in the result array for a successful uploads request.
#[derive(Debug, Serialize, Deserialize)]
pub struct Upload {
    pub path: MountPath,
    pub sync_state: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Success(Value),
    Error(String),
}

impl Response {
    pub fn into_json(self) -> Value {
        match self {
            Self::Success(result) => json!({"ok": true, "result": result}),
            Self::Error(error) => json!({"ok": false, "error": error}),
        }
    }

    fn from_json(mut envelope: Value) -> io::Result<Self> {
        if let Some(fields) = envelope.as_object_mut()
            && fields.len() == 2
        {
            match fields.remove("ok") {
                Some(Value::Bool(true)) => {
                    if let Some(result) = fields.remove("result") {
                        return Ok(Self::Success(result));
                    }
                }
                Some(Value::Bool(false)) => {
                    if let Some(Value::String(error)) = fields.remove("error") {
                        return Ok(Self::Error(error));
                    }
                }
                _ => {}
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid response envelope",
        ))
    }
}

pub struct IpcClient {
    path: PathBuf,
}

impl IpcClient {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn request(&self, request: &Request) -> io::Result<Response> {
        let mut stream = UnixStream::connect(&self.path)?;
        stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
        let response_timeout = if request.is_long_running() {
            LONG_JOB_TIMEOUT + SOCKET_TIMEOUT
        } else {
            SOCKET_TIMEOUT
        };
        stream.set_read_timeout(Some(response_timeout))?;
        write_frame(&mut stream, request)?;
        let mut line = Vec::new();
        BufReader::new(stream).read_until(b'\n', &mut line)?;
        let reply = serde_json::from_slice(require_newline(&line)?)?;
        Response::from_json(reply)
    }
}

pub struct IpcServer {
    listener: UnixListener,
    directory: OwnedFd,
    name: OsString,
    socket: File,
}

impl IpcServer {
    pub fn bind(path: &Path) -> io::Result<Self> {
        SocketAddrUnix::new(path)?;
        let (runtime, parent, name) = path
            .parent()
            .and_then(|parent| Some((parent.parent()?, parent.file_name()?, path.file_name()?)))
            .filter(|(runtime, _, _)| path.is_absolute() && *runtime != Path::new("/"))
            .filter(|_| {
                !path
                    .components()
                    .any(|part| part == std::path::Component::ParentDir)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "socket needs a private runtime directory",
                )
            })?;

        // Resolve the runtime path without symlinks, then anchor all socket
        // operations to the opened directory even if a path above it moves.
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        let runtime = File::from(openat2(
            CWD,
            runtime,
            flags,
            Mode::empty(),
            ResolveFlags::NO_SYMLINKS,
        )?);
        validate_private_directory(&runtime)?;
        match mkdirat(&runtime, parent, Mode::RWXU) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let directory = File::from(openat(&runtime, parent, flags, Mode::empty())?);
        validate_private_directory(&directory)?;
        match flock(&directory, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(Errno::WOULDBLOCK) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another server owns the socket directory",
                ));
            }
            Err(error) => return Err(error.into()),
        }

        // AF_UNIX has no bindat/connectat. The procfs path pins the parent
        // directory for both operations without changing the process cwd.
        let anchored = fd_path(directory.as_raw_fd()).join(name);
        match open_socket_entry(&directory, name) {
            Ok(entry) => {
                let metadata = entry.metadata()?;
                if !metadata.file_type().is_socket()
                    || metadata.uid() != rustix::process::geteuid().as_raw()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "socket path is occupied",
                    ));
                }
                let probe = socket_with(
                    AddressFamily::UNIX,
                    SocketType::STREAM,
                    SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
                    None,
                )?;
                let address = SocketAddrUnix::new(&anchored)?;
                if connect(&probe, &address) != Err(Errno::CONNREFUSED) {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "socket may still be in use",
                    ));
                }
                if !same_entry(&directory, name, &entry)? {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "socket path changed during startup",
                    ));
                }
                unlinkat(&directory, name, AtFlags::empty())?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let listener = UnixListener::bind(&anchored)?;
        let socket = open_socket_entry(&directory, name)?;
        if !socket.metadata()?.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "socket path changed during bind",
            ));
        }
        let server = Self {
            listener,
            directory: directory.into(),
            name: name.to_owned(),
            socket,
        };
        fs::set_permissions(
            fd_path(server.socket.as_raw_fd()),
            Permissions::from_mode(0o600),
        )?;
        Ok(server)
    }

    pub fn async_listener(&self) -> io::Result<AsyncListener> {
        self.listener.set_nonblocking(true)?;
        AsyncListener::from_std(self.listener.try_clone()?)
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        if same_entry(&self.directory, &self.name, &self.socket).unwrap_or(false) {
            let _ = unlinkat(&self.directory, &self.name, AtFlags::empty());
        }
    }
}

pub async fn serve_connection(
    mut stream: AsyncStream,
    dispatch: impl AsyncFnOnce(Request) -> Result<Value, String>,
) -> io::Result<()> {
    let mut frame = Vec::new();
    let read = timeout(
        SOCKET_TIMEOUT,
        tokio::io::BufReader::new(&mut stream)
            .take(MAX_REQUEST_BYTES)
            .read_until(b'\n', &mut frame),
    )
    .await;
    let request = match read {
        Ok(Ok(_)) => decode_request(&frame),
        Ok(Err(error)) => Err(error),
        Err(error) => return Err(error.into()),
    };
    let response = match request {
        Ok(request) => {
            let deadline = if request.is_long_running() {
                LONG_JOB_TIMEOUT
            } else {
                SOCKET_TIMEOUT
            };
            match timeout(deadline, dispatch(request)).await {
                Ok(Ok(result)) => Response::Success(result),
                Ok(Err(error)) => Response::Error(error),
                Err(_) => Response::Error("OneDrive request timed out".into()),
            }
        }
        Err(error) => Response::Error(format!("invalid request: {error}")),
    };
    timeout(
        SOCKET_TIMEOUT,
        stream.write_all(&encode_frame(&response.into_json())?),
    )
    .await?
}

fn validate_private_directory(file: &File) -> io::Result<()> {
    let directory = file.metadata()?;
    if directory.uid() == rustix::process::geteuid().as_raw() && directory.mode() & 0o777 == 0o700 {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "runtime and socket directories must be owned by the effective user and have mode 0700",
    ))
}

fn fd_path(fd: std::os::fd::RawFd) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{fd}"))
}

fn open_socket_entry(
    directory: impl std::os::fd::AsFd,
    name: &std::ffi::OsStr,
) -> io::Result<File> {
    Ok(File::from(openat(
        directory,
        name,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?))
}

fn same_entry(
    directory: impl std::os::fd::AsFd,
    name: &std::ffi::OsStr,
    entry: &File,
) -> io::Result<bool> {
    let expected = entry.metadata()?;
    let found = open_socket_entry(directory, name)?.metadata()?;
    Ok(found.file_type().is_socket()
        && found.dev() == expected.dev()
        && found.ino() == expected.ino())
}

fn decode_request(frame: &[u8]) -> io::Result<Request> {
    Ok(serde_json::from_slice(require_newline(frame)?)?)
}

fn require_newline(frame: &[u8]) -> io::Result<&[u8]> {
    frame.strip_suffix(b"\n").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "missing newline or frame too large",
        )
    })
}

fn write_frame(stream: &mut UnixStream, message: &impl Serialize) -> io::Result<()> {
    stream.write_all(&encode_frame(message)?)
}

fn encode_frame(message: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut frame = serde_json::to_vec(message)?;
    frame.push(b'\n');
    Ok(frame)
}
