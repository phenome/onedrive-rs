use std::fs::{self, DirBuilder, Permissions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_REQUEST_BYTES: u64 = 64 * 1024;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);

pub fn socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("onedrive-rs/daemon.sock")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Pin { path: MountPath },
    Unpin { path: MountPath },
    Purge { path: MountPath },
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
        write_frame(&mut stream, request)?;
        let mut line = Vec::new();
        BufReader::new(stream).read_until(b'\n', &mut line)?;
        let reply = serde_json::from_slice(require_newline(&line)?)?;
        Response::from_json(reply)
    }
}

pub struct IpcServer {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl IpcServer {
    pub fn bind(path: &Path) -> io::Result<Self> {
        let parent = path
            .parent()
            .filter(|parent| path.is_absolute() && *parent != Path::new("/"))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "socket needs an absolute path in a private directory",
                )
            })?;
        match DirBuilder::new().mode(0o700).create(parent) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let directory = fs::symlink_metadata(parent)?;
        if !directory.is_dir()
            || directory.uid() != rustix::process::geteuid().as_raw()
            || directory.mode() & 0o777 != 0o700
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "socket directory must be owned by the effective user and have mode 0700",
            ));
        }

        // The private directory protects the socket while its permissions are set.
        let listener = UnixListener::bind(path)?;
        let metadata = fs::symlink_metadata(path)?;
        let server = Self {
            listener,
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        fs::set_permissions(path, Permissions::from_mode(0o600))?;
        Ok(server)
    }

    pub fn set_nonblocking(&self, enabled: bool) -> io::Result<()> {
        self.listener.set_nonblocking(enabled)
    }

    /// Accept and handle one connection. In nonblocking mode, an empty accept
    /// queue returns WouldBlock. Accepted streams still use blocking I/O.
    pub fn serve_one(
        &self,
        dispatch: impl FnOnce(Request) -> Result<Value, String>,
    ) -> io::Result<()> {
        let (mut stream, _) = self.listener.accept()?;
        stream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
        stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
        let response = match read_request(&stream) {
            Ok(request) => match dispatch(request) {
                Ok(result) => Response::Success(result),
                Err(error) => Response::Error(error),
            },
            Err(error) => Response::Error(format!("invalid request: {error}")),
        };
        write_frame(&mut stream, &response.into_json())
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn read_request(stream: &UnixStream) -> io::Result<Request> {
    let mut frame = Vec::new();
    BufReader::new(stream)
        .take(MAX_REQUEST_BYTES)
        .read_until(b'\n', &mut frame)?;
    Ok(serde_json::from_slice(require_newline(&frame)?)?)
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
    let mut frame = serde_json::to_vec(message)?;
    frame.push(b'\n');
    stream.write_all(&frame)
}
