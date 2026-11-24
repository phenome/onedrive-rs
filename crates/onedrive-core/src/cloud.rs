use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use thiserror::Error;

/// The Graph adapter translates HTTP responses into this port's domain types.
#[async_trait]
pub trait CloudDrive: Send + Sync {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError>;

    /// Read up to `length` authentic file bytes starting at `offset`.
    /// Return an empty vector at EOF and for a zero-length read.
    async fn read_range(
        &self,
        _item_id: &str,
        _offset: u64,
        _length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        Err(CloudError::Unsupported)
    }

    /// Read a range whose source is verified against the supplied version.
    async fn read_range_at_version(
        &self,
        _item_id: &str,
        _etag: Option<&str>,
        _ctag: Option<&str>,
        _offset: u64,
        _length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        Err(CloudError::Unsupported)
    }

    async fn list_thumbnails(&self, _folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        Err(CloudError::Unsupported)
    }

    async fn download_thumbnail(
        &self,
        _thumbnail: &RemoteThumbnail,
    ) -> Result<Vec<u8>, CloudError> {
        Err(CloudError::Unsupported)
    }

    async fn upload(
        &self,
        _target: UploadTarget<'_>,
        _bytes: &[u8],
        _if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        Err(CloudError::Unsupported)
    }

    /// Upload from an immutable seekable source. Legacy adapters can handle a
    /// bounded small upload through their byte-slice implementation.
    async fn upload_from(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        if source.len() > 10 * 1024 * 1024 {
            return Err(CloudError::Unsupported);
        }
        self.upload(target, &source.read(0, source.len()).await?, if_match)
            .await
    }

    /// Report whether the upload returned a mutation response or was confirmed by observation.
    async fn upload_with_outcome(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<UploadOutcome, CloudError> {
        self.upload_from(target, source, if_match)
            .await
            .map(UploadOutcome::Observed)
    }

    /// Resolve the current upload destination. Absence is represented by None.
    async fn upload_destination(
        &self,
        _target: UploadTarget<'_>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        Err(CloudError::Unsupported)
    }

    /// Confirm the destination's identity and stable version against the source.
    /// None means the request can be retried with its original precondition.
    async fn confirm_upload(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        let Some(item) = self.upload_destination(target).await? else {
            return Ok(None);
        };
        if item.etag.is_none() {
            return Err(CloudError::Unavailable);
        }
        if item.etag.as_deref() == if_match {
            return Ok(None);
        }
        let mut matches = item.kind == ItemKind::File && item.size == source.len() as u64;
        if matches {
            const BLOCK: usize = 1024 * 1024;
            for offset in (0..source.len()).step_by(BLOCK) {
                let length = (source.len() - offset).min(BLOCK);
                if self
                    .read_range(&item.item_id, offset as u64, length)
                    .await?
                    != source.read(offset, length).await?
                {
                    matches = false;
                    break;
                }
            }
        }
        let current = self
            .upload_destination(target)
            .await?
            .ok_or(CloudError::Unavailable)?;
        if current.item_id != item.item_id || current.etag != item.etag || current.ctag != item.ctag
        {
            return Err(CloudError::Unavailable);
        }
        if matches {
            Ok(Some(current))
        } else {
            Err(CloudError::Conflict)
        }
    }

    async fn create_folder(&self, _parent_id: &str, _name: &str) -> Result<RemoteItem, CloudError> {
        Err(CloudError::Unsupported)
    }

    async fn move_item(
        &self,
        _item_id: &str,
        _parent_id: &str,
        _name: &str,
    ) -> Result<RemoteItem, CloudError> {
        Err(CloudError::Unsupported)
    }

    /// Move the item only while its eTag matches the acknowledged version.
    async fn move_if_match(
        &self,
        _item_id: &str,
        _parent_id: &str,
        _name: &str,
        _etag: &str,
    ) -> Result<RemoteItem, CloudError> {
        Err(CloudError::Unsupported)
    }

    async fn delete_if_match(&self, _item_id: &str, _etag: &str) -> Result<(), CloudError> {
        Err(CloudError::Unsupported)
    }

    /// Recycle the directory only if its identity and version match and it is empty.
    async fn delete_empty_folder_if_match(
        &self,
        _item_id: &str,
        _etag: &str,
    ) -> Result<(), CloudError> {
        Err(CloudError::Unsupported)
    }
}

/// The large rendition of a child item. Its temporary URL carries access.
#[derive(Clone)]
pub struct RemoteThumbnail {
    pub item_id: String,
    pub url: String,
}

/// A mutation response can advance a queued delete. A destination observation
/// can confirm uploaded bytes but may include a peer's later metadata change.
pub enum UploadOutcome {
    Acknowledged(RemoteItem),
    Observed(RemoteItem),
}

impl UploadOutcome {
    pub fn into_item(self) -> RemoteItem {
        match self {
            Self::Acknowledged(item) | Self::Observed(item) => item,
        }
    }
}

#[derive(Clone, Copy)]
pub enum UploadSource<'a> {
    Bytes(&'a [u8]),
    File { file: &'a File, size: usize },
}

impl UploadSource<'_> {
    pub fn len(self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::File { size, .. } => size,
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub async fn read(self, offset: usize, length: usize) -> Result<Vec<u8>, CloudError> {
        if offset > self.len() || length > self.len() - offset {
            return Err(CloudError::InvalidResponse);
        }
        match self {
            Self::Bytes(bytes) => Ok(bytes[offset..offset + length].to_vec()),
            Self::File { file, .. } => {
                let file = file.try_clone().map_err(|_| CloudError::Unavailable)?;
                tokio::task::spawn_blocking(move || {
                    let mut bytes = vec![0; length];
                    file.read_exact_at(&mut bytes, offset as u64)
                        .map_err(|_| CloudError::Unavailable)?;
                    Ok(bytes)
                })
                .await
                .map_err(|_| CloudError::Unavailable)?
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum UploadTarget<'a> {
    New { parent_id: &'a str, name: &'a str },
    Existing { item_id: &'a str },
}

#[derive(Clone, PartialEq, Eq)]
pub enum DeltaRequest {
    /// Enumerate /me/drive/root/delta with $top=1000.
    Initial,
    /// Follow the complete server-provided URL verbatim.
    Link(String),
}

impl fmt::Debug for DeltaRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Initial => "Initial",
            Self::Link(_) => "Link(<redacted>)",
        })
    }
}

pub struct DeltaPage {
    pub changes: Vec<DeltaChange>,
    pub cursor: DeltaCursor,
}

pub enum DeltaCursor {
    Next(String),
    Complete(String),
}

#[derive(Clone)]
pub enum DeltaChange {
    Upsert(RemoteItem),
    /// Graph tombstones may carry only the immutable ID and deleted facet.
    Delete(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemKind {
    File,
    Folder,
}

/// A drive item normalized by the cloud adapter. The root folder has no parent.
/// Graph's parentReference.path is intentionally absent from the port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteItem {
    pub item_id: String,
    pub parent_id: Option<String>,
    pub name: Option<String>,
    pub kind: ItemKind,
    pub size: u64,
    pub mtime: i64,
    pub etag: Option<String>,
    pub ctag: Option<String>,
    pub quickxor_hash: Option<String>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CloudError {
    #[error("cloud temporarily throttled requests")]
    Throttled { retry_after: Duration },
    #[error("cloud requires a full metadata resync")]
    ResyncRequired,
    #[error("cloud item changed since it was last read")]
    Conflict,
    #[error("cloud item does not exist")]
    NotFound,
    #[error("cloud operation is unsupported")]
    Unsupported,
    #[error("cloud request requires authentication")]
    Authentication,
    #[error("cloud returned an invalid response")]
    InvalidResponse,
    #[error("cloud unavailable")]
    Unavailable,
}

#[async_trait]
pub trait Clock: Send + Sync {
    /// Time since the Unix epoch, used for deadlines that survive restarts.
    fn now(&self) -> Duration;
    async fn sleep(&self, duration: Duration);
}

pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Duration {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
