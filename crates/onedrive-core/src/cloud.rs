use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use thiserror::Error;

/// The Graph adapter translates HTTP responses into this port's domain types.
#[async_trait]
pub trait CloudDrive: Send + Sync {
    /// Read up to length bytes, stopping at remote EOF.
    async fn read_range(
        &self,
        _item_id: &str,
        _offset: u64,
        _length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        Err(CloudError::Unavailable)
    }

    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError>;
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
