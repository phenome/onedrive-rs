use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{
    CloudDrive, CloudError, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest, ItemKind,
    RemoteItem, RemoteThumbnail, UploadSource, UploadTarget,
};

/// A versioned in-memory drive. Changes during enumeration are delivered on
/// the next incremental sync, and every generated link can be replayed.
pub struct FakeCloudDrive {
    page_size: NonZeroUsize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    items: BTreeMap<String, RemoteItem>,
    changes: Vec<DeltaChange>,
    links: HashMap<String, Link>,
    next_token: u64,
    failures: VecDeque<(usize, CloudError)>,
    requests: Vec<DeltaRequest>,
    contents: HashMap<String, Content>,
    range_requests: Vec<(String, u64, usize)>,
    thumbnails: HashMap<String, Vec<u8>>,
    thumbnail_list_requests: Vec<String>,
    thumbnail_download_requests: Vec<String>,
    offline: bool,
    next_version: u64,
    recycled: Vec<String>,
}

enum Content {
    Bytes(Vec<u8>),
    File { file: File, size: usize },
}

impl Content {
    fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::File { size, .. } => *size,
        }
    }

    fn range(&self, start: usize, end: usize) -> Result<Vec<u8>, CloudError> {
        match self {
            Self::Bytes(bytes) => Ok(bytes[start..end].to_vec()),
            Self::File { file, .. } => {
                let mut bytes = vec![0; end - start];
                file.read_exact_at(&mut bytes, start as u64)
                    .map_err(|_| CloudError::Unavailable)?;
                Ok(bytes)
            }
        }
    }
}

#[derive(Clone)]
enum Link {
    Page {
        changes: Arc<[DeltaChange]>,
        offset: usize,
        version: usize,
    },
    Delta(usize),
}

impl FakeCloudDrive {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size: NonZeroUsize::new(page_size).expect("page size must be positive"),
            state: Mutex::default(),
        }
    }

    pub fn upsert(&self, item: RemoteItem) {
        let mut state = self.state.lock().unwrap();
        if item.name.is_some() || !state.items.contains_key(&item.item_id) {
            state.items.insert(item.item_id.clone(), item.clone());
        }
        state.changes.push(DeltaChange::Upsert(item));
    }

    pub fn delete(&self, item_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.remove(item_id);
    }

    pub fn set_online(&self, online: bool) {
        self.state.lock().unwrap().offline = !online;
    }

    pub fn recycled_items(&self) -> Vec<String> {
        self.state.lock().unwrap().recycled.clone()
    }

    pub fn expire_tokens(&self) {
        self.state.lock().unwrap().links.clear();
    }

    pub fn fail_after(&self, requests_before_failure: usize, error: CloudError) {
        self.state
            .lock()
            .unwrap()
            .failures
            .push_back((requests_before_failure, error));
    }

    pub fn set_content(&self, item_id: &str, bytes: Vec<u8>) {
        self.state
            .lock()
            .unwrap()
            .contents
            .insert(item_id.into(), Content::Bytes(bytes));
    }

    pub fn range_requests(&self) -> Vec<(String, u64, usize)> {
        self.state.lock().unwrap().range_requests.clone()
    }

    pub fn set_thumbnail(&self, item_id: &str, bytes: Vec<u8>) {
        self.state
            .lock()
            .unwrap()
            .thumbnails
            .insert(item_id.into(), bytes);
    }

    pub fn remove_thumbnail(&self, item_id: &str) {
        self.state.lock().unwrap().thumbnails.remove(item_id);
    }

    pub fn thumbnail_list_requests(&self) -> Vec<String> {
        self.state.lock().unwrap().thumbnail_list_requests.clone()
    }

    pub fn thumbnail_download_requests(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .thumbnail_download_requests
            .clone()
    }

    fn recycle(&self, item_id: &str, etag: &str, empty_folder: bool) -> Result<(), CloudError> {
        let mut state = self.state.lock().unwrap();
        state.online()?;
        let item = state.items.get(item_id).ok_or(CloudError::NotFound)?;
        if item.etag.as_deref() != Some(etag) {
            return Err(CloudError::Conflict);
        }
        if empty_folder
            && (item.kind != ItemKind::Folder
                || state
                    .items
                    .values()
                    .any(|child| child.parent_id.as_deref() == Some(item_id)))
        {
            return Err(CloudError::Conflict);
        }
        state.remove(item_id);
        state.recycled.push(item_id.into());
        Ok(())
    }

    pub fn requests(&self) -> Vec<DeltaRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl State {
    fn remove(&mut self, item_id: &str) {
        let mut removed = vec![item_id.to_owned()];
        while let Some(parent) = removed.pop() {
            self.contents.remove(&parent);
            self.thumbnails.remove(&parent);
            self.items.remove(&parent);
            removed.extend(
                self.items
                    .values()
                    .filter(|item| item.parent_id.as_deref() == Some(&parent))
                    .map(|item| item.item_id.clone()),
            );
        }
        self.changes.push(DeltaChange::Delete(item_id.into()));
    }

    fn online(&self) -> Result<(), CloudError> {
        if self.offline {
            return Err(CloudError::Unavailable);
        }
        Ok(())
    }

    fn require_destination(
        &self,
        parent: &str,
        name: &str,
        except: Option<&str>,
    ) -> Result<(), CloudError> {
        if !self
            .items
            .get(parent)
            .is_some_and(|item| item.kind == ItemKind::Folder)
        {
            return Err(CloudError::NotFound);
        }
        if self.items.values().any(|item| {
            Some(item.item_id.as_str()) != except
                && item.parent_id.as_deref() == Some(parent)
                && item.name.as_deref() == Some(name)
        }) {
            return Err(CloudError::Conflict);
        }
        Ok(())
    }

    fn create(
        &mut self,
        parent: &str,
        name: &str,
        kind: ItemKind,
    ) -> Result<RemoteItem, CloudError> {
        self.require_destination(parent, name, None)?;
        let item_id = loop {
            self.next_version += 1;
            let id = format!("fake-upload-{}", self.next_version);
            if !self.items.contains_key(&id) {
                break id;
            }
        };
        Ok(RemoteItem {
            item_id,
            parent_id: Some(parent.into()),
            name: Some(name.into()),
            kind,
            size: 0,
            mtime: 1_700_000_000,
            etag: None,
            ctag: None,
            quickxor_hash: None,
        })
    }

    fn commit(&mut self, mut item: RemoteItem, bytes: Option<Content>) -> RemoteItem {
        self.next_version += 1;
        let version = self.next_version;
        item.etag = Some(format!("\"fake-{version}\""));
        item.mtime += 1;
        if let Some(bytes) = bytes {
            item.size = bytes.len() as u64;
            item.ctag = Some(format!("fake-content-{version}"));
            self.contents.insert(item.item_id.clone(), bytes);
        }
        self.items.insert(item.item_id.clone(), item.clone());
        self.changes.push(DeltaChange::Upsert(item.clone()));
        item
    }
}

impl FakeCloudDrive {
    fn move_with_condition(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
        etag: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        let mut state = self.state.lock().unwrap();
        state.online()?;
        state.require_destination(parent_id, name, Some(item_id))?;
        let mut item = state
            .items
            .get(item_id)
            .cloned()
            .ok_or(CloudError::NotFound)?;
        if etag.is_some_and(|etag| item.etag.as_deref() != Some(etag)) {
            return Err(CloudError::Conflict);
        }
        item.parent_id = Some(parent_id.into());
        item.name = Some(name.into());
        Ok(state.commit(item, None))
    }
}

#[async_trait]
impl CloudDrive for FakeCloudDrive {
    async fn upload(
        &self,
        target: UploadTarget<'_>,
        bytes: &[u8],
        if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        self.upload_from(target, UploadSource::Bytes(bytes), if_match)
            .await
    }

    async fn upload_from(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        let bytes = match source {
            UploadSource::Bytes(bytes) => Content::Bytes(bytes.to_vec()),
            UploadSource::File { file, size } => Content::File {
                file: file.try_clone().map_err(|_| CloudError::Unavailable)?,
                size,
            },
        };
        let mut state = self.state.lock().unwrap();
        state.online()?;
        let item = match target {
            UploadTarget::New { parent_id, name } => {
                state.create(parent_id, name, ItemKind::File)?
            }
            UploadTarget::Existing { item_id } => {
                let item = state.items.get(item_id).ok_or(CloudError::NotFound)?;
                if if_match.is_some() && if_match != item.etag.as_deref() {
                    return Err(CloudError::Conflict);
                }
                item.clone()
            }
        };
        Ok(state.commit(item, Some(bytes)))
    }

    async fn upload_destination(
        &self,
        target: UploadTarget<'_>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        let state = self.state.lock().unwrap();
        state.online()?;
        Ok(match target {
            UploadTarget::Existing { item_id } => state.items.get(item_id),
            UploadTarget::New { parent_id, name } => state.items.values().find(|item| {
                item.parent_id.as_deref() == Some(parent_id) && item.name.as_deref() == Some(name)
            }),
        }
        .cloned())
    }

    async fn create_folder(&self, parent_id: &str, name: &str) -> Result<RemoteItem, CloudError> {
        let mut state = self.state.lock().unwrap();
        state.online()?;
        let item = state.create(parent_id, name, ItemKind::Folder)?;
        Ok(state.commit(item, None))
    }

    async fn move_item(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
    ) -> Result<RemoteItem, CloudError> {
        self.move_with_condition(item_id, parent_id, name, None)
    }

    async fn move_if_match(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
        etag: &str,
    ) -> Result<RemoteItem, CloudError> {
        self.move_with_condition(item_id, parent_id, name, Some(etag))
    }

    async fn delete_if_match(&self, item_id: &str, etag: &str) -> Result<(), CloudError> {
        self.recycle(item_id, etag, false)
    }

    async fn delete_empty_folder_if_match(
        &self,
        item_id: &str,
        etag: &str,
    ) -> Result<(), CloudError> {
        self.recycle(item_id, etag, true)
    }

    async fn read_range(
        &self,
        item_id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        tokio::task::yield_now().await;
        let mut state = self.state.lock().unwrap();
        state.online()?;
        state.range_requests.push((item_id.into(), offset, length));
        let bytes = state.contents.get(item_id).ok_or(CloudError::Unavailable)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = start.saturating_add(length).min(bytes.len());
        bytes.range(start, end)
    }

    async fn read_range_at_version(
        &self,
        item_id: &str,
        etag: Option<&str>,
        ctag: Option<&str>,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        tokio::task::yield_now().await;
        let mut state = self.state.lock().unwrap();
        state.range_requests.push((item_id.into(), offset, length));
        state.online()?;
        let item = state.items.get(item_id).ok_or(CloudError::NotFound)?;
        if (etag.is_none() && ctag.is_none())
            || etag.is_some_and(|tag| item.etag.as_deref() != Some(tag))
            || ctag.is_some_and(|tag| item.ctag.as_deref() != Some(tag))
        {
            return Err(CloudError::Conflict);
        }
        let bytes = state.contents.get(item_id).ok_or(CloudError::Unavailable)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        bytes.range(start, start.saturating_add(length).min(bytes.len()))
    }

    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        tokio::task::yield_now().await;
        let mut state = self.state.lock().unwrap();
        state.online()?;
        state.thumbnail_list_requests.push(folder_id.into());
        Ok(state
            .items
            .values()
            .filter(|item| {
                item.parent_id.as_deref() == Some(folder_id)
                    && state.thumbnails.contains_key(&item.item_id)
            })
            .map(|item| RemoteThumbnail {
                item_id: item.item_id.clone(),
                url: "https://fake-drive.invalid/thumbnail".into(),
            })
            .collect())
    }

    async fn download_thumbnail(&self, thumbnail: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        tokio::task::yield_now().await;
        let mut state = self.state.lock().unwrap();
        state.online()?;
        state
            .thumbnail_download_requests
            .push(thumbnail.item_id.clone());
        state
            .thumbnails
            .get(&thumbnail.item_id)
            .cloned()
            .ok_or(CloudError::Unavailable)
    }

    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        let mut state = self.state.lock().unwrap();
        state.online()?;
        state.requests.push(request.clone());
        if let Some((remaining, _)) = state.failures.front_mut() {
            if *remaining == 0 {
                return Err(state.failures.pop_front().unwrap().1);
            }
            *remaining -= 1;
        }

        let (changes, offset, version) = match request {
            DeltaRequest::Initial => {
                let mut items: Vec<_> = state.items.values().cloned().collect();
                items.sort_by_key(|item| item.parent_id.is_some());
                let changes: Vec<_> = items.into_iter().map(DeltaChange::Upsert).collect();
                (changes.into(), 0, state.changes.len())
            }
            DeltaRequest::Link(url) => match state.links.get(&url) {
                Some(Link::Page {
                    changes,
                    offset,
                    version,
                }) => (changes.clone(), *offset, *version),
                Some(Link::Delta(version)) => (
                    Arc::from(&state.changes[*version..]),
                    0,
                    state.changes.len(),
                ),
                None => return Err(CloudError::ResyncRequired),
            },
        };
        let end = offset
            .saturating_add(self.page_size.get())
            .min(changes.len());
        let page = changes[offset..end].to_vec();
        state.next_token += 1;
        let token = state.next_token;
        let url = format!("https://fake-drive.invalid/me/drive/root/delta?token={token}%2Bopaque");
        let cursor = if end == changes.len() {
            state.links.insert(url.clone(), Link::Delta(version));
            DeltaCursor::Complete(url)
        } else {
            state.links.insert(
                url.clone(),
                Link::Page {
                    changes,
                    offset: end,
                    version,
                },
            );
            DeltaCursor::Next(url)
        };
        Ok(DeltaPage {
            changes: page,
            cursor,
        })
    }
}
