use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{
    CloudDrive, CloudError, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest, RemoteItem,
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
    contents: HashMap<String, Vec<u8>>,
    range_requests: Vec<(String, u64, usize)>,
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
        let mut removed = vec![item_id.to_owned()];
        while let Some(parent) = removed.pop() {
            state.contents.remove(&parent);
            state.items.remove(&parent);
            removed.extend(state.items.iter().filter_map(|(id, item)| {
                (item.parent_id.as_deref() == Some(&parent)).then(|| id.clone())
            }));
        }
        state.changes.push(DeltaChange::Delete(item_id.into()));
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
            .insert(item_id.into(), bytes);
    }

    pub fn range_requests(&self) -> Vec<(String, u64, usize)> {
        self.state.lock().unwrap().range_requests.clone()
    }

    pub fn requests(&self) -> Vec<DeltaRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

#[async_trait]
impl CloudDrive for FakeCloudDrive {
    async fn read_range(
        &self,
        item_id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        let mut state = self.state.lock().unwrap();
        state.range_requests.push((item_id.into(), offset, length));
        let bytes = state.contents.get(item_id).ok_or(CloudError::Unavailable)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = start.saturating_add(length).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        let mut state = self.state.lock().unwrap();
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
