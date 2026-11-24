use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest, ItemKind,
    RemoteItem, RemoteThumbnail, UploadTarget,
};
use reqwest::header::{self, HeaderValue};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep_until};

use crate::Auth;

const GRAPH_BASE: &str = "https://graph.microsoft.com/v1.0";
const DOWNLOAD_URL_LIFETIME: Duration = Duration::from_secs(300);
const SIMPLE_UPLOAD_LIMIT: usize = 10 * 1024 * 1024;
const FRAGMENT_ALIGNMENT: usize = 320 * 1024;
const FRAGMENT_SIZE: usize = 16 * FRAGMENT_ALIGNMENT;

pub struct GraphDrive {
    auth: Arc<Auth>,
    base: Url,
    client: Client,
    pause_until: Mutex<Instant>,
    download_urls: Mutex<HashMap<String, DownloadUrl>>,
}

#[derive(Clone)]
struct DownloadUrl {
    url: Url,
    expires: Instant,
}

impl GraphDrive {
    pub fn new(auth: Arc<Auth>) -> Result<Self, CloudError> {
        Self::with_base(auth, Url::parse(GRAPH_BASE).expect("constant Graph URL"))
    }

    fn with_base(auth: Arc<Auth>, base: Url) -> Result<Self, CloudError> {
        Ok(Self {
            auth,
            base,
            client: crate::http_client().map_err(|_| CloudError::Unavailable)?,
            pause_until: Mutex::new(Instant::now()),
            download_urls: Mutex::new(HashMap::new()),
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("hierarchical Graph base")
            .extend(["me", "drive"])
            .extend(segments);
        url
    }

    fn continuation(&self, value: &str) -> Result<Url, CloudError> {
        let url = parse_url(value)?;
        if url.origin() != self.base.origin() {
            return Err(CloudError::InvalidResponse);
        }
        Ok(url)
    }

    async fn wait_until_allowed(&self) {
        loop {
            let deadline = *self.pause_until.lock().unwrap();
            if deadline <= Instant::now() {
                return;
            }
            sleep_until(deadline).await;
        }
    }

    async fn graph(&self, request: RequestBuilder) -> Result<Response, CloudError> {
        self.wait_until_allowed().await;
        let bearer = self
            .auth
            .bearer()
            .await
            .map_err(|_| CloudError::Authentication)?;
        self.send(request.bearer_auth(bearer)).await
    }

    async fn send(&self, request: RequestBuilder) -> Result<Response, CloudError> {
        let request = request.build().map_err(|_| CloudError::InvalidResponse)?;
        self.wait_until_allowed().await;
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|_| CloudError::Unavailable)?;
        if matches!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
        ) {
            let retry_after = retry_after(&response);
            let mut pause = self.pause_until.lock().unwrap();
            *pause = (*pause).max(Instant::now() + retry_after);
            return Err(CloudError::Throttled { retry_after });
        }
        Ok(response)
    }

    async fn download_url(&self, item_id: &str) -> Result<Url, CloudError> {
        #[derive(Deserialize)]
        struct Metadata {
            #[serde(rename = "@microsoft.graph.downloadUrl")]
            download_url: String,
        }

        let now = Instant::now();
        {
            let mut urls = self.download_urls.lock().unwrap();
            urls.retain(|_, entry| entry.expires > now);
            if let Some(entry) = urls.get(item_id) {
                return Ok(entry.url.clone());
            }
        }

        let url = self.endpoint(&["items", item_id]);
        let metadata: Metadata = decode(self.graph(self.client.get(url)).await?).await?;
        let url = parse_url(&metadata.download_url)?;
        self.download_urls.lock().unwrap().insert(
            item_id.to_owned(),
            DownloadUrl {
                url: url.clone(),
                expires: Instant::now() + DOWNLOAD_URL_LIFETIME,
            },
        );
        Ok(url)
    }

    fn forget_download_url(&self, item_id: &str, expired: Option<&Url>) {
        let mut urls = self.download_urls.lock().unwrap();
        if expired
            .is_none_or(|expired| urls.get(item_id).is_some_and(|entry| &entry.url == expired))
        {
            urls.remove(item_id);
        }
    }

    async fn resumable_upload(
        &self,
        target: UploadTarget<'_>,
        bytes: &[u8],
        if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Session {
            upload_url: String,
        }

        let url = self.upload_endpoint(target, "createUploadSession");
        let body = match target {
            UploadTarget::New { name, .. } => json!({
                "item": {"name": name, "@microsoft.graph.conflictBehavior": "fail"}
            }),
            UploadTarget::Existing { .. } => json!({}),
        };
        let request = conditional(self.client.post(url).json(&body), if_match);
        let session: Session = decode(self.graph(request).await?).await?;
        let url = parse_url(&session.upload_url)?;
        let mut start = 0;
        let mut recoveries = 0;

        loop {
            let end = (start + FRAGMENT_SIZE).min(bytes.len());
            let request = self
                .client
                .put(url.clone())
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end - 1, bytes.len()),
                )
                .header(header::CONTENT_LENGTH, end - start)
                .body(bytes[start..end].to_vec());
            let response = match self.send(request).await {
                Ok(response)
                    if !response.status().is_server_error()
                        && response.status() != StatusCode::RANGE_NOT_SATISFIABLE =>
                {
                    response
                }
                Ok(_) | Err(CloudError::Unavailable | CloudError::Throttled { .. })
                    if recoveries < 3 =>
                {
                    recoveries += 1;
                    let status = self.send(self.client.get(url.clone())).await?;
                    start = next_upload_offset(status, bytes.len()).await?;
                    continue;
                }
                Ok(_) => return Err(CloudError::Unavailable),
                Err(error) => return Err(error),
            };
            match response.status() {
                StatusCode::OK | StatusCode::CREATED if end == bytes.len() => {
                    return decode_item(response).await;
                }
                StatusCode::ACCEPTED => {
                    let next = next_upload_offset(response, bytes.len()).await?;
                    if next != end {
                        recoveries += 1;
                        if recoveries > 3 {
                            return Err(CloudError::InvalidResponse);
                        }
                    }
                    start = next;
                }
                status if status.is_success() => return Err(CloudError::InvalidResponse),
                status => return Err(status_error(status)),
            }
        }
    }

    fn upload_endpoint(&self, target: UploadTarget<'_>, operation: &str) -> Url {
        match target {
            UploadTarget::Existing { item_id } => self.endpoint(&["items", item_id, operation]),
            UploadTarget::New { parent_id, name } => self.endpoint(&[
                "items",
                &format!("{parent_id}:"),
                &format!("{name}:"),
                operation,
            ]),
        }
    }
}

#[async_trait]
impl CloudDrive for GraphDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        let url = match request {
            DeltaRequest::Initial => {
                let mut url = self.endpoint(&["root", "delta"]);
                url.query_pairs_mut().append_pair("$top", "1000");
                url
            }
            DeltaRequest::Link(link) => self.continuation(&link)?,
        };
        let response = self.graph(self.client.get(url)).await?;
        match response.status() {
            StatusCode::GONE => return Err(CloudError::ResyncRequired),
            StatusCode::NOT_FOUND => {
                let body = response.json::<Value>().await.unwrap_or_default();
                return Err(if body["error"]["code"] == "resyncRequired" {
                    CloudError::ResyncRequired
                } else {
                    CloudError::NotFound
                });
            }
            _ => {}
        }

        let page: Page<DriveItem> = decode(response).await?;
        let cursor = match (page.next, page.delta) {
            (Some(link), None) => DeltaCursor::Next(link),
            (None, Some(link)) => DeltaCursor::Complete(link),
            _ => return Err(CloudError::InvalidResponse),
        };
        let changes = page
            .value
            .into_iter()
            .map(|item| {
                if item.deleted.is_some() {
                    Ok(DeltaChange::Delete(item.id))
                } else {
                    item.into_remote().map(DeltaChange::Upsert)
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(DeltaPage { changes, cursor })
    }

    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        let mut url = self.endpoint(&["items", folder_id, "children"]);
        url.query_pairs_mut().append_pair("$expand", "thumbnails");
        let mut thumbnails = Vec::new();
        loop {
            let page: Page<ThumbnailChild> =
                decode(self.graph(self.client.get(url)).await?).await?;
            thumbnails.extend(page.value.into_iter().filter_map(|child| {
                let large = child.thumbnails.into_iter().find_map(|set| set.large)?;
                Some(RemoteThumbnail {
                    item_id: child.id,
                    url: large.url,
                })
            }));
            let Some(next) = page.next else {
                return Ok(thumbnails);
            };
            url = self.continuation(&next)?;
        }
    }

    async fn download_thumbnail(&self, thumbnail: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        let url = parse_url(&thumbnail.url)?;
        let response = successful(self.send(self.client.get(url)).await?)?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|_| CloudError::Unavailable)
    }

    async fn read_range(
        &self,
        item_id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length as u64 - 1)
            .ok_or(CloudError::InvalidResponse)?;
        for attempt in 0..2 {
            let url = self.download_url(item_id).await?;
            let request = self
                .client
                .get(url.clone())
                .header(header::RANGE, format!("bytes={offset}-{end}"))
                .header(header::ACCEPT_ENCODING, "identity");
            let response = self.send(request).await?;
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::GONE
            ) {
                self.forget_download_url(item_id, Some(&url));
                if attempt == 0 {
                    continue;
                }
            }
            return range_bytes(response, offset, end, length).await;
        }
        unreachable!("the second download attempt always returns")
    }

    async fn upload(
        &self,
        target: UploadTarget<'_>,
        bytes: &[u8],
        if_match: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        let item = if bytes.len() > SIMPLE_UPLOAD_LIMIT {
            self.resumable_upload(target, bytes, if_match).await?
        } else {
            let mut url = self.upload_endpoint(target, "content");
            if matches!(target, UploadTarget::New { .. }) {
                url.query_pairs_mut()
                    .append_pair("@microsoft.graph.conflictBehavior", "fail");
            }
            let request = self
                .client
                .put(url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(bytes.to_vec());
            decode_item(self.graph(conditional(request, if_match)).await?).await?
        };
        self.forget_download_url(&item.item_id, None);
        Ok(item)
    }

    async fn create_folder(&self, parent_id: &str, name: &str) -> Result<RemoteItem, CloudError> {
        let url = self.endpoint(&["items", parent_id, "children"]);
        let body = json!({"name": name, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"});
        decode_item(self.graph(self.client.post(url).json(&body)).await?).await
    }

    async fn move_item(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
    ) -> Result<RemoteItem, CloudError> {
        let url = self.endpoint(&["items", item_id]);
        let body = json!({"name": name, "parentReference": {"id": parent_id}});
        decode_item(self.graph(self.client.patch(url).json(&body)).await?).await
    }

    async fn delete_item(&self, item_id: &str) -> Result<(), CloudError> {
        let url = self.endpoint(&["items", item_id]);
        successful(self.graph(self.client.delete(url)).await?)?;
        self.forget_download_url(item_id, None);
        Ok(())
    }
}

fn retry_after(response: &Response) -> Duration {
    response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| {
            let value = value.to_str().ok()?;
            if let Ok(seconds) = value.parse() {
                return Some(Duration::from_secs(seconds));
            }
            Some(
                httpdate::parse_http_date(value)
                    .ok()?
                    .duration_since(SystemTime::now())
                    .unwrap_or_default(),
            )
        })
        .filter(|delay| Instant::now().checked_add(*delay).is_some())
        .unwrap_or(Duration::from_secs(1))
}

fn parse_url(value: &str) -> Result<Url, CloudError> {
    let url = Url::parse(value).map_err(|_| CloudError::InvalidResponse)?;
    let loopback = cfg!(test) && url.scheme() == "http" && url.host_str() == Some("127.0.0.1");
    if (url.scheme() != "https" && !loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(CloudError::InvalidResponse);
    }
    Ok(url)
}

fn conditional(request: RequestBuilder, if_match: Option<&str>) -> RequestBuilder {
    match if_match {
        Some(tag) => request.header(header::IF_MATCH, tag),
        None => request,
    }
}

async fn next_upload_offset(response: Response, size: usize) -> Result<usize, CloudError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Progress {
        next_expected_ranges: Vec<String>,
    }

    let progress: Progress = decode(response).await?;
    progress
        .next_expected_ranges
        .iter()
        .map(|range| {
            range
                .split_once('-')
                .and_then(|(start, _)| start.parse::<usize>().ok())
                .filter(|start| *start < size && start.is_multiple_of(FRAGMENT_ALIGNMENT))
                .ok_or(CloudError::InvalidResponse)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .min()
        .ok_or(CloudError::InvalidResponse)
}

async fn range_bytes(
    mut response: Response,
    offset: u64,
    end: u64,
    length: usize,
) -> Result<Vec<u8>, CloudError> {
    let headers = response.headers();
    if headers
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .any(|value| value != "identity")
    {
        return Err(CloudError::InvalidResponse);
    }
    let content_range = headers.get(header::CONTENT_RANGE);
    let wanted = match response.status() {
        StatusCode::PARTIAL_CONTENT => checked_range(content_range, offset, end)?,
        StatusCode::OK if offset == 0 && content_range.is_none() => length,
        StatusCode::OK => return Err(CloudError::InvalidResponse),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            let size = content_range
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("bytes */"))
                .and_then(|value| value.parse::<u64>().ok());
            return match size {
                Some(size) if offset >= size => Ok(Vec::new()),
                _ => Err(CloudError::InvalidResponse),
            };
        }
        status => return Err(status_error(status)),
    };
    let mut bytes = Vec::new();
    while bytes.len() < wanted {
        let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CloudError::Unavailable)?
        else {
            break;
        };
        let remaining = wanted - bytes.len();
        if response.status() == StatusCode::PARTIAL_CONTENT && chunk.len() > remaining {
            return Err(CloudError::InvalidResponse);
        }
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    if response.status() == StatusCode::PARTIAL_CONTENT
        && (bytes.len() != wanted
            || response
                .chunk()
                .await
                .map_err(|_| CloudError::Unavailable)?
                .is_some())
    {
        return Err(CloudError::InvalidResponse);
    }
    Ok(bytes)
}

fn checked_range(
    value: Option<&HeaderValue>,
    offset: u64,
    requested_end: u64,
) -> Result<usize, CloudError> {
    let (range, total) = value
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .ok_or(CloudError::InvalidResponse)?;
    let (start, end) = range.split_once('-').ok_or(CloudError::InvalidResponse)?;
    let start: u64 = start.parse().map_err(|_| CloudError::InvalidResponse)?;
    let end: u64 = end.parse().map_err(|_| CloudError::InvalidResponse)?;
    let expected_end = if total == "*" {
        requested_end
    } else {
        let total: u64 = total.parse().map_err(|_| CloudError::InvalidResponse)?;
        requested_end.min(total.checked_sub(1).ok_or(CloudError::InvalidResponse)?)
    };
    if start != offset || end < start || end != expected_end {
        return Err(CloudError::InvalidResponse);
    }
    Ok((end - start + 1) as usize)
}

fn successful(response: Response) -> Result<Response, CloudError> {
    response
        .status()
        .is_success()
        .then_some(())
        .ok_or_else(|| status_error(response.status()))?;
    Ok(response)
}

fn status_error(status: StatusCode) -> CloudError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => CloudError::Authentication,
        StatusCode::NOT_FOUND | StatusCode::GONE => CloudError::NotFound,
        StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => CloudError::Conflict,
        status if status.is_server_error() => CloudError::Unavailable,
        _ => CloudError::InvalidResponse,
    }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, CloudError> {
    successful(response)?
        .json()
        .await
        .map_err(|_| CloudError::InvalidResponse)
}

async fn decode_item(response: Response) -> Result<RemoteItem, CloudError> {
    decode::<DriveItem>(response).await?.into_remote()
}

#[derive(Deserialize)]
struct Page<T> {
    value: Vec<T>,
    #[serde(rename = "@odata.nextLink")]
    next: Option<String>,
    #[serde(rename = "@odata.deltaLink")]
    delta: Option<String>,
}

#[derive(Deserialize)]
struct ThumbnailChild {
    id: String,
    #[serde(default)]
    thumbnails: Vec<ThumbnailSet>,
}

#[derive(Deserialize)]
struct ThumbnailSet {
    large: Option<Thumbnail>,
}

#[derive(Deserialize)]
struct Thumbnail {
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveItem {
    id: String,
    name: Option<String>,
    parent_reference: Option<Parent>,
    size: Option<u64>,
    #[serde(rename = "eTag")]
    etag: Option<String>,
    #[serde(rename = "cTag")]
    ctag: Option<String>,
    last_modified_date_time: Option<String>,
    file_system_info: Option<FileSystemInfo>,
    file: Option<FileFacet>,
    folder: Option<Value>,
    root: Option<Value>,
    deleted: Option<Value>,
}

#[derive(Deserialize)]
struct Parent {
    id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileSystemInfo {
    last_modified_date_time: Option<String>,
}

#[derive(Deserialize)]
struct FileFacet {
    hashes: Option<Hashes>,
}

#[derive(Deserialize)]
struct Hashes {
    #[serde(rename = "quickXorHash")]
    quickxor_hash: Option<String>,
}

impl DriveItem {
    fn into_remote(self) -> Result<RemoteItem, CloudError> {
        let timestamp = self
            .file_system_info
            .and_then(|info| info.last_modified_date_time)
            .or(self.last_modified_date_time);
        let mtime = timestamp
            .as_deref()
            .map(chrono::DateTime::parse_from_rfc3339)
            .transpose()
            .map_err(|_| CloudError::InvalidResponse)?
            .map_or(0, |timestamp| timestamp.timestamp());
        Ok(RemoteItem {
            item_id: self.id,
            parent_id: if self.root.is_some() {
                None
            } else {
                self.parent_reference.and_then(|parent| parent.id)
            },
            name: self.name,
            kind: if self.folder.is_some() {
                ItemKind::Folder
            } else {
                ItemKind::File
            },
            size: self.size.unwrap_or_default(),
            mtime,
            etag: self.etag,
            ctag: self.ctag,
            quickxor_hash: self
                .file
                .and_then(|file| file.hashes)
                .and_then(|hashes| hashes.quickxor_hash),
        })
    }
}

#[cfg(test)]
mod tests;
