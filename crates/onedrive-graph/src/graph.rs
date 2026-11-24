use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, DeltaChange, DeltaCursor, DeltaPage, DeltaRequest, ItemKind,
    RemoteItem, RemoteThumbnail, UploadSource, UploadTarget,
};
use reqwest::header::{self, HeaderValue};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Instant, sleep_until};

use crate::{Auth, AuthError};

const GRAPH_BASE: &str = "https://graph.microsoft.com/v1.0";
const SIMPLE_UPLOAD_LIMIT: usize = 10 * 1024 * 1024;
const FRAGMENT_ALIGNMENT: usize = 320 * 1024;
const FRAGMENT_SIZE: usize = 16 * FRAGMENT_ALIGNMENT;

pub struct GraphDrive {
    auth: Arc<Auth>,
    base: Url,
    client: Client,
    pause_until: Mutex<Instant>,
    binding: DriveBinding,
    verified: AsyncMutex<Option<(String, String)>>,
}

enum DriveBinding {
    Unselected,
    Selected(String),
    #[cfg(test)]
    Unchecked,
}

impl GraphDrive {
    pub fn new(auth: Arc<Auth>) -> Result<Self, CloudError> {
        Self::build(
            auth,
            Url::parse(GRAPH_BASE).expect("constant Graph URL"),
            DriveBinding::Unselected,
        )
    }

    #[cfg(test)]
    fn with_base(auth: Arc<Auth>, base: Url) -> Result<Self, CloudError> {
        Self::build(auth, base, DriveBinding::Unchecked)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_with_base(auth: Arc<Auth>, base: &str) -> Result<Self, CloudError> {
        let base = Url::parse(base).map_err(|_| CloudError::InvalidResponse)?;
        if base.host_str() != Some("127.0.0.1") {
            return Err(CloudError::InvalidResponse);
        }
        Self::build(auth, base, DriveBinding::Unselected)
    }

    fn build(auth: Arc<Auth>, base: Url, binding: DriveBinding) -> Result<Self, CloudError> {
        Ok(Self {
            auth,
            base,
            client: crate::http_client().map_err(|_| CloudError::Unavailable)?,
            pause_until: Mutex::new(Instant::now()),
            binding,
            verified: AsyncMutex::new(None),
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
        let url = self.parse_url(value)?;
        if url.origin() != self.base.origin() {
            return Err(CloudError::InvalidResponse);
        }
        Ok(url)
    }

    fn parse_url(&self, value: &str) -> Result<Url, CloudError> {
        let url = Url::parse(value).map_err(|_| CloudError::InvalidResponse)?;
        let loopback = cfg!(any(test, feature = "test-support"))
            && self.base.host_str() == Some("127.0.0.1")
            && url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1");
        if (url.scheme() != "https" && !loopback)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
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

    /// A filesystem-safe key derived from the authenticated personal drive ID.
    pub async fn discover_drive(&self) -> Result<String, CloudError> {
        self.wait_until_allowed().await;
        let bearer = self.bearer().await?;
        self.identify(&bearer).await
    }

    /// Keep the namespace fixed for the lifetime of the mounted Engine.
    pub fn bind(mut self, drive_key: String) -> Self {
        self.binding = DriveBinding::Selected(drive_key);
        self
    }

    pub async fn root_id(&self) -> Result<String, CloudError> {
        let root: DriveItem = decode(
            self.graph(self.client.get(self.endpoint(&["root"])))
                .await?,
        )
        .await?;
        if root.id.is_empty() || root.folder.is_none() || root.deleted.is_some() {
            return Err(CloudError::InvalidResponse);
        }
        Ok(root.id)
    }

    async fn bearer(&self) -> Result<String, CloudError> {
        self.auth.bearer().await.map_err(|error| match error {
            AuthError::LoginRequired
            | AuthError::GrantRejected
            | AuthError::Denied
            | AuthError::MissingClientId => CloudError::Authentication,
            AuthError::InvalidToken => CloudError::InvalidResponse,
            _ => CloudError::Unavailable,
        })
    }

    async fn identify(&self, bearer: &str) -> Result<String, CloudError> {
        let mut verified = self.verified.lock().await;
        if let Some((token, drive)) = verified.as_ref()
            && token == bearer
        {
            return Ok(drive.clone());
        }
        self.wait_until_allowed().await;
        #[derive(Deserialize)]
        struct Drive {
            id: String,
        }
        let mut url = self.endpoint(&[]);
        url.query_pairs_mut().append_pair("$select", "id");
        let drive: Drive = decode(
            self.dispatch(self.client.get(url).bearer_auth(bearer))
                .await?,
        )
        .await?;
        if drive.id.is_empty() {
            return Err(CloudError::InvalidResponse);
        }
        let key = format!("{:x}", Sha256::digest(drive.id.as_bytes()));
        *verified = Some((bearer.to_owned(), key.clone()));
        Ok(key)
    }

    async fn bound_bearer(&self) -> Result<String, CloudError> {
        let bound = match &self.binding {
            DriveBinding::Selected(bound) => bound,
            DriveBinding::Unselected => return Err(CloudError::Authentication),
            #[cfg(test)]
            DriveBinding::Unchecked => return self.bearer().await,
        };
        loop {
            self.wait_until_allowed().await;
            let bearer = self.bearer().await?;
            if self.identify(&bearer).await? != *bound {
                return Err(CloudError::Authentication);
            }
            // The identity probe may take time. Recheck the credentials before
            // using this bearer or dispatching a signed URL without a bearer.
            if self.bearer().await? == bearer && *self.pause_until.lock().unwrap() <= Instant::now()
            {
                return Ok(bearer);
            }
        }
    }

    async fn graph(&self, request: RequestBuilder) -> Result<Response, CloudError> {
        self.wait_until_allowed().await;
        let bearer = self.bound_bearer().await?;
        self.dispatch(request.bearer_auth(bearer)).await
    }

    async fn send(&self, request: RequestBuilder) -> Result<Response, CloudError> {
        self.wait_until_allowed().await;
        #[cfg(test)]
        if matches!(self.binding, DriveBinding::Unchecked) {
            return self.dispatch(request).await;
        }
        self.bound_bearer().await?;
        self.dispatch(request).await
    }

    async fn dispatch(&self, request: RequestBuilder) -> Result<Response, CloudError> {
        let request = request.build().map_err(|_| CloudError::InvalidResponse)?;
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

        // A signed URL may keep serving an old version after its item changes.
        let url = self.endpoint(&["items", item_id]);
        let metadata: Metadata = decode(self.graph(self.client.get(url)).await?).await?;
        self.parse_url(&metadata.download_url)
    }

    async fn resumable_upload(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
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
        let url = self.parse_url(&session.upload_url)?;
        let mut start = 0;
        let mut recoveries = 0;
        let mut final_uncertain = false;

        loop {
            let end = (start + FRAGMENT_SIZE).min(source.len());
            let final_fragment = end == source.len();
            let request = self
                .client
                .put(url.clone())
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end - 1, source.len()),
                )
                .header(header::CONTENT_LENGTH, end - start)
                .body(source.read(start, end - start).await?);
            let retry_error = match self.send(request).await {
                Ok(response)
                    if final_fragment
                        && matches!(response.status(), StatusCode::OK | StatusCode::CREATED) =>
                {
                    if let Ok(item) = decode_upload_item(response, target).await {
                        return Ok(item);
                    }
                    break;
                }
                Ok(response) if response.status() == StatusCode::ACCEPTED => {
                    let next = match next_upload_offset(response, source.len()).await {
                        Ok(next) => next,
                        Err(_) if final_fragment || final_uncertain => break,
                        Err(error) => return Err(error),
                    };
                    if next != end {
                        recoveries += 1;
                        if recoveries > 3 {
                            if final_fragment || final_uncertain {
                                break;
                            }
                            return Err(CloudError::InvalidResponse);
                        }
                    }
                    start = next;
                    continue;
                }
                Ok(response) if retryable_upload_status(response.status()) => {
                    CloudError::Unavailable
                }
                Err(error @ (CloudError::Unavailable | CloudError::Throttled { .. })) => error,
                Ok(response) => {
                    let status = response.status();
                    if ((final_fragment || final_uncertain)
                        && (session_disappeared(status) || status.is_success()))
                        || (final_uncertain && status_error(status) == CloudError::Conflict)
                    {
                        break;
                    }
                    return Err(if session_disappeared(status) {
                        CloudError::Unavailable
                    } else {
                        status_error(status)
                    });
                }
                Err(error) => return Err(error),
            };
            final_uncertain |= final_fragment;
            if recoveries >= 3 {
                if final_uncertain {
                    break;
                }
                return Err(retry_error);
            }
            recoveries += 1;
            let progress = match self.send(self.client.get(url.clone())).await {
                Ok(status) => next_upload_offset(status, source.len()).await,
                Err(error) => Err(error),
            };
            match progress {
                Ok(next) => start = next,
                Err(_) if final_uncertain => break,
                Err(CloudError::NotFound) => return Err(CloudError::Unavailable),
                Err(error) => return Err(error),
            }
        }

        self.confirm_upload(target, source, if_match)
            .await?
            .ok_or(CloudError::Unavailable)
    }

    async fn confirm_destination(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        let destination = match target {
            UploadTarget::Existing { item_id } => self.endpoint(&["items", item_id]),
            UploadTarget::New { parent_id, name } => {
                self.endpoint(&["items", &format!("{parent_id}:"), name])
            }
        };
        let Some(item) = self.fetch_destination(&destination).await? else {
            return Ok(None);
        };
        let tag = item.etag.as_deref().filter(|tag| !tag.is_empty());
        if !item.matches_destination(target) || tag.is_none() {
            return Err(CloudError::Unavailable);
        }
        if if_match != Some("*") && tag == if_match {
            return Ok(None);
        }
        let matches = if item.file.is_some()
            && item.folder.is_none()
            && item.size == Some(source.len() as u64)
        {
            let url = item
                .download_url
                .as_deref()
                .ok_or(CloudError::Unavailable)
                .and_then(|url| self.parse_url(url))?;
            let request = self
                .client
                .get(url)
                .header(header::ACCEPT_ENCODING, "identity");
            matches_upload_bytes(self.send(request).await?, source).await?
        } else if item.folder.is_some() || item.size.is_some() {
            false
        } else {
            return Err(CloudError::Unavailable);
        };

        // Confirm the download and destination lookup refer to the same current version.
        let current = self
            .fetch_destination(&destination)
            .await?
            .ok_or(CloudError::Unavailable)?;
        if !current.matches_destination(target)
            || current.id != item.id
            || current.etag != item.etag
        {
            return Err(CloudError::Unavailable);
        }
        if matches {
            current.into_remote().map(Some)
        } else if matches!(target, UploadTarget::New { .. })
            || if_match.is_some_and(|tag| tag != "*")
        {
            Err(CloudError::Conflict)
        } else {
            Err(CloudError::Unavailable)
        }
    }

    async fn move_with_condition(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
        etag: Option<&str>,
    ) -> Result<RemoteItem, CloudError> {
        let url = self.endpoint(&["items", item_id]);
        let body = json!({"name": name, "parentReference": {"id": parent_id}});
        let request = conditional(self.client.patch(url).json(&body), etag);
        decode_item(self.graph(request).await?).await
    }

    async fn fetch_destination(&self, url: &Url) -> Result<Option<DriveItem>, CloudError> {
        let response = self.graph(self.client.get(url.clone())).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode(response).await.map(Some)
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
        let url = self.parse_url(&thumbnail.url)?;
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
        let range = ByteRange::new(offset, length)?;
        for attempt in 0..2 {
            let url = self.download_url(item_id).await?;
            let request = self
                .client
                .get(url)
                .header(
                    header::RANGE,
                    format!("bytes={}-{}", range.start, range.end),
                )
                .header(header::ACCEPT_ENCODING, "identity");
            let response = self.send(request).await?;
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::GONE
            ) && attempt == 0
            {
                continue;
            }
            return range_bytes(response, range).await;
        }
        unreachable!("the second download attempt always returns")
    }

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
        if source.len() > SIMPLE_UPLOAD_LIMIT {
            return self.resumable_upload(target, source, if_match).await;
        }
        let mut url = self.upload_endpoint(target, "content");
        if matches!(target, UploadTarget::New { .. }) {
            url.query_pairs_mut()
                .append_pair("@microsoft.graph.conflictBehavior", "fail");
        }
        let request = self
            .client
            .put(url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(source.read(0, source.len()).await?);
        match self.graph(conditional(request, if_match)).await {
            Ok(response) if response.status().is_success() => {
                match decode_upload_item(response, target).await {
                    Ok(item) => Ok(item),
                    Err(_) => self
                        .confirm_upload(target, source, if_match)
                        .await?
                        .ok_or(CloudError::Unavailable),
                }
            }
            Ok(response)
                if response.status().is_server_error()
                    || response.status() == StatusCode::REQUEST_TIMEOUT =>
            {
                self.confirm_upload(target, source, if_match)
                    .await?
                    .ok_or(CloudError::Unavailable)
            }
            Err(CloudError::Unavailable) => self
                .confirm_upload(target, source, if_match)
                .await?
                .ok_or(CloudError::Unavailable),
            Ok(response) => Err(status_error(response.status())),
            Err(error) => Err(error),
        }
    }

    async fn upload_destination(
        &self,
        target: UploadTarget<'_>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        let url = match target {
            UploadTarget::Existing { item_id } => self.endpoint(&["items", item_id]),
            UploadTarget::New { parent_id, name } => {
                self.endpoint(&["items", &format!("{parent_id}:"), name])
            }
        };
        self.fetch_destination(&url)
            .await?
            .map(|item| {
                if !item.matches_destination(target) {
                    return Err(CloudError::InvalidResponse);
                }
                item.into_remote()
            })
            .transpose()
    }

    async fn confirm_upload(
        &self,
        target: UploadTarget<'_>,
        source: UploadSource<'_>,
        if_match: Option<&str>,
    ) -> Result<Option<RemoteItem>, CloudError> {
        self.confirm_destination(target, source, if_match).await
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
        self.move_with_condition(item_id, parent_id, name, None)
            .await
    }

    async fn move_if_match(
        &self,
        item_id: &str,
        parent_id: &str,
        name: &str,
        etag: &str,
    ) -> Result<RemoteItem, CloudError> {
        self.move_with_condition(item_id, parent_id, name, Some(etag))
            .await
    }

    async fn delete_if_match(&self, item_id: &str, etag: &str) -> Result<(), CloudError> {
        let url = self.endpoint(&["items", item_id]);
        successful(
            self.graph(conditional(self.client.delete(url), Some(etag)))
                .await?,
        )?;
        Ok(())
    }

    async fn delete_item(&self, item_id: &str) -> Result<(), CloudError> {
        let url = self.endpoint(&["items", item_id]);
        successful(self.graph(self.client.delete(url)).await?)?;
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

fn conditional(request: RequestBuilder, if_match: Option<&str>) -> RequestBuilder {
    match if_match {
        Some(tag) => request.header(header::IF_MATCH, tag),
        None => request,
    }
}

fn session_disappeared(status: StatusCode) -> bool {
    matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE)
}

fn retryable_upload_status(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::RANGE_NOT_SATISFIABLE | StatusCode::REQUEST_TIMEOUT
        )
}

async fn matches_upload_bytes(
    mut response: Response,
    source: UploadSource<'_>,
) -> Result<bool, CloudError> {
    if response.status() != StatusCode::OK
        || response.headers().contains_key(header::CONTENT_RANGE)
        || !identity_encoding(&response)
    {
        return Err(CloudError::Unavailable);
    }
    let mut offset = 0;
    let mut matches = true;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| CloudError::Unavailable)?
    {
        if chunk.len() > source.len() - offset {
            return Err(CloudError::Unavailable);
        }
        for part in chunk.chunks(FRAGMENT_SIZE) {
            matches &= part == source.read(offset, part.len()).await?;
            offset += part.len();
        }
    }
    if offset != source.len() {
        return Err(CloudError::Unavailable);
    }
    Ok(matches)
}

fn identity_encoding(response: &Response) -> bool {
    response
        .headers()
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .all(|value| value == "identity")
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

struct ByteRange {
    start: u64,
    end: u64,
    length: usize,
}

impl ByteRange {
    fn new(start: u64, length: usize) -> Result<Self, CloudError> {
        let end = u64::try_from(length)
            .ok()
            .and_then(|length| length.checked_sub(1))
            .and_then(|last| start.checked_add(last))
            .ok_or(CloudError::InvalidResponse)?;
        Ok(Self { start, end, length })
    }
}

async fn range_bytes(mut response: Response, range: ByteRange) -> Result<Vec<u8>, CloudError> {
    if !identity_encoding(&response) {
        return Err(CloudError::InvalidResponse);
    }
    let content_range = response.headers().get(header::CONTENT_RANGE);
    let wanted = match response.status() {
        StatusCode::PARTIAL_CONTENT => checked_range(content_range, &range)?,
        StatusCode::OK if range.start == 0 && content_range.is_none() => range.length,
        StatusCode::OK => return Err(CloudError::InvalidResponse),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            let size = content_range
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("bytes */"))
                .and_then(|value| value.parse::<u64>().ok());
            return match size {
                Some(size) if range.start >= size => Ok(Vec::new()),
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

fn checked_range(value: Option<&HeaderValue>, requested: &ByteRange) -> Result<usize, CloudError> {
    let (range, total) = value
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes "))
        .and_then(|value| value.split_once('/'))
        .ok_or(CloudError::InvalidResponse)?;
    let (start, end) = range.split_once('-').ok_or(CloudError::InvalidResponse)?;
    let start: u64 = start.parse().map_err(|_| CloudError::InvalidResponse)?;
    let end: u64 = end.parse().map_err(|_| CloudError::InvalidResponse)?;
    let expected_end = if total == "*" {
        requested.end
    } else {
        let total: u64 = total.parse().map_err(|_| CloudError::InvalidResponse)?;
        requested
            .end
            .min(total.checked_sub(1).ok_or(CloudError::InvalidResponse)?)
    };
    if start != requested.start || end < start || end != expected_end {
        return Err(CloudError::InvalidResponse);
    }
    usize::try_from(end - start + 1).map_err(|_| CloudError::InvalidResponse)
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

async fn decode_upload_item(
    response: Response,
    target: UploadTarget<'_>,
) -> Result<RemoteItem, CloudError> {
    let item: DriveItem = decode(response).await?;
    if !item.matches_destination(target) || item.folder.is_some() {
        return Err(CloudError::InvalidResponse);
    }
    item.into_remote()
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
    #[serde(rename = "@microsoft.graph.downloadUrl")]
    download_url: Option<String>,
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
    fn matches_destination(&self, target: UploadTarget<'_>) -> bool {
        if self.id.is_empty() || self.deleted.is_some() {
            return false;
        }
        match target {
            UploadTarget::Existing { item_id } => self.id == item_id,
            UploadTarget::New { parent_id, name } => {
                self.name.as_deref() == Some(name)
                    && self
                        .parent_reference
                        .as_ref()
                        .and_then(|parent| parent.id.as_deref())
                        == Some(parent_id)
            }
        }
    }

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

#[cfg(test)]
mod regressions;
