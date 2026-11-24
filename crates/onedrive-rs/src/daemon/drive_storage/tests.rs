use std::sync::Mutex;
use std::time::Duration;

use onedrive_core::{CloudDrive, CloudError, EngineError, ItemKind, RemoteThumbnail, UploadTarget};
use onedrive_graph::{Auth, FakeTokenStore, TokenStore};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::oneshot;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::*;

struct Fixture {
    _dir: TempDir,
    data: PathBuf,
    cache: PathBuf,
    state: PathBuf,
    server: MockServer,
    store: Arc<FakeTokenStore>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let cache = dir.path().join("cache");
        let state = dir.path().join("state");
        let _lock = super::super::prepare_storage(&data, &cache).unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(|request: &Request| {
                let body = String::from_utf8_lossy(&request.body);
                let account = ["a", "b"]
                    .into_iter()
                    .find(|account| body.contains(&format!("refresh_token={account}")))
                    .unwrap();
                assert!(body.contains("scope=Files.ReadWrite+offline_access"));
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token":format!("token-{account}"), "expires_in":3600
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1.0/me/drive"))
            .respond_with(|request: &Request| {
                assert_eq!(request.url.query(), Some("%24select=id"));
                ResponseTemplate::new(200).set_body_json(json!({"id": account(request)}))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1.0/me/drive/root"))
            .respond_with(|request: &Request| {
                ResponseTemplate::new(200).set_body_json(root(account(request)))
            })
            .mount(&server)
            .await;
        let continuation = server.uri() + "/v1.0/old-continuation?opaque=%2B";
        Mock::given(method("GET"))
            .and(path("/v1.0/me/drive/root/delta"))
            .respond_with(move |request: &Request| {
                let account = account(request);
                ResponseTemplate::new(200).set_body_json(json!({
                    "value": [root(account), file(account, "shared-item", "draft.txt", 0, "initial"),
                        file(account, "cold-item", "cold.txt", 4, "initial")],
                    "@odata.deltaLink": continuation
                }))
            })
            .mount(&server)
            .await;
        Self {
            _dir: dir,
            data,
            cache,
            state,
            server,
            store: Arc::default(),
        }
    }

    fn auth(&self) -> Arc<Auth> {
        Arc::new(
            Auth::test_with_authority(&(self.server.uri() + "/oauth/"), self.store.clone())
                .unwrap(),
        )
    }

    fn graph(&self) -> GraphDrive {
        GraphDrive::test_with_base(self.auth(), &(self.server.uri() + "/v1.0")).unwrap()
    }

    async fn sign_in(&self, account: &str) {
        self.store.replace_refresh_token(account).await.unwrap();
    }

    async fn open(&self) -> Engine {
        let _lock = super::super::lock_storage(&self.data).unwrap();
        open(
            &self.data,
            &self.cache,
            Some(&self.state),
            self.graph(),
            Config {
                min_free_disk_bytes: 0,
                ..Config::default()
            },
        )
        .await
        .unwrap()
    }
}

fn account(request: &Request) -> &str {
    request
        .headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .strip_prefix("Bearer token-")
        .unwrap()
}

fn root(account: &str) -> serde_json::Value {
    json!({"id":format!("root-{account}"), "name":"root", "root":{}, "folder":{}})
}

fn file(account: &str, id: &str, name: &str, size: usize, tag: &str) -> serde_json::Value {
    json!({"id":id, "name":name, "parentReference":{"id":format!("root-{account}")},
        "file":{}, "size":size, "eTag":tag, "cTag":tag})
}

#[tokio::test]
async fn account_switch_keeps_mount_queue_and_checkpoints_with_their_drive() {
    let fixture = Fixture::new().await;
    let first_run = fixture.open().await;
    assert!(first_run.root().unwrap().is_none());
    assert!(matches!(
        first_run.sync().await,
        Err(EngineError::Cloud(CloudError::Authentication))
    ));
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
    drop(first_run);

    fixture.sign_in("a").await;
    let a = fixture.open().await;
    let bytes = b"account A offline bytes";
    a.sync().await.unwrap();
    assert_root(&a, "root-a");
    let inode = a.item("shared-item").unwrap().unwrap().inode;

    fixture.auth().logout().await.unwrap();
    a.write(inode, 0, bytes, false).await.unwrap();
    assert_authentication(a.sync().await);
    fixture.sign_in("b").await;
    let no_work_after = fixture.server.received_requests().await.unwrap().len();
    assert_authentication(a.sync().await);
    assert_authentication(a.pin("cold-item").await);
    assert!(matches!(
        a.drain_uploads().await,
        Err(EngineError::Cloud(CloudError::Authentication))
    ));
    assert_eq!(a.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    assert_eq!(a.pending_uploads().unwrap().len(), 1);
    let requests = fixture.server.received_requests().await.unwrap();
    assert!(
        requests[no_work_after..]
            .iter()
            .all(|request| { matches!(request.url.path(), "/oauth/token" | "/v1.0/me/drive") })
    );
    drop(a);

    let b = fixture.open().await;
    b.sync().await.unwrap();
    assert_root(&b, "root-b");
    assert!(b.pending_uploads().unwrap().is_empty());
    assert_eq!(b.item("shared-item").unwrap().unwrap().size, 0);
    drop(b);

    fixture.auth().logout().await.unwrap();
    let offline = fixture.open().await;
    assert_root(&offline, "root-b");
    assert_authentication(offline.sync().await);
    drop(offline);

    fixture.sign_in("a").await;
    let a = fixture.open().await;
    assert_root(&a, "root-a");
    assert_eq!(a.read(inode, 0, bytes.len()).await.unwrap(), bytes);
    assert_eq!(a.pending_uploads().unwrap().len(), 1);

    Mock::given(method("GET"))
        .and(path("/v1.0/me/drive/items/shared-item"))
        .respond_with(|request: &Request| {
            assert_eq!(account(request), "a");
            ResponseTemplate::new(200).set_body_json(file(
                "a",
                "shared-item",
                "draft.txt",
                0,
                "initial",
            ))
        })
        .mount(&fixture.server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/v1.0/me/drive/items/shared-item/content"))
        .respond_with(move |request: &Request| {
            assert_eq!(account(request), "a");
            assert_eq!(request.body, bytes);
            ResponseTemplate::new(200).set_body_json(file(
                "a",
                "shared-item",
                "draft.txt",
                bytes.len(),
                "updated",
            ))
        })
        .expect(1)
        .mount(&fixture.server)
        .await;
    assert_eq!(a.drain_uploads().await.unwrap(), 1);
    assert!(a.pending_uploads().unwrap().is_empty());
}

fn assert_root(engine: &Engine, id: &str) {
    let root = engine.root().unwrap().unwrap();
    assert_eq!((root.inode, root.item_id.as_str()), (1, id));
}

fn assert_authentication(result: Result<(), EngineError>) {
    assert!(
        matches!(result, Err(EngineError::Cloud(CloudError::Authentication))),
        "{result:?}"
    );
}

#[tokio::test]
async fn switch_during_upload_session_creation_blocks_signed_fragments_and_thumbnails() {
    let fixture = Fixture::new().await;
    fixture.sign_in("a").await;
    let graph = fixture.graph();
    let graph = graph.bind(fixture.graph().discover_drive().await.unwrap());
    let (notify, started) = oneshot::channel();
    let notify = Mutex::new(Some(notify));
    let url = fixture.server.uri() + "/a-signed";
    let signed = url.clone();
    Mock::given(method("POST"))
        .and(path("/v1.0/me/drive/items/shared-item/createUploadSession"))
        .respond_with(move |request: &Request| {
            assert_eq!(account(request), "a");
            notify.lock().unwrap().take().unwrap().send(()).unwrap();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(100))
                .set_body_json(json!({"uploadUrl":signed}))
        })
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(path("/a-signed"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&fixture.server)
        .await;

    let bytes = vec![0; 10 * 1024 * 1024 + 1];
    let upload = graph.upload(
        UploadTarget::Existing {
            item_id: "shared-item",
        },
        &bytes,
        None,
    );
    let switch = async {
        started.await.unwrap();
        fixture.auth().logout().await.unwrap();
        fixture.sign_in("b").await;
    };
    let (result, ()) = tokio::join!(upload, switch);
    assert_eq!(result, Err(CloudError::Authentication));
    assert_eq!(
        graph
            .download_thumbnail(&RemoteThumbnail {
                item_id: "shared-item".into(),
                url
            })
            .await,
        Err(CloudError::Authentication)
    );
}

#[tokio::test]
async fn adopts_only_a_verified_legacy_root_and_keeps_its_dirty_bytes() {
    let fixture = Fixture::new().await;
    drop(super::super::prepare_storage(&fixture.state, &fixture.cache).unwrap());
    let source = fixture.state.join(CATALOG);
    let orphan = fixture.data.join(CATALOG).with_added_extension("staging");
    fs::create_dir(&orphan).unwrap();
    fs::write(orphan.join("unidentified"), b"unknown contents").unwrap();
    fixture.sign_in("a").await;
    let graph = fixture.graph();
    let graph = graph.bind(fixture.graph().discover_drive().await.unwrap());
    let legacy = Engine::open_with_cache(
        &source,
        &fixture.cache,
        Arc::new(graph),
        Arc::new(SystemClock),
    )
    .unwrap();
    legacy.sync().await.unwrap();
    let draft = legacy.create(1, "legacy.txt", ItemKind::File).unwrap();
    let bytes = b"legacy dirty bytes";
    legacy.write(draft.inode, 0, bytes, false).await.unwrap();
    drop(legacy);
    fixture.auth().logout().await.unwrap();

    let offline = fixture.open().await;
    assert!(offline.root().unwrap().is_none());
    drop(offline);
    fixture.sign_in("b").await;
    let b = fixture.open().await;
    b.sync().await.unwrap();
    assert_root(&b, "root-b");
    assert!(b.pending_uploads().unwrap().is_empty());
    assert!(source.is_file());
    assert!(source.with_added_extension("staging").is_dir());
    drop(b);

    fixture.sign_in("a").await;
    let adopted = fixture.open().await;
    assert_root(&adopted, "root-a");
    assert_eq!(
        adopted.read(draft.inode, 0, bytes.len()).await.unwrap(),
        bytes
    );
    assert_eq!(adopted.pending_uploads().unwrap().len(), 1);
    assert!(!source.exists());
    assert_eq!(
        fs::read(orphan.join("unidentified")).unwrap(),
        b"unknown contents"
    );
}
