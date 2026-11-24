use super::*;
use onedrive_core::{CloudDrive, DeltaRequest};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "/consumers/oauth2/v2.0/token";

fn form(request: &wiremock::Request) -> HashMap<String, String> {
    let mut url = Url::parse("http://localhost").unwrap();
    url.set_query(Some(std::str::from_utf8(&request.body).unwrap()));
    url.query_pairs().into_owned().collect()
}

fn mock_auth(server: &MockServer, store: Arc<dyn TokenStore>) -> Auth {
    Auth::with_authority(
        "test-client",
        Url::parse(&format!("{}/consumers/oauth2/v2.0/", server.uri())).unwrap(),
        store,
    )
    .unwrap()
}

#[tokio::test]
async fn pkce_login_checks_state_and_exchanges_code_from_real_loopback_listener() {
    let server = MockServer::start().await;
    let store = Arc::new(FakeTokenStore::default());
    let auth = mock_auth(&server, store.clone());
    assert_eq!(
        auth.bearer().await.unwrap_err().to_string(),
        "sign in to OneDrive first"
    );

    let authorization = Mock::given(path("/consumers/oauth2/v2.0/authorize"))
        .and(method("GET"))
        .respond_with(|request: &wiremock::Request| {
            let query: HashMap<_, _> = request.url.query_pairs().into_owned().collect();
            let mut callback = Url::parse(&query["redirect_uri"]).unwrap();
            callback
                .query_pairs_mut()
                .append_pair("state", &query["state"])
                .append_pair("code", "browser-code");
            ResponseTemplate::new(302).insert_header("Location", callback.as_str())
        })
        .expect(1)
        .mount_as_scoped(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .and(|request: &wiremock::Request| {
            let form = form(request);
            form.len() == 6
                && form["client_id"] == "test-client"
                && form["grant_type"] == "authorization_code"
                && form["code"] == "browser-code"
                && form["scope"] == "Files.ReadWrite offline_access"
        })
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"browser-access","refresh_token":"browser-refresh","expires_in":3600
        })))
        .expect(1)
        .mount(&server)
        .await;

    auth.login_with(|authorize| async move {
        let browser = crate::http_client().unwrap();
        let response = browser.get(authorize).send().await.unwrap();
        assert_eq!(response.status(), 302);
        let callback = Url::parse(response.headers()["location"].to_str().unwrap()).unwrap();
        let mut incorrect = callback.clone();
        incorrect.set_query(Some("state=wrong&code=untrusted"));
        assert_eq!(browser.get(incorrect).send().await.unwrap().status(), 400);
        assert_eq!(browser.get(callback).send().await.unwrap().status(), 200);
        // xdg-open can remain running if the browser was launched by this login.
        std::future::pending::<Result<(), AuthError>>().await
    })
    .await
    .unwrap();

    let requests = server.received_requests().await.unwrap();
    let auth_query: HashMap<_, _> = requests[0].url.query_pairs().into_owned().collect();
    let token_form = form(&requests[1]);
    let redirect = Url::parse(&auth_query["redirect_uri"]).unwrap();
    assert_eq!(redirect.scheme(), "http");
    assert_eq!(redirect.host_str(), Some("localhost"));
    assert!(redirect.port().is_some());
    assert_eq!(auth_query["client_id"], "test-client");
    assert_eq!(auth_query["scope"], "Files.ReadWrite offline_access");
    assert_eq!(auth_query["response_type"], "code");
    assert_eq!(auth_query["code_challenge_method"], "S256");
    assert_eq!(auth_query["redirect_uri"], token_form["redirect_uri"]);
    assert_eq!(token_form["code_verifier"].len(), 43);
    assert_eq!(
        URL_SAFE_NO_PAD.encode(Sha256::digest(token_form["code_verifier"].as_bytes())),
        auth_query["code_challenge"]
    );
    assert_eq!(
        store.load_refresh_token().await.unwrap().as_deref(),
        Some("browser-refresh")
    );
    assert_eq!(auth.bearer().await.unwrap(), "browser-access");
    drop(authorization);
}

#[tokio::test]
async fn concurrent_requests_share_refresh_and_restart_uses_rotated_token() {
    let server = MockServer::start().await;
    let store = Arc::new(FakeTokenStore::default());
    store.replace_refresh_token("first-refresh").await.unwrap();
    for (refresh, access, rotated) in [
        ("first-refresh", "first-access", "second-refresh"),
        ("second-refresh", "second-access", "third-refresh"),
    ] {
        Mock::given(method("POST"))
            .and(path(TOKEN))
            .and(move |request: &wiremock::Request| {
                let form = form(request);
                form.len() == 4
                    && form["refresh_token"] == refresh
                    && form["client_id"] == "test-client"
                    && form["grant_type"] == "refresh_token"
                    && form["scope"] == "Files.ReadWrite offline_access"
            })
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token":access,"refresh_token":rotated,"expires_in":3600
            })))
            .expect(1)
            .mount(&server)
            .await;
    }
    let auth = mock_auth(&server, store.clone());
    let (one, two) = tokio::join!(auth.bearer(), auth.bearer());
    assert_eq!(one.unwrap(), "first-access");
    assert_eq!(two.unwrap(), "first-access");
    assert_eq!(
        store.load_refresh_token().await.unwrap().as_deref(),
        Some("second-refresh")
    );
    let restarted = mock_auth(&server, store.clone());
    assert_eq!(restarted.bearer().await.unwrap(), "second-access");
    assert_eq!(
        store.load_refresh_token().await.unwrap().as_deref(),
        Some("third-refresh")
    );
}

#[tokio::test]
async fn rotated_refresh_token_must_be_persisted_before_access_is_used() {
    struct UnavailableStore;
    #[async_trait]
    impl TokenStore for UnavailableStore {
        async fn load_refresh_token(&self) -> Result<Option<String>, AuthError> {
            Ok(Some("original".into()))
        }
        async fn replace_refresh_token(&self, _: &str) -> Result<(), AuthError> {
            Err(AuthError::Store)
        }
    }

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"uncommitted","refresh_token":"rotated","expires_in":3600
        })))
        .expect(2)
        .mount(&server)
        .await;
    let auth = Arc::new(mock_auth(&server, Arc::new(UnavailableStore)));
    let drive = crate::GraphDrive::new(auth.clone()).unwrap();
    assert!(matches!(
        drive.delta(DeltaRequest::Initial).await,
        Err(onedrive_core::CloudError::Authentication)
    ));
    assert!(matches!(auth.bearer().await, Err(AuthError::Store)));
}

#[tokio::test]
#[ignore = "requires an isolated Secret Service session with an unlocked default collection"]
async fn secret_service_replaces_synthetic_refresh_token_across_store_instances() {
    let client_id = format!("synthetic-test-{}", random_token().unwrap());
    let store = SecretServiceStore::new(&client_id);
    assert_eq!(store.load_refresh_token().await.unwrap(), None);
    store
        .replace_refresh_token("first-synthetic-secret")
        .await
        .unwrap();
    store
        .replace_refresh_token("rotated-synthetic-secret")
        .await
        .unwrap();
    let reopened = SecretServiceStore::new(client_id);
    assert_eq!(
        reopened.load_refresh_token().await.unwrap().as_deref(),
        Some("rotated-synthetic-secret")
    );
}
