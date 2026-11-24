use super::*;
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

    let mut browser_task = None;
    let login = auth.login_with(|authorize| {
        browser_task = Some(tokio::spawn(async move {
            let browser = crate::http_client().unwrap();
            let response = browser.get(authorize).send().await.unwrap();
            assert_eq!(response.status(), 302);
            let callback = Url::parse(response.headers()["location"].to_str().unwrap()).unwrap();
            let mut incorrect = callback.clone();
            incorrect.set_query(Some("state=wrong&code=untrusted"));
            assert_eq!(browser.get(incorrect).send().await.unwrap().status(), 400);
            assert_eq!(browser.get(callback).send().await.unwrap().status(), 200);
        }));
        // xdg-open can remain running if the browser was launched by this login.
        std::future::pending::<Result<(), AuthError>>()
    });
    timeout(Duration::from_secs(5), login)
        .await
        .unwrap()
        .unwrap();
    browser_task.unwrap().await.unwrap();

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
    let auth = mock_auth(&server, Arc::new(UnavailableStore));
    for _ in 0..2 {
        assert!(matches!(auth.bearer().await, Err(AuthError::Store)));
    }
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

#[tokio::test]
async fn refresh_outage_and_rejected_grant_have_distinct_graph_errors() {
    use onedrive_core::{CloudDrive, CloudError, DeltaRequest};

    for (status, expected) in [
        (503, CloudError::Unavailable),
        (429, CloudError::Unavailable),
        (400, CloudError::Authentication),
    ] {
        let server = MockServer::start().await;
        let store = Arc::new(FakeTokenStore::default());
        store.replace_refresh_token("refresh").await.unwrap();
        Mock::given(path(TOKEN))
            .respond_with(ResponseTemplate::new(status))
            .expect(1)
            .mount(&server)
            .await;
        let graph = crate::GraphDrive::new(Arc::new(mock_auth(&server, store))).unwrap();
        assert_eq!(
            graph.delta(DeltaRequest::Initial).await.err(),
            Some(expected)
        );
    }
}

#[tokio::test]
async fn production_xdg_open_runs_in_isolated_process() {
    use std::os::unix::fs::PermissionsExt;

    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let fixture = Fixture(
        std::env::temp_dir().join(format!("onedrive-xdg-open-{}", random_token().unwrap())),
    );
    std::fs::create_dir(&fixture.0).unwrap();
    let executable = fixture.0.join("xdg-open");
    std::fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" > \"$ONEDRIVE_XDG_OPEN_ARGS\"\nexit \"$ONEDRIVE_XDG_OPEN_STATUS\"\n",
    )
    .unwrap();
    std::fs::set_permissions(executable, std::fs::Permissions::from_mode(0o700)).unwrap();

    for (mode, status) in [("callback", "0"), ("failure", "37"), ("timeout", "0")] {
        let output = timeout(
            Duration::from_secs(10),
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auth::tests::xdg_open_fixture_worker",
                    "--nocapture",
                ])
                .env("PATH", &fixture.0)
                .env("ONEDRIVE_XDG_OPEN_MODE", mode)
                .env("ONEDRIVE_XDG_OPEN_ARGS", fixture.0.join(mode))
                .env("ONEDRIVE_XDG_OPEN_STATUS", status)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("fixture subprocess should finish promptly")
        .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "{mode}: {stdout}{}",
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[tokio::test]
async fn xdg_open_fixture_worker() {
    let Ok(mode) = std::env::var("ONEDRIVE_XDG_OPEN_MODE") else {
        return;
    };
    let args = std::env::var_os("ONEDRIVE_XDG_OPEN_ARGS").unwrap();
    let server = MockServer::start().await;
    let store = Arc::new(FakeTokenStore::default());
    let auth = mock_auth(&server, store.clone());
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .and(|request: &wiremock::Request| form(request)["code"] == "fixture-code")
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"fixture-access","refresh_token":"fixture-refresh","expires_in":3600
        })))
        .expect(u64::from(mode == "callback"))
        .mount(&server)
        .await;

    let check_browser = async {
        let authorize = loop {
            if let Ok(contents) = std::fs::read_to_string(&args)
                && let Some((count, url)) = contents.trim_end().split_once('\n')
            {
                assert_eq!(count, "1");
                break Url::parse(url).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(
            authorize.as_str().split('?').next().unwrap(),
            format!("{}/consumers/oauth2/v2.0/authorize", server.uri()),
        );
        let query: HashMap<_, _> = authorize.query_pairs().into_owned().collect();
        assert_eq!(query["client_id"], "test-client");
        assert_eq!(query["scope"], SCOPES);
        assert_eq!(query["code_challenge_method"], "S256");
        let mut redirect = Url::parse(&query["redirect_uri"]).unwrap();
        assert_eq!(redirect.scheme(), "http");
        assert_eq!(redirect.host_str(), Some("localhost"));
        let port = redirect.port().unwrap();

        if mode == "callback" {
            redirect
                .query_pairs_mut()
                .append_pair("state", &query["state"])
                .append_pair("code", "fixture-code");
            let response = crate::http_client()
                .unwrap()
                .get(redirect)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        } else if mode == "timeout" {
            tokio::time::pause();
            tokio::time::advance(LOGIN_TIMEOUT + Duration::from_secs(1)).await;
        }
        port
    };

    let (result, port) = tokio::join!(auth.login(), check_browser);
    match mode.as_str() {
        "callback" => {
            result.unwrap();
            assert_eq!(
                store.load_refresh_token().await.unwrap().as_deref(),
                Some("fixture-refresh")
            );
        }
        "failure" => assert!(matches!(result, Err(AuthError::Browser)), "{result:?}"),
        "timeout" => assert!(matches!(result, Err(AuthError::Timeout)), "{result:?}"),
        _ => unreachable!(),
    }
    assert!(
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn logout_invalidates_cached_access_in_other_auth_instances() {
    let server = MockServer::start().await;
    let store = Arc::new(FakeTokenStore::default());
    store
        .replace_refresh_token("synthetic-refresh")
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path(TOKEN))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"synthetic-access","expires_in":3600
        })))
        .expect(1)
        .mount(&server)
        .await;
    let daemon = mock_auth(&server, store.clone());
    let cli = mock_auth(&server, store);
    assert_eq!(daemon.bearer().await.unwrap(), "synthetic-access");
    cli.logout().await.unwrap();
    assert!(!cli.is_signed_in().await.unwrap());
    assert!(matches!(
        daemon.bearer().await,
        Err(AuthError::LoginRequired)
    ));
    assert!(daemon.access.lock().await.is_none());
}
