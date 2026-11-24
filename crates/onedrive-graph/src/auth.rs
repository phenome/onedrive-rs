use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::{Router, routing::get};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use onedrive_core::Config;
use reqwest::{Client, Url};
use secret_service::{EncryptionType, SecretService};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::{Mutex, oneshot};
use tokio::time::{Instant, sleep, timeout};
use zbus::Connection;
use zbus::fdo::{DBusProxy, RequestNameFlags, RequestNameReply};
use zbus::names::WellKnownName;

const AUTHORITY: &str = "https://login.microsoftonline.com/consumers/oauth2/v2.0/";
const SCOPES: &str = "Files.ReadWrite offline_access";
const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("configure a Microsoft Entra client_id")]
    MissingClientId,
    #[error("sign in to OneDrive first")]
    LoginRequired,
    #[error("desktop Secret Service is unavailable")]
    Store,
    #[error("could not initialize OAuth login")]
    Initialize,
    #[error("could not open the system browser")]
    Browser,
    #[error("could not receive the OAuth loopback callback")]
    Callback,
    #[error("OAuth login timed out")]
    Timeout,
    #[error("OAuth authorization was denied")]
    Denied,
    #[error("OAuth token endpoint is unavailable")]
    Transport,
    #[error("OAuth token endpoint rejected the grant")]
    GrantRejected,
    #[error("OAuth token endpoint returned invalid credentials")]
    InvalidToken,
}

/// The refresh token is persisted as a single secret. The access token stays in memory.
#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn load_refresh_token(&self) -> Result<Option<String>, AuthError>;
    async fn replace_refresh_token(&self, token: &str) -> Result<(), AuthError>;

    async fn clear_refresh_token(&self) -> Result<(), AuthError> {
        Err(AuthError::Store)
    }

    /// Return a store that holds exclusive access to this credential scope,
    /// when coordination outside this Auth instance is required.
    async fn begin_transaction(&self) -> Result<Option<Box<dyn TokenStore>>, AuthError> {
        Ok(None)
    }
}

pub struct SecretServiceStore {
    client_id: String,
    connection: Option<Connection>,
}

impl SecretServiceStore {
    pub fn new(client_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            connection: None,
        }
    }

    async fn service(&self) -> Result<SecretService<'_>, AuthError> {
        match &self.connection {
            Some(connection) => {
                SecretService::connect_with_existing(EncryptionType::Dh, connection.clone()).await
            }
            None => SecretService::connect(EncryptionType::Dh).await,
        }
        .map_err(|_| AuthError::Store)
    }

    fn attributes(&self) -> HashMap<&str, &str> {
        HashMap::from([
            ("application", "onedrive-rs"),
            ("client-id", self.client_id.as_str()),
        ])
    }
}

#[async_trait]
impl TokenStore for SecretServiceStore {
    async fn load_refresh_token(&self) -> Result<Option<String>, AuthError> {
        let service = self.service().await?;
        let collection = service
            .get_default_collection()
            .await
            .map_err(|_| AuthError::Store)?;
        let items = collection
            .search_items(self.attributes())
            .await
            .map_err(|_| AuthError::Store)?;
        let Some(item) = items.first() else {
            return Ok(None);
        };
        if item.is_locked().await.map_err(|_| AuthError::Store)? {
            item.unlock().await.map_err(|_| AuthError::Store)?;
        }
        let value = item.get_secret().await.map_err(|_| AuthError::Store)?;
        String::from_utf8(value)
            .map(Some)
            .map_err(|_| AuthError::Store)
    }

    async fn replace_refresh_token(&self, token: &str) -> Result<(), AuthError> {
        let service = self.service().await?;
        let collection = service
            .get_default_collection()
            .await
            .map_err(|_| AuthError::Store)?;
        if collection.is_locked().await.map_err(|_| AuthError::Store)? {
            collection.unlock().await.map_err(|_| AuthError::Store)?;
        }
        collection
            .create_item(
                "OneDrive refresh token",
                self.attributes(),
                token.as_bytes(),
                true,
                "text/plain",
            )
            .await
            .map_err(|_| AuthError::Store)?;
        Ok(())
    }

    async fn clear_refresh_token(&self) -> Result<(), AuthError> {
        let service = self.service().await?;
        let collection = service
            .get_default_collection()
            .await
            .map_err(|_| AuthError::Store)?;
        for item in collection
            .search_items(self.attributes())
            .await
            .map_err(|_| AuthError::Store)?
        {
            if item.is_locked().await.map_err(|_| AuthError::Store)? {
                item.unlock().await.map_err(|_| AuthError::Store)?;
            }
            item.delete().await.map_err(|_| AuthError::Store)?;
        }
        Ok(())
    }

    async fn begin_transaction(&self) -> Result<Option<Box<dyn TokenStore>>, AuthError> {
        // The bus releases this name when the owning connection closes, including
        // when a process dies during a refresh. Secret operations use that connection.
        let scope = format!("{:x}", Sha256::digest(self.client_id.as_bytes()));
        let name = format!("org.onedrive_rs.Auth.Client_{scope}");
        let name = WellKnownName::try_from(name).map_err(|_| AuthError::Store)?;
        let connection = Connection::session().await.map_err(|_| AuthError::Store)?;
        let bus = DBusProxy::new(&connection)
            .await
            .map_err(|_| AuthError::Store)?;
        timeout(Duration::from_secs(135), async {
            loop {
                let reply = bus
                    .request_name(name.clone(), RequestNameFlags::DoNotQueue.into())
                    .await
                    .map_err(|_| AuthError::Store)?;
                match reply {
                    RequestNameReply::PrimaryOwner => return Ok(()),
                    RequestNameReply::Exists => sleep(Duration::from_millis(25)).await,
                    _ => return Err(AuthError::Store),
                }
            }
        })
        .await
        .map_err(|_| AuthError::Store)??;
        Ok(Some(Box::new(Self {
            client_id: self.client_id.clone(),
            connection: Some(connection.clone()),
        })))
    }
}

#[derive(Default)]
pub struct FakeTokenStore {
    refresh_token: Mutex<Option<String>>,
}

#[async_trait]
impl TokenStore for FakeTokenStore {
    async fn load_refresh_token(&self) -> Result<Option<String>, AuthError> {
        Ok(self.refresh_token.lock().await.clone())
    }

    async fn replace_refresh_token(&self, token: &str) -> Result<(), AuthError> {
        *self.refresh_token.lock().await = Some(token.to_owned());
        Ok(())
    }

    async fn clear_refresh_token(&self) -> Result<(), AuthError> {
        *self.refresh_token.lock().await = None;
        Ok(())
    }
}

pub struct Auth {
    client_id: String,
    authority: Url,
    client: Client,
    store: Arc<dyn TokenStore>,
    // A single lock serializes login and refresh, including publishing rotated credentials.
    access: Mutex<Option<AccessToken>>,
}

struct AccessToken {
    value: String,
    refresh: String,
    fresh_until: Instant,
}

#[derive(Deserialize)]
struct Grant {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
}

impl Auth {
    /// Use this constructor for a real desktop sign-in and in the daemon.
    pub fn new(config: &Config) -> Result<Self, AuthError> {
        let client_id = config
            .client_id
            .as_deref()
            .or(option_env!("ONEDRIVE_CLIENT_ID"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(AuthError::MissingClientId)?;
        Self::with_authority(
            client_id,
            Url::parse(AUTHORITY).expect("constant authority URL"),
            Arc::new(SecretServiceStore::new(client_id)),
        )
    }

    fn with_authority(
        client_id: &str,
        authority: Url,
        store: Arc<dyn TokenStore>,
    ) -> Result<Self, AuthError> {
        Ok(Self {
            client_id: client_id.to_owned(),
            authority,
            client: crate::http_client().map_err(|_| AuthError::Initialize)?,
            store,
            access: Mutex::new(None),
        })
    }

    pub async fn login(&self) -> Result<(), AuthError> {
        self.login_with(open_system_browser).await
    }

    async fn login_with<F, Fut>(&self, launch_browser: F) -> Result<(), AuthError>
    where
        F: FnOnce(Url) -> Fut,
        Fut: Future<Output = Result<(), AuthError>>,
    {
        let mut access = self.access.lock().await;
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|_| AuthError::Callback)?;
        let port = listener
            .local_addr()
            .map_err(|_| AuthError::Callback)?
            .port();
        let redirect = format!("http://localhost:{port}");
        let verifier = random_token()?;
        let state = random_token()?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut authorize = self
            .authority
            .join("authorize")
            .map_err(|_| AuthError::Initialize)?;
        authorize.query_pairs_mut().extend_pairs([
            ("client_id", self.client_id.as_str()),
            ("response_type", "code"),
            ("response_mode", "query"),
            ("redirect_uri", &redirect),
            ("scope", SCOPES),
            ("code_challenge_method", "S256"),
            ("code_challenge", &challenge),
            ("state", &state),
        ]);

        let (send, receive) = oneshot::channel();
        let (shutdown, stop) = oneshot::channel();
        let router = Router::new()
            .route("/", get(callback))
            .with_state(Arc::new(Callback {
                state,
                host: format!("localhost:{port}"),
                send: Mutex::new(Some(send)),
            }));
        let serve = async {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stop.await;
                })
                .await
                .map_err(|_| AuthError::Callback)
        };
        let flow = async {
            let mut receive = receive;
            tokio::select! {
                result = &mut receive => result.map_err(|_| AuthError::Callback)?,
                launched = launch_browser(authorize) => {
                    launched?;
                    receive.await.map_err(|_| AuthError::Callback)?
                }
            }
        };
        let code = timeout(LOGIN_TIMEOUT, async {
            tokio::pin!(serve);
            let result = tokio::select! {
                completion = &mut serve => return completion.and(Err(AuthError::Callback)),
                result = flow => result,
            };
            let _ = shutdown.send(());
            // Let the browser finish receiving its reply even when the callback denied access.
            let _ = timeout(Duration::from_secs(1), &mut serve).await;
            result
        })
        .await
        .map_err(|_| AuthError::Timeout)??;

        let transaction = self.store.begin_transaction().await?;
        let store = transaction.as_deref().unwrap_or(self.store.as_ref());
        let grant = self
            .exchange(&[
                ("grant_type", "authorization_code"),
                ("code", &code),
                ("redirect_uri", &redirect),
                ("code_verifier", &verifier),
            ])
            .await?;
        *access = Some(Self::persist(store, grant, None).await?);
        Ok(())
    }

    pub async fn is_signed_in(&self) -> Result<bool, AuthError> {
        let transaction = self.store.begin_transaction().await?;
        let store = transaction.as_deref().unwrap_or(self.store.as_ref());
        Ok(store.load_refresh_token().await?.is_some())
    }

    pub async fn logout(&self) -> Result<(), AuthError> {
        let mut access = self.access.lock().await;
        *access = None;
        let transaction = self.store.begin_transaction().await?;
        let store = transaction.as_deref().unwrap_or(self.store.as_ref());
        store.clear_refresh_token().await
    }

    pub(crate) async fn bearer(&self) -> Result<String, AuthError> {
        let mut access = self.access.lock().await;
        let transaction = self.store.begin_transaction().await?;
        let store = transaction.as_deref().unwrap_or(self.store.as_ref());
        let refresh = match store.load_refresh_token().await {
            Ok(Some(refresh)) => refresh,
            result => {
                *access = None;
                return Err(result.err().unwrap_or(AuthError::LoginRequired));
            }
        };
        if let Some(token) = access
            .as_ref()
            .filter(|token| token.refresh == refresh && Instant::now() < token.fresh_until)
        {
            return Ok(token.value.clone());
        }
        *access = None;
        let grant = self
            .exchange(&[("grant_type", "refresh_token"), ("refresh_token", &refresh)])
            .await?;
        let token = Self::persist(store, grant, Some(refresh)).await?;
        let value = token.value.clone();
        *access = Some(token);
        Ok(value)
    }

    async fn exchange(&self, params: &[(&str, &str)]) -> Result<Grant, AuthError> {
        let url = self
            .authority
            .join("token")
            .map_err(|_| AuthError::Initialize)?;
        let mut form = vec![("client_id", self.client_id.as_str()), ("scope", SCOPES)];
        form.extend_from_slice(params);
        let response = self
            .client
            .post(url)
            .form(&form)
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        let status = response.status();
        if status.is_server_error()
            || matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
            )
        {
            return Err(AuthError::Transport);
        }
        if !status.is_success() {
            return Err(AuthError::GrantRejected);
        }
        response.json().await.map_err(|_| AuthError::InvalidToken)
    }

    async fn persist(
        store: &dyn TokenStore,
        grant: Grant,
        previous_refresh: Option<String>,
    ) -> Result<AccessToken, AuthError> {
        if grant.access_token.is_empty() || grant.expires_in == 0 {
            return Err(AuthError::InvalidToken);
        }
        let lifetime = Duration::from_secs(grant.expires_in.saturating_sub(60).max(1));
        let fresh_until = Instant::now()
            .checked_add(lifetime)
            .ok_or(AuthError::InvalidToken)?;
        let refresh = match grant.refresh_token {
            Some(token) if !token.is_empty() => {
                store.replace_refresh_token(&token).await?;
                token
            }
            None => previous_refresh.ok_or(AuthError::InvalidToken)?,
            _ => return Err(AuthError::InvalidToken),
        };
        Ok(AccessToken {
            value: grant.access_token,
            refresh,
            fresh_until,
        })
    }

    #[cfg(test)]
    pub(crate) fn synthetic() -> Self {
        let mut auth = Self::with_authority(
            "synthetic-client",
            Url::parse(AUTHORITY).unwrap(),
            Arc::new(FakeTokenStore {
                refresh_token: Mutex::new(Some("synthetic-refresh".to_owned())),
            }),
        )
        .unwrap();
        auth.access = Mutex::new(Some(AccessToken {
            value: "synthetic-access".to_owned(),
            refresh: "synthetic-refresh".to_owned(),
            fresh_until: Instant::now() + Duration::from_secs(3600),
        }));
        auth
    }
}

struct Callback {
    state: String,
    host: String,
    send: Mutex<Option<oneshot::Sender<Result<String, AuthError>>>>,
}

#[derive(Deserialize)]
struct CallbackQuery {
    state: Option<String>,
    code: Option<String>,
    error: Option<String>,
}

async fn callback(
    State(callback): State<Arc<Callback>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<CallbackQuery>,
) -> (StatusCode, &'static str) {
    if headers
        .get(axum::http::header::HOST)
        .and_then(|host| host.to_str().ok())
        != Some(&callback.host)
        || query.state.as_deref() != Some(&callback.state)
    {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid OneDrive sign-in callback.",
        );
    }
    let result = match (query.code, query.error) {
        (Some(code), None) if !code.is_empty() => Ok(code),
        (None, Some(_)) => Err(AuthError::Denied),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "Invalid OneDrive sign-in callback.",
            );
        }
    };
    if let Some(send) = callback.send.lock().await.take() {
        let _ = send.send(result);
    }
    (
        StatusCode::OK,
        "OneDrive sign-in response received. Return to the application.",
    )
}

fn random_token() -> Result<String, AuthError> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| AuthError::Initialize)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

async fn open_system_browser(url: Url) -> Result<(), AuthError> {
    let status = Command::new("xdg-open")
        .arg(url.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map_err(|_| AuthError::Browser)?;
    status.success().then_some(()).ok_or(AuthError::Browser)
}

#[cfg(test)]
mod tests;
