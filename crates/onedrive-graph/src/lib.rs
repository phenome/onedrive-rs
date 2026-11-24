use std::time::Duration;

mod auth;
mod graph;

pub use auth::{Auth, AuthError, FakeTokenStore, SecretServiceStore, TokenStore};
pub use graph::GraphDrive;

const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    http_client_builder().build()
}

fn http_client_builder() -> reqwest::ClientBuilder {
    base_http_client_builder().timeout(HTTP_TIMEOUT)
}

fn base_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .connect_timeout(Duration::from_secs(15))
}
