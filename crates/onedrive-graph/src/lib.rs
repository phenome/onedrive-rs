mod auth;
mod graph;

pub use auth::{Auth, AuthError, FakeTokenStore, SecretServiceStore, TokenStore};
pub use graph::GraphDrive;

fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(120))
        .build()
}
