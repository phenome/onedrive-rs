use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Config {
    pub max_unpinned_cache_bytes: u64,
    pub max_idle_age_days: u64,
    pub min_free_disk_bytes: u64,
    pub delta_poll_interval_secs: u64,
    pub mount_path: Option<PathBuf>,
    pub client_id: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_unpinned_cache_bytes: 1024 * 1024 * 1024,
            max_idle_age_days: 7,
            min_free_disk_bytes: 5 * 1024 * 1024 * 1024,
            delta_poll_interval_secs: 60,
            mount_path: None,
            client_id: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("HOME must be set to an absolute path when XDG_CONFIG_HOME is unavailable")]
    MissingHome,
    #[error("cannot read configuration at {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid configuration at {path:?}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config_home = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
        let home = env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
        Self::load_from(config_home.as_deref(), &home)
    }

    pub fn load_from(config_home: Option<&Path>, home: &Path) -> Result<Self, ConfigError> {
        let base = match config_home.filter(|path| path.is_absolute()) {
            Some(path) => path.to_path_buf(),
            None if home.is_absolute() => home.join(".config"),
            None => return Err(ConfigError::MissingHome),
        };
        let path = base.join("onedrive-rs/config.toml");
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(source) => return Err(ConfigError::Read { path, source }),
        };
        toml::from_str(&contents).map_err(|source| ConfigError::Parse { path, source })
    }
}
