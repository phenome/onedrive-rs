use std::fs;

use onedrive_core::Config;
use tempfile::TempDir;

#[test]
fn missing_xdg_config_uses_defaults_and_partial_file_overrides_them() {
    let home = TempDir::new().unwrap();
    let config_home = home.path().join("xdg");
    let defaults = Config::load_from(Some(&config_home), home.path()).unwrap();
    assert_eq!(defaults.max_unpinned_cache_bytes, 1_073_741_824);
    assert_eq!(defaults.max_idle_age_days, 7);
    assert_eq!(defaults.min_free_disk_bytes, 5_368_709_120);
    assert_eq!(defaults.delta_poll_interval_secs, 60);
    assert_eq!(defaults.client_id, None);
    assert!(!config_home.exists());

    let app = config_home.join("onedrive-rs");
    fs::create_dir_all(&app).unwrap();
    fs::write(
        app.join("config.toml"),
        "delta_poll_interval_secs = 15\nclient_id = 'test-id'\n",
    )
    .unwrap();

    let config = Config::load_from(Some(&config_home), home.path()).unwrap();
    assert_eq!(config.delta_poll_interval_secs, 15);
    assert_eq!(config.client_id.as_deref(), Some("test-id"));
    assert_eq!(
        config.max_unpinned_cache_bytes,
        defaults.max_unpinned_cache_bytes
    );
    assert_eq!(config.max_idle_age_days, defaults.max_idle_age_days);
    assert_eq!(config.min_free_disk_bytes, defaults.min_free_disk_bytes);
}

#[test]
fn relative_xdg_config_home_falls_back_to_home_and_malformed_config_is_reported() {
    let home = TempDir::new().unwrap();
    let app = home.path().join(".config/onedrive-rs");
    fs::create_dir_all(&app).unwrap();
    let path = app.join("config.toml");
    fs::write(&path, "max_idle_age_days = 4\n").unwrap();
    let config = Config::load_from(Some("relative".as_ref()), home.path()).unwrap();
    assert_eq!(config.max_idle_age_days, 4);

    fs::write(path, "max_idle_age_days = 'bad'\n").unwrap();
    assert!(matches!(
        Config::load_from(None, home.path()),
        Err(onedrive_core::ConfigError::Parse { .. })
    ));
}
