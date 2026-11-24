use std::fs::{self, DirBuilder, File};
use std::io::{self, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use onedrive_core::{Config, Engine, SystemClock};
use onedrive_graph::GraphDrive;
use tempfile::NamedTempFile;

use super::{lock_storage, validate_storage};

const CATALOG: &str = "catalog.sqlite3";
type StorageError = Box<dyn std::error::Error>;

/// The caller holds the data directory lock until the Engine is closed.
pub(super) async fn open(
    data: &Path,
    cache: &Path,
    legacy_state: Option<&Path>,
    cloud: GraphDrive,
    config: Config,
) -> Result<Engine, StorageError> {
    let marker = data.join("active-drive");
    let previous = selected_drive(&marker)?;
    let verified = match cloud.discover_drive().await {
        Ok(key) => Some(key),
        Err(error) => {
            eprintln!("Cannot verify the signed-in OneDrive ({error}). Starting offline.");
            None
        }
    };
    let selected = verified.as_deref().or(previous.as_deref());
    let drives = data.join("drives");
    ensure_private_directory(&drives)?;
    let directory = drives.join(selected.unwrap_or("signed-out"));
    ensure_private_directory(&directory)?;

    let database = directory.join(CATALOG);
    let cloud = match selected {
        Some(key) => cloud.bind(key.to_owned()),
        None => cloud,
    };

    if verified.is_some() {
        adopt_legacy(data, legacy_state, &database, &cloud).await?;
    } else {
        for source in legacy_catalogs(data, legacy_state)? {
            report_preserved(&source);
        }
    }
    if !database.try_exists()? && Engine::catalog_storage_present(&database)? {
        return Err(io::Error::other(format!(
            "OneDrive catalog missing beside its staging files or WAL at {database:?}"
        ))
        .into());
    }
    // A previous adoption may have renamed the catalog before a directory
    // sync failed. Discovery no longer sees that catalog at its legacy path.
    if database.try_exists()? {
        File::open(&directory)?.sync_all()?;
        File::open(data)?.sync_all()?;
        if let Some(state) = legacy_state.filter(|state| *state != data) {
            match File::open(state) {
                Ok(directory) => directory.sync_all()?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    // Sparse content already uses a persistent, randomly named namespace
    // belonging to the catalog. Reuse it when a verified catalog is moved.
    let engine = Engine::open_with_config(
        database,
        cache,
        Arc::new(cloud),
        Arc::new(SystemClock),
        config,
    )?;
    if let Some(key) = verified.as_deref().filter(|_| verified != previous) {
        File::open(&drives)?.sync_all()?;
        let mut staged = NamedTempFile::new_in(data)?;
        staged.write_all(key.as_bytes())?;
        staged.as_file().sync_all()?;
        staged.persist(&marker)?;
        File::open(data)?.sync_all()?;
    }
    Ok(engine)
}

fn ensure_private_directory(directory: &Path) -> io::Result<()> {
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    validate_storage(directory).map_err(io::Error::other)
}

fn selected_drive(marker: &Path) -> io::Result<Option<String>> {
    match fs::read_to_string(marker) {
        Ok(key)
            if key.len() == 64
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) =>
        {
            Ok(Some(key))
        }
        Ok(_) => Err(io::Error::other("invalid active OneDrive storage key")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn legacy_catalogs(data: &Path, legacy_state: Option<&Path>) -> io::Result<Vec<PathBuf>> {
    let mut sources = Vec::new();
    for directory in std::iter::once(data).chain(legacy_state) {
        let database = directory.join(CATALOG);
        // Also surface an orphaned stage or WAL instead of opening a new
        // catalog beside it and having writeback clean up unrecognized bytes.
        if Engine::catalog_storage_present(&database)? && !sources.contains(&database) {
            sources.push(database);
        }
    }
    Ok(sources)
}

async fn adopt_legacy(
    data: &Path,
    state: Option<&Path>,
    destination: &Path,
    cloud: &GraphDrive,
) -> Result<(), StorageError> {
    let sources = legacy_catalogs(data, state)?;
    if sources.is_empty() {
        return Ok(());
    }
    if destination.try_exists()? {
        for source in sources {
            report_preserved(&source);
        }
        return Ok(());
    }

    let mut identified = Vec::new();
    let mut seen = Vec::new();
    for source in sources {
        let parent = source.parent().expect("legacy catalog has a parent");
        let canonical = parent.canonicalize()?;
        if seen.contains(&canonical) {
            continue;
        }
        seen.push(canonical.clone());
        let result = (|| {
            let lock = if canonical == data.canonicalize()? {
                None
            } else {
                Some(lock_storage(parent).map_err(io::Error::other)?)
            };
            if !fs::symlink_metadata(&source)?.is_file() {
                return Err(io::Error::other("legacy catalog must be a regular file"));
            }
            let root = Engine::stored_root_id(&source).map_err(io::Error::other)?;
            Ok((root, lock))
        })();
        match result {
            Ok((Some(root), lock)) => identified.push((source, root, lock)),
            Ok((None, _)) => report_preserved(&source),
            Err(error) => {
                eprintln!("Cannot inspect legacy OneDrive data at {source:?} ({error}).");
                report_preserved(&source);
            }
        }
    }
    if identified.is_empty() {
        return Ok(());
    }
    let root = match cloud.root_id().await {
        Ok(root) => root,
        Err(error) => {
            for (source, _, _) in identified {
                report_preserved(&source);
            }
            return Err(error.into());
        }
    };
    let mut matching = Vec::new();
    for (source, stored_root, lock) in identified {
        if stored_root == root {
            matching.push((source, lock));
        } else {
            report_preserved(&source);
        }
    }
    if matching.len() == 1 {
        let (source, _legacy_lock) = matching.pop().unwrap();
        Engine::relocate_catalog(&source, destination)
            .map_err(|error| io::Error::other(format!("Cannot adopt {source:?} ({error})")))?;
    } else {
        for (source, _lock) in matching {
            report_preserved(&source);
        }
    }
    Ok(())
}

fn report_preserved(source: &Path) {
    eprintln!("Legacy OneDrive catalog and staging preserved at {source:?} for recovery.");
}

#[cfg(test)]
mod tests;
