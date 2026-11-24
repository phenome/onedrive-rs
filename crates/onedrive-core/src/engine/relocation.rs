use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use rustix::fs::{CWD, RenameFlags, renameat_with};

use super::{Engine, writeback::Writeback};

fn sqlite_companion(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn entry_present(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

impl Engine {
    /// Does the storage directory contain a catalog or a companion requiring recovery?
    pub fn catalog_storage_present(database: &Path) -> io::Result<bool> {
        Ok(entry_present(database)?
            || entry_present(&sqlite_companion(database, "-wal"))?
            || entry_present(&sqlite_companion(database, "-shm"))?
            || entry_present(&Writeback::staging_directory(database))?)
    }

    /// Relocate an offline catalog on the same filesystem. The caller owns
    /// exclusive locks on both storage directories for the entire operation.
    pub fn relocate_catalog(source: &Path, destination: &Path) -> io::Result<()> {
        let source_stage = Writeback::staging_directory(source);
        let destination_stage = Writeback::staging_directory(destination);
        if !entry_present(source)? {
            if Self::catalog_storage_present(source)? {
                return Err(io::Error::other(
                    "legacy catalog missing beside its WAL or staged files",
                ));
            }
            return Ok(());
        }
        if !fs::symlink_metadata(source)?.is_file() {
            return Err(io::Error::other("legacy catalog must be a regular file"));
        }
        if entry_present(destination)?
            || entry_present(&sqlite_companion(destination, "-wal"))?
            || entry_present(&sqlite_companion(destination, "-shm"))?
        {
            return Err(io::Error::other(
                "catalog files exist in both storage directories",
            ));
        }
        Self::checkpoint_for_relocation(source).map_err(io::Error::other)?;
        let directory = |path: &Path| {
            File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
        };
        let from = directory(source)?;
        let to = directory(destination)?;
        let move_entry = |old: &Path, new: &Path| -> io::Result<()> {
            renameat_with(CWD, old, CWD, new, RenameFlags::NOREPLACE)?;
            to.sync_all()?;
            from.sync_all()
        };
        // Move durable bytes first. A restart between these operations finds
        // the stage at the destination and finishes by publishing the catalog.
        if entry_present(&source_stage)? {
            move_entry(&source_stage, &destination_stage)?;
        }
        move_entry(source, destination)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn dangling_destination_preserves_catalog_and_dirty_staging() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("old.sqlite");
        let destination = dir.path().join("new.sqlite");
        rusqlite::Connection::open(&source).unwrap();
        let stage = Writeback::staging_directory(&source);
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("edit"), b"offline").unwrap();
        symlink(dir.path().join("absent"), &destination).unwrap();
        assert!(Engine::relocate_catalog(&source, &destination).is_err());
        assert!(source.is_file());
        assert_eq!(fs::read(stage.join("edit")).unwrap(), b"offline");
        assert!(!Writeback::staging_directory(&destination).exists());
    }
}
