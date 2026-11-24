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

impl Engine {
    /// Relocate an offline catalog on the same filesystem. The caller owns
    /// exclusive locks on both storage directories for the entire operation.
    pub fn relocate_catalog(source: &Path, destination: &Path) -> io::Result<()> {
        let source_stage = Writeback::staging_directory(source);
        let destination_stage = Writeback::staging_directory(destination);
        if !source.try_exists()? {
            if sqlite_companion(source, "-wal").try_exists()? || source_stage.try_exists()? {
                return Err(io::Error::other(
                    "legacy catalog missing beside its WAL or staged files",
                ));
            }
            return Ok(());
        }
        if !fs::symlink_metadata(source)?.is_file() {
            return Err(io::Error::other("legacy catalog must be a regular file"));
        }
        if destination.try_exists()?
            || sqlite_companion(destination, "-wal").try_exists()?
            || sqlite_companion(destination, "-shm").try_exists()?
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
        if source_stage.try_exists()? {
            move_entry(&source_stage, &destination_stage)?;
        }
        move_entry(source, destination)
    }
}
