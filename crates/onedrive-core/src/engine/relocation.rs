use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
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

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn absolute_location(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("missing catalog filename"))?;
    Ok(parent(path).canonicalize()?.join(name))
}

fn check_relocation_stage(
    source: &Path,
    destination: &Path,
    source_stage: &Path,
    destination_stage: &Path,
    move_stage: bool,
) -> io::Result<()> {
    let map = io::Error::other;
    let db = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_WRITE).map_err(map)?;
    let table_exists = |table| {
        db.query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .map_err(map)
    };
    let destination = absolute_location(destination)?;
    let recorded = if table_exists("catalog_relocation")? {
        db.query_row("SELECT destination FROM catalog_relocation", [], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .optional()
        .map_err(map)?
    } else {
        None
    };
    let resuming = if let Some(recorded) = recorded {
        if recorded == destination.as_os_str().as_encoded_bytes() {
            true
        } else if recorded == absolute_location(source)?.as_os_str().as_encoded_bytes() {
            false // This catalog completed a previous relocation.
        } else {
            return Err(io::Error::other(
                "interrupted catalog relocation has a different destination",
            ));
        }
    } else {
        false
    };

    let stage = if move_stage {
        source_stage
    } else {
        destination_stage
    };
    let mut referenced = false;
    // These are the live references also used by writeback startup. Older
    // catalogs may predate either table.
    for table in ["writeback_queue", "writeback_attempts"] {
        if !table_exists(table)? {
            continue;
        }
        let mut select = db
            .prepare(&format!(
                "SELECT stage FROM {table} WHERE stage IS NOT NULL"
            ))
            .map_err(map)?;
        let mut rows = select.query([]).map_err(map)?;
        while let Some(row) = rows.next().map_err(map)? {
            if !referenced && !fs::symlink_metadata(stage)?.is_dir() {
                return Err(io::Error::other("writeback staging must be a directory"));
            }
            referenced = true;
            let name: String = row.get(0).map_err(map)?;
            let mut components = Path::new(&name).components();
            if !matches!(components.next(), Some(Component::Normal(_)))
                || components.next().is_some()
                || !fs::symlink_metadata(stage.join(&name))?.is_file()
            {
                return Err(io::Error::other("catalog stage file is invalid"));
            }
        }
    }
    if move_stage || resuming {
        if !fs::symlink_metadata(stage)?.is_dir() {
            return Err(io::Error::other("writeback staging must be a directory"));
        }
    } else if !referenced && entry_present(destination_stage)? {
        return Err(io::Error::other(
            "destination staging cannot be attributed to this catalog",
        ));
    }
    if !resuming && (move_stage || referenced) {
        // Bind the destination before the next rename, including legacy retries.
        db.pragma_update(None, "synchronous", "EXTRA")
            .map_err(map)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS catalog_relocation (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                destination BLOB NOT NULL
             )",
        )
        .map_err(map)?;
        db.execute(
            "INSERT OR REPLACE INTO catalog_relocation VALUES (1, ?1)",
            [destination.as_os_str().as_encoded_bytes()],
        )
        .map_err(map)?;
    }
    Ok(())
}

impl Engine {
    /// Does the storage directory contain a catalog or a companion requiring recovery?
    pub fn catalog_storage_present(database: &Path) -> io::Result<bool> {
        Ok(entry_present(database)?
            || entry_present(&sqlite_companion(database, "-wal"))?
            || entry_present(&sqlite_companion(database, "-shm"))?
            || entry_present(&Writeback::staging_directory(database))?)
    }

    /// Inspect an offline catalog without opening its cache or staging directory.
    pub fn stored_root_id(database: &Path) -> rusqlite::Result<Option<String>> {
        let db = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.query_row(
            "SELECT item_id FROM items WHERE inode = 1 AND parent_id IS NULL AND kind = 1",
            [],
            |row| row.get(0),
        )
        .optional()
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
            if entry_present(destination)? {
                File::open(parent(destination))?.sync_all()?;
                File::open(parent(source))?.sync_all()?;
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
        let from = File::open(parent(source))?;
        let to = File::open(parent(destination))?;
        let move_stage = entry_present(&source_stage)?;
        if move_stage && entry_present(&destination_stage)? {
            return Err(io::Error::other(
                "staged files exist in both storage directories",
            ));
        }
        check_relocation_stage(
            source,
            destination,
            &source_stage,
            &destination_stage,
            move_stage,
        )?;
        let move_entry = |old: &Path, new: &Path| -> io::Result<()> {
            renameat_with(CWD, old, CWD, new, RenameFlags::NOREPLACE)?;
            to.sync_all()?;
            from.sync_all()
        };
        // Move durable bytes first. A restart between these operations finds
        // the stage at the destination and finishes by publishing the catalog.
        if move_stage {
            move_entry(&source_stage, &destination_stage)?;
        } else if entry_present(&destination_stage)? {
            // An interrupted stage rename may have happened just before fsync.
            to.sync_all()?;
            from.sync_all()?;
        }
        move_entry(source, destination)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    use crate::{FakeCloudDrive, ItemKind, RemoteItem, SystemClock};
    use std::sync::Arc;

    #[tokio::test]
    async fn interrupted_stage_move_only_publishes_at_the_matching_destination() {
        for recorded in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("old.sqlite");
            let destination = dir.path().join("first.sqlite");
            let other = dir.path().join("second.sqlite");
            let source_stage = Writeback::staging_directory(&source);
            let destination_stage = Writeback::staging_directory(&destination);
            let other_stage = Writeback::staging_directory(&other);
            let drive = Arc::new(FakeCloudDrive::new(10));
            drive.upsert(RemoteItem {
                item_id: "root".into(),
                parent_id: None,
                name: Some("root".into()),
                kind: ItemKind::Folder,
                size: 0,
                mtime: 1,
                etag: None,
                ctag: None,
                quickxor_hash: None,
            });
            let open =
                |path: &Path| Engine::open(path, drive.clone(), Arc::new(SystemClock)).unwrap();
            let engine = open(&source);
            engine.sync().await.unwrap();
            let item = engine.create(1, "draft.txt", ItemKind::File).unwrap();
            let bytes = b"queued edit";
            engine.write(item.inode, 0, bytes, false).await.unwrap();
            drop(engine);
            Engine::checkpoint_for_relocation(&source).unwrap();
            if recorded {
                check_relocation_stage(
                    &source,
                    &destination,
                    &source_stage,
                    &destination_stage,
                    true,
                )
                .unwrap();
            }
            fs::rename(&source_stage, &destination_stage).unwrap();

            assert!(Engine::relocate_catalog(&source, &other).is_err());
            fs::create_dir(&other_stage).unwrap();
            fs::write(other_stage.join("unrelated"), b"keep").unwrap();
            assert!(Engine::relocate_catalog(&source, &other).is_err());
            assert!(source.is_file());
            assert!(!other.exists());
            assert_eq!(fs::read(other_stage.join("unrelated")).unwrap(), b"keep");

            let saved_stage = dir.path().join("saved-stage");
            fs::rename(&destination_stage, &saved_stage).unwrap();
            fs::create_dir(&destination_stage).unwrap();
            assert!(Engine::relocate_catalog(&source, &destination).is_err());
            assert!(source.is_file());
            assert!(!destination.exists());
            fs::remove_dir(&destination_stage).unwrap();
            fs::rename(saved_stage, &destination_stage).unwrap();

            Engine::relocate_catalog(&source, &destination).unwrap();
            Engine::relocate_catalog(&source, &destination).unwrap();
            assert!(!source.exists());
            let engine = open(&destination);
            assert_eq!(
                engine.pending_upload_paths().unwrap()[0].0,
                Path::new("/draft.txt")
            );
            assert_eq!(
                engine.read(item.inode, 0, bytes.len()).await.unwrap(),
                bytes
            );
            assert_eq!(engine.drain_uploads().await.unwrap(), 1);
        }
    }

    #[test]
    fn interrupted_empty_stage_still_requires_the_recorded_destination() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("old.sqlite");
        let destination = dir.path().join("new.sqlite");
        let other = dir.path().join("other.sqlite");
        let source_stage = Writeback::staging_directory(&source);
        let destination_stage = Writeback::staging_directory(&destination);
        Connection::open(&source).unwrap();
        fs::create_dir(&source_stage).unwrap();

        check_relocation_stage(
            &source,
            &destination,
            &source_stage,
            &destination_stage,
            true,
        )
        .unwrap();
        fs::rename(&source_stage, &destination_stage).unwrap();
        assert!(Engine::relocate_catalog(&source, &other).is_err());
        assert!(source.is_file());
        assert!(!other.exists());
        Engine::relocate_catalog(&source, &destination).unwrap();
        assert!(destination.is_file());
    }

    #[test]
    fn completed_retry_checks_both_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old");
        fs::create_dir(&old).unwrap();
        let source = old.join("catalog");
        let destination = dir.path().join("catalog");
        Connection::open(&source).unwrap();
        Engine::relocate_catalog(&source, &destination).unwrap();
        fs::remove_dir(&old).unwrap();
        assert!(Engine::relocate_catalog(&source, &destination).is_err());
        fs::create_dir(&old).unwrap();
        Engine::relocate_catalog(&source, &destination).unwrap();
    }

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
