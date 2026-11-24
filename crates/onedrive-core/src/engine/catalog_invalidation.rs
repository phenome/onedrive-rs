use std::sync::Arc;

use rusqlite::Connection;

pub(crate) enum CatalogInvalidation {
    Inode { inode: u64, content: bool },
    Entry { parent: u64, name: String },
}

pub(super) type Observer = dyn Fn(&[CatalogInvalidation]) + Send + Sync;
pub(crate) type CatalogObserver = Arc<Observer>;

pub(super) fn install(db: &Connection) -> rusqlite::Result<()> {
    // Capture changes at the catalog boundary, including rows pruned at the
    // end of a full enumeration and old names lost during rename or move.
    db.execute_batch(
        "CREATE TEMP TABLE changed_inodes (inode INTEGER, content INTEGER);
         CREATE TEMP TABLE changed_entries (parent_id TEXT, name TEXT);
         CREATE TEMP TRIGGER catalog_added AFTER INSERT ON main.items BEGIN
             INSERT INTO changed_entries VALUES (NEW.parent_id, NEW.name);
         END;
         CREATE TEMP TRIGGER catalog_removed AFTER DELETE ON main.items BEGIN
             INSERT INTO changed_inodes VALUES (OLD.inode, 1);
             INSERT INTO changed_entries VALUES (OLD.parent_id, OLD.name);
         END;
         CREATE TEMP TRIGGER catalog_attributes
         AFTER UPDATE OF parent_id, name, kind, size, mtime, etag, ctag ON main.items
         WHEN OLD.parent_id IS NOT NEW.parent_id OR OLD.name IS NOT NEW.name
              OR OLD.kind IS NOT NEW.kind OR OLD.size IS NOT NEW.size
              OR OLD.mtime IS NOT NEW.mtime OR OLD.etag IS NOT NEW.etag
              OR OLD.ctag IS NOT NEW.ctag
         BEGIN
             INSERT INTO changed_inodes VALUES (
                 NEW.inode, OLD.kind IS NOT NEW.kind OR OLD.size IS NOT NEW.size
                            OR OLD.mtime IS NOT NEW.mtime OR OLD.ctag IS NOT NEW.ctag
             );
         END;
         CREATE TEMP TRIGGER catalog_moved AFTER UPDATE OF parent_id, name ON main.items
         WHEN OLD.parent_id IS NOT NEW.parent_id OR OLD.name IS NOT NEW.name
         BEGIN
             INSERT INTO changed_entries VALUES (OLD.parent_id, OLD.name);
             INSERT INTO changed_entries VALUES (NEW.parent_id, NEW.name);
         END;",
    )
}

pub(super) fn collect(db: &Connection) -> rusqlite::Result<Vec<CatalogInvalidation>> {
    let mut inodes = db.prepare("SELECT inode, max(content) FROM changed_inodes GROUP BY inode")?;
    let mut changes = inodes
        .query_map([], |row| {
            Ok(CatalogInvalidation::Inode {
                inode: row.get(0)?,
                content: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut entries = db.prepare(
        "SELECT DISTINCT items.inode, changed_entries.name
         FROM changed_entries JOIN items ON items.item_id = changed_entries.parent_id",
    )?;
    for entry in entries.query_map([], |row| {
        Ok(CatalogInvalidation::Entry {
            parent: row.get(0)?,
            name: row.get(1)?,
        })
    })? {
        changes.push(entry?);
    }
    Ok(changes)
}
