use std::collections::BTreeSet;
use std::path::Path;

use rusqlite::{Connection, Rows, Transaction, params};

use crate::ItemKind;

const MAX_COMPONENT_BYTES: usize = 255;

pub(crate) fn migrate(transaction: &Transaction<'_>) -> rusqlite::Result<()> {
    let migrated: bool = transaction.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info('items') WHERE name = 'alias')",
        [],
        |row| row.get(0),
    )?;
    if migrated {
        return Ok(());
    }

    transaction.execute_batch(
        "ALTER TABLE items ADD COLUMN alias TEXT;
         ALTER TABLE items ADD COLUMN claim_revision INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE items ADD COLUMN claim_position INTEGER NOT NULL DEFAULT 0;
         CREATE INDEX items_parent_visible_name ON items (parent_id, coalesce(alias, name));
         CREATE INDEX items_parent_alias ON items (parent_id, alias) WHERE alias IS NOT NULL;",
    )?;
    let mut affected = AffectedNames::default();
    affected.extend(
        transaction,
        "SELECT parent_id, name FROM items WHERE parent_id IS NOT NULL
         GROUP BY parent_id, name HAVING count(*) > 1",
    )?;
    affected.reconcile(transaction, false)
}

#[derive(Default)]
pub(crate) struct AffectedNames(BTreeSet<(String, String)>);

impl AffectedNames {
    pub(crate) fn insert(&mut self, parent_id: Option<String>, name: String) {
        if let Some(parent_id) = parent_id {
            self.0.insert((parent_id, name));
        }
    }

    pub(crate) fn read(&mut self, rows: &mut Rows<'_>) -> rusqlite::Result<()> {
        while let Some(row) = rows.next()? {
            self.insert(row.get(0)?, row.get(1)?);
        }
        Ok(())
    }

    pub(crate) fn extend(&mut self, db: &Connection, sql: &str) -> rusqlite::Result<()> {
        self.read(&mut db.prepare(sql)?.query([])?)
    }

    pub(crate) fn record_item(&mut self, db: &Connection, item_id: &str) -> rusqlite::Result<()> {
        self.read(
            &mut db
                .prepare_cached("SELECT parent_id, name FROM items WHERE item_id = ?1")?
                .query([item_id])?,
        )
    }

    pub(crate) fn reconcile(mut self, db: &Connection, complete: bool) -> rusqlite::Result<()> {
        if complete {
            self.extend(
                db,
                "SELECT parent_id, name FROM items WHERE alias IS NOT NULL",
            )?;
        } else {
            // A newly claimed name can itself be a temporary alias for another item.
            let mut aliases = db.prepare_cached(
                "SELECT parent_id, name FROM items WHERE parent_id = ?1 AND alias = ?2",
            )?;
            let mut displaced = Self::default();
            for (parent_id, name) in &self.0 {
                displaced.read(&mut aliases.query(params![parent_id, name])?)?;
            }
            self.0.extend(displaced.0);
        }

        for (parent_id, name) in self.0 {
            reconcile_name(db, &parent_id, &name, complete)?;
        }
        Ok(())
    }
}

fn reconcile_name(
    db: &Connection,
    parent_id: &str,
    name: &str,
    complete: bool,
) -> rusqlite::Result<()> {
    // Keep tombstones addressable until final reconciliation, with live claims
    // taking precedence. Old catalogs have no claim order, so break live ties by inode.
    let mut claims = db.prepare_cached(
        "SELECT item_id, kind, alias, unavailable OR local
         FROM (
             SELECT item_id, kind, alias, inode, claim_revision, claim_position,
                    (remote_missing OR item_id IN (SELECT item_id FROM delta_deletions)) AS unavailable,
                    sync_state <> 0 AS local
             FROM items WHERE parent_id = ?1 AND name = ?2
               AND inode NOT IN (SELECT inode FROM writeback_queue WHERE deleted)
         )
         ORDER BY unavailable, local,
                  CASE WHEN NOT (unavailable OR local) THEN claim_revision END DESC,
                  CASE WHEN NOT (unavailable OR local) THEN claim_position END DESC,
                  CASE WHEN NOT (unavailable OR local) THEN inode END DESC, inode",
    )?;
    let entries = claims
        .query_map(params![parent_id, name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                if row.get(1)? {
                    ItemKind::Folder
                } else {
                    ItemKind::File
                },
                row.get::<_, Option<String>>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut set_alias = db.prepare_cached("UPDATE items SET alias = ?2 WHERE item_id = ?1")?;
    for (position, (item_id, kind, alias, local)) in entries.into_iter().enumerate() {
        if position == 0 {
            if alias.is_some() {
                set_alias.execute(params![item_id, Option::<String>::None])?;
            }
        } else if complete && local {
            let conflict = choose_conflict_name(db, parent_id, name, kind, "conflict")?;
            db.prepare_cached("UPDATE items SET name = ?2, alias = NULL WHERE item_id = ?1")?
                .execute(params![item_id, conflict])?;
        } else {
            let alias_claimed = match alias {
                Some(alias) => db
                    .prepare_cached(
                        "SELECT EXISTS (SELECT 1 FROM items WHERE parent_id = ?1 AND name = ?2)",
                    )?
                    .query_row(params![parent_id, alias], |row| row.get(0))?,
                None => true,
            };
            if alias_claimed {
                let alias = choose_conflict_name(db, parent_id, name, kind, "pending")?;
                set_alias.execute(params![item_id, alias])?;
            }
        }
    }
    Ok(())
}
fn choose_conflict_name(
    db: &Connection,
    parent_id: &str,
    name: &str,
    kind: ItemKind,
    label: &str,
) -> rusqlite::Result<String> {
    let (stem, extension) = match kind {
        ItemKind::File => Path::new(name)
            .extension()
            .and_then(|extension| extension.to_str())
            .map_or((name, ""), |extension| {
                name.split_at(name.len() - extension.len() - 1)
            }),
        ItemKind::Folder => (name, ""),
    };
    let mut exists = db.prepare_cached(
        "SELECT EXISTS (SELECT 1 FROM items WHERE parent_id = ?1 AND name = ?2)
              OR EXISTS (SELECT 1 FROM items WHERE parent_id = ?1 AND alias = ?2)",
    )?;
    let mut number = 1;
    loop {
        let suffix = if number == 1 {
            format!(" ({label})")
        } else {
            format!(" ({label} {number})")
        };
        let budget = MAX_COMPONENT_BYTES - suffix.len();
        let extension = &extension[..extension.floor_char_boundary(budget)];
        let stem = &stem[..stem.floor_char_boundary(budget - extension.len())];
        let candidate = format!("{stem}{suffix}{extension}");
        if !exists.query_row(params![parent_id, candidate], |row| row.get::<_, bool>(0))? {
            return Ok(candidate);
        }
        number += 1;
    }
}
