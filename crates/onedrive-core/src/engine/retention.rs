use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::sparse_cache::{HEADER_BYTES, STREAM_BYTES, allocate_generation, is_hydrated};
use super::{Engine, EngineError, ItemKind, PinState, SyncState, item_by_id, write_pin};

const SUBTREE: &str = "WITH RECURSIVE subtree(item_id) AS (
    SELECT item_id FROM items WHERE item_id = ?1
    UNION
    SELECT i.item_id FROM items i JOIN subtree ON i.parent_id = subtree.item_id
)";

impl Engine {
    /// Remove a Pin while retaining cached bytes according to the eviction policy.
    pub fn unpin(&self, item_id: &str) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = item_by_id(&tx, item_id)?.ok_or(EngineError::NotFound)?;
        write_pin(
            &tx,
            item_id,
            unpin_directive(&tx, item.parent_id.as_deref())?,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Retire all cached bytes and explicit pins in a clean subtree.
    pub fn purge(&self, item_id: &str) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = item_by_id(&tx, item_id)?.ok_or(EngineError::NotFound)?;
        let protected = tx
            .prepare_cached(&format!(
                "{SUBTREE} SELECT i.item_id, i.sync_state FROM items i JOIN subtree USING (item_id)
             WHERE i.sync_state <> 0 ORDER BY i.sync_state, i.inode LIMIT 1",
            ))?
            .query_row([item_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if let Some((item_id, state)) = protected {
            return Err(match state {
                SyncState::Conflict => EngineError::UnresolvedConflict { item_id },
                _ => EngineError::PendingUpload { item_id },
            });
        }

        let pin = unpin_directive(&tx, item.parent_id.as_deref())?;
        tx.execute(
            &format!(
                "{SUBTREE} DELETE FROM sparse_files WHERE inode IN (
                SELECT inode FROM items JOIN subtree USING (item_id)
            )"
            ),
            [item_id],
        )?;
        tx.execute(
            &format!("{SUBTREE} DELETE FROM pin_directives WHERE item_id IN (SELECT item_id FROM subtree)"),
            [item_id],
        )?;
        write_pin(&tx, item_id, pin)?;
        tx.commit()?;
        self.cache.reclaim(&db)
    }

    /// Persist a Pin (Sticky Pin for folders) and hydrate its eligible files.
    pub async fn pin(&self, item_id: &str) -> Result<(), EngineError> {
        if !self.set_pin_state(item_id, PinState::Pinned)? {
            return Err(EngineError::NotFound);
        }
        self.hydrate_pins(Some(item_id)).await?;
        self.evict()?;
        Ok(())
    }

    pub(super) async fn hydrate_pins(&self, root: Option<&str>) -> Result<(), EngineError> {
        let candidates = self
            .db()
            .prepare_cached(
                "WITH RECURSIVE pinned(item_id) AS (
                 SELECT i.item_id FROM items i JOIN pin_directives p USING (item_id)
                 WHERE p.pin_state = 1 AND (?1 IS NULL OR i.item_id = ?1)
                 UNION
                 SELECT i.item_id FROM items i JOIN pinned ON i.parent_id = pinned.item_id
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pin_directives WHERE item_id = i.item_id AND pin_state = 2
                 )
             )
             SELECT i.item_id FROM items i JOIN pinned USING (item_id)
             WHERE i.kind = 0 AND i.sync_state = 0 AND i.remote_missing = 0
             ORDER BY i.inode",
            )?
            .query_map([root], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for id in candidates {
            self.hydrate_pinned_file(&id).await?;
        }
        Ok(())
    }

    async fn hydrate_pinned_file(&self, item_id: &str) -> Result<(), EngineError> {
        let (item, generation) = {
            let mut db = self.db();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let Some(item) = item_by_id(&tx, item_id)? else {
                return Ok(());
            };
            if item.kind != ItemKind::File
                || item.sync_state != SyncState::Clean
                || effective_pin(&tx, item_id)? != PinState::Pinned
                || is_hydrated(&tx, &item)?
            {
                return Ok(());
            }
            let generation = allocate_generation(&tx, item.inode)?;
            tx.commit()?;
            (item, generation)
        };

        let mut output = vec![0; item.size.min(STREAM_BYTES) as usize];
        let mut offset = 0;
        while offset < item.size {
            let length = (item.size - offset).min(STREAM_BYTES) as usize;
            if !self
                .read_generation(
                    &item,
                    generation,
                    offset,
                    item.size <= HEADER_BYTES,
                    &mut output[..length],
                )
                .await?
            {
                break;
            }
            offset += length as u64;
        }
        Ok(())
    }

    /// Enforce the idle-age limit, then the unpinned cache budget.
    pub fn evict(&self) -> Result<(), EngineError> {
        let mut db = self.db();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let entries = cached_files(&tx)?;
        let mut unpinned: u64 = entries
            .iter()
            .filter(|file| !file.pinned)
            .map(|file| file.bytes)
            .sum();
        let now = self.access_time();
        let max_age_ms = self.config.max_idle_age_days.saturating_mul(86_400_000);
        for file in entries {
            if file.pinned || file.state != SyncState::Clean {
                continue;
            }
            let idle = now.saturating_sub(file.accessed_at).max(0) as u64;
            if idle > max_age_ms || unpinned > self.config.max_unpinned_cache_bytes {
                tx.execute("DELETE FROM sparse_files WHERE inode = ?1", [file.inode])?;
                unpinned -= file.bytes;
            }
        }
        tx.commit()?;
        self.cache.reclaim(&db)
    }

    pub(super) fn access_time(&self) -> i64 {
        i64::try_from(self.clock.now().as_millis()).unwrap_or(i64::MAX)
    }
}

pub(super) fn effective_pin(db: &Connection, item_id: &str) -> rusqlite::Result<PinState> {
    // Visit the item's ancestry from closest to farthest. The path guard also
    // handles a transient cycle in a partially applied remote move.
    let directive = db
        .query_row(
            "WITH RECURSIVE ancestors(inode, item_id, parent_id, depth, visited) AS (
             SELECT inode, item_id, parent_id, 0, '/' || inode || '/' FROM items WHERE item_id = ?1
             UNION ALL
             SELECT i.inode, i.item_id, i.parent_id, a.depth + 1, a.visited || i.inode || '/'
             FROM items i JOIN ancestors a ON i.item_id = a.parent_id
             WHERE instr(a.visited, '/' || i.inode || '/') = 0
         )
         SELECT p.pin_state FROM ancestors JOIN pin_directives p USING (item_id)
         ORDER BY depth LIMIT 1",
            [item_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(directive.unwrap_or(PinState::None))
}

struct CachedFile {
    inode: u64,
    accessed_at: i64,
    bytes: u64,
    state: SyncState,
    pinned: bool,
}

fn cached_files(db: &Connection) -> Result<Vec<CachedFile>, EngineError> {
    let mut stmt = db.prepare_cached(
        "SELECT i.inode, i.item_id, i.sync_state, coalesce(i.last_accessed_at, 0),
                sum(CASE
                    WHEN r.block >= 0 THEN min(?1, i.size - r.block)
                    WHEN NOT EXISTS (
                        SELECT 1 FROM sparse_ranges WHERE generation = f.generation AND block = 0
                    ) THEN min(?2, i.size)
                    ELSE 0 END)
         FROM sparse_ranges r
         JOIN sparse_files f USING (generation)
         JOIN items i USING (inode)
         GROUP BY f.generation
         ORDER BY coalesce(i.last_accessed_at, 0), i.inode",
    )?;
    let rows = stmt.query_map(params![STREAM_BYTES, HEADER_BYTES], |row| {
        Ok((
            row.get(0)?,
            row.get::<_, String>(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
        ))
    })?;
    rows.map(|row| {
        let (inode, id, state, accessed_at, bytes) = row?;
        Ok(CachedFile {
            inode,
            accessed_at,
            bytes,
            state,
            pinned: effective_pin(db, &id)? == PinState::Pinned,
        })
    })
    .collect()
}

fn unpin_directive(db: &Connection, parent_id: Option<&str>) -> rusqlite::Result<PinState> {
    if let Some(parent_id) = parent_id
        && effective_pin(db, parent_id)? == PinState::Pinned
    {
        return Ok(PinState::UnpinOverride);
    }
    Ok(PinState::None)
}
