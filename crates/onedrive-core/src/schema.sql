CREATE TABLE IF NOT EXISTS items (
    inode INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL UNIQUE,
    parent_id TEXT,
    name TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (0, 1)),
    size INTEGER NOT NULL CHECK (size >= 0),
    mtime INTEGER NOT NULL,
    etag TEXT,
    ctag TEXT,
    quickxor_hash TEXT,
    sync_state INTEGER NOT NULL DEFAULT 0 CHECK (sync_state IN (0, 1, 2, 3)),
    remote_missing INTEGER NOT NULL DEFAULT 0 CHECK (remote_missing IN (0, 1)),
    last_accessed_at INTEGER
);
-- Reserve inode 1 for the drive root before any page can insert a child.
INSERT INTO sqlite_sequence(name, seq)
SELECT 'items', 1 WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'items');

CREATE INDEX IF NOT EXISTS items_remote_missing ON items (remote_missing) WHERE remote_missing = 1;
CREATE INDEX IF NOT EXISTS items_parent_name ON items (parent_id, name);
CREATE INDEX IF NOT EXISTS items_local_changes ON items (sync_state) WHERE sync_state <> 0;

CREATE TABLE IF NOT EXISTS pin_directives (
    item_id TEXT PRIMARY KEY,
    pin_state INTEGER NOT NULL CHECK (pin_state IN (1, 2))
);

CREATE TABLE IF NOT EXISTS delta_checkpoint (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    link TEXT,
    full_scan INTEGER NOT NULL DEFAULT 1 CHECK (full_scan IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 0,
    retry_at_ms INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO delta_checkpoint (singleton) VALUES (1);

CREATE TABLE IF NOT EXISTS delta_deletions (
    item_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS full_sync_seen (
    item_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sparse_files (
    generation INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL UNIQUE REFERENCES items(inode) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sparse_ranges (
    generation INTEGER NOT NULL REFERENCES sparse_files(generation) ON DELETE CASCADE,
    block INTEGER NOT NULL,
    PRIMARY KEY (generation, block)
);

CREATE TABLE IF NOT EXISTS sparse_retired (
    generation INTEGER PRIMARY KEY
);

CREATE TRIGGER IF NOT EXISTS sparse_invalidate_on_content_change
AFTER UPDATE OF ctag, mtime, size, kind ON items
WHEN OLD.ctag IS NOT NEW.ctag OR OLD.mtime IS NOT NEW.mtime
     OR OLD.size IS NOT NEW.size OR OLD.kind IS NOT NEW.kind
BEGIN
    DELETE FROM sparse_files WHERE inode = NEW.inode;
END;

CREATE TRIGGER IF NOT EXISTS sparse_reclaim_on_retirement
AFTER DELETE ON sparse_files
BEGIN
    INSERT INTO sparse_retired VALUES (OLD.generation);
END;
