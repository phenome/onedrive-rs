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

CREATE TABLE IF NOT EXISTS sparse_namespace (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    name TEXT NOT NULL
);
-- Legacy paths were shared across catalogs. Their bytes cannot be attributed
-- to this catalog, so discard coverage when assigning the namespace.
DELETE FROM sparse_ranges WHERE NOT EXISTS (SELECT 1 FROM sparse_namespace);
INSERT OR IGNORE INTO sparse_namespace VALUES (1, lower(hex(randomblob(16))));

CREATE TABLE IF NOT EXISTS sparse_range_files (
    generation INTEGER NOT NULL,
    block INTEGER NOT NULL,
    stamp BLOB NOT NULL,
    PRIMARY KEY (generation, block),
    FOREIGN KEY (generation, block) REFERENCES sparse_ranges(generation, block) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sparse_pending (
    generation INTEGER NOT NULL REFERENCES sparse_files(generation) ON DELETE CASCADE,
    block INTEGER NOT NULL,
    PRIMARY KEY (generation, block)
);

CREATE TABLE IF NOT EXISTS sparse_retired (
    generation INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sparse_preserve_on_move (inode INTEGER PRIMARY KEY);
DROP TRIGGER IF EXISTS sparse_invalidate_on_content_change;
CREATE TRIGGER sparse_invalidate_on_content_change
AFTER UPDATE OF ctag, mtime, size, kind ON items
WHEN (OLD.ctag IS NOT NEW.ctag OR OLD.mtime IS NOT NEW.mtime
     OR OLD.size IS NOT NEW.size OR OLD.kind IS NOT NEW.kind)
     AND NOT EXISTS (SELECT 1 FROM sparse_preserve_on_move WHERE inode = NEW.inode)
BEGIN
    DELETE FROM sparse_files WHERE inode = NEW.inode;
END;

CREATE TRIGGER IF NOT EXISTS sparse_reclaim_on_retirement
AFTER DELETE ON sparse_files
BEGIN
    INSERT INTO sparse_retired VALUES (OLD.generation);
END;

-- A row records the last acknowledged cloud identity and the desired local
-- version. A NULL cloud ID means the item has yet to be created remotely.
CREATE TABLE IF NOT EXISTS writeback_queue (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    inode INTEGER NOT NULL UNIQUE REFERENCES items(inode) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 1,
    remote_id TEXT,
    remote_parent TEXT,
    remote_name TEXT,
    stage TEXT,
    deleted INTEGER NOT NULL DEFAULT 0,
    conflict INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS writeback_remote_namespace ON writeback_queue (remote_parent, remote_name);

CREATE TABLE IF NOT EXISTS writeback_recycled (
    item_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS local_attributes (
    inode INTEGER PRIMARY KEY REFERENCES items(inode) ON DELETE CASCADE,
    mode INTEGER,
    atime INTEGER,
    mtime INTEGER
);

-- The immutable request survives a crash between dispatch and acknowledgment.
CREATE TABLE IF NOT EXISTS writeback_attempts (
    inode INTEGER PRIMARY KEY REFERENCES writeback_queue(inode) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    remote_id TEXT,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    etag TEXT,
    stage TEXT,
    size INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS writeback_attempt_destination ON writeback_attempts (parent_id, name);

CREATE TABLE IF NOT EXISTS writeback_replacements (
    target_inode INTEGER PRIMARY KEY REFERENCES writeback_queue(inode) ON DELETE CASCADE,
    source_inode INTEGER REFERENCES items(inode) ON DELETE SET NULL,
    etag TEXT
);

CREATE TABLE IF NOT EXISTS writeback_temporary_names (
    name TEXT PRIMARY KEY,
    item_id TEXT NOT NULL
);

-- The stage remains available to reads and conflict recovery until publication.
CREATE TABLE IF NOT EXISTS writeback_publication (
    inode INTEGER PRIMARY KEY REFERENCES writeback_queue(inode) ON DELETE CASCADE,
    stage TEXT
);

-- Both regular moves and rename-cycle steps can succeed before a local acknowledgment.
CREATE TABLE IF NOT EXISTS writeback_moves (
    inode INTEGER PRIMARY KEY REFERENCES writeback_queue(inode) ON DELETE CASCADE,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    etag TEXT,
    ctag TEXT,
    temporary INTEGER NOT NULL
);

-- An immutable stage with missing ranges refers to one original cloud version.
CREATE TABLE IF NOT EXISTS writeback_bases (
    stage TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    etag TEXT,
    ctag TEXT,
    cache_generation INTEGER
);

CREATE TABLE IF NOT EXISTS writeback_missing_ranges (
    stage TEXT NOT NULL REFERENCES writeback_bases(stage) ON DELETE CASCADE,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    PRIMARY KEY (stage, start)
);

CREATE TABLE IF NOT EXISTS sparse_range_versions (
    generation INTEGER NOT NULL,
    block INTEGER NOT NULL,
    etag TEXT,
    ctag TEXT,
    PRIMARY KEY (generation, block),
    FOREIGN KEY (generation, block) REFERENCES sparse_ranges(generation, block) ON DELETE CASCADE
);


-- Delta pages can report an edge before the catalog can safely attach it.
CREATE TABLE IF NOT EXISTS deferred_remote_parents (
    inode INTEGER PRIMARY KEY REFERENCES items(inode) ON DELETE CASCADE,
    parent_id TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS supersede_deferred_parent_on_local_edit
AFTER INSERT ON writeback_queue
BEGIN
    DELETE FROM deferred_remote_parents WHERE inode = NEW.inode;
END;
