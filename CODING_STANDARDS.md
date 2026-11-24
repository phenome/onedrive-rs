# Coding standards

## Boundaries

- Keep domain behavior and SQLite ownership in `Engine`. FUSE, IPC, and CLI adapters translate their protocols into Engine operations.
- Put remote operations behind `CloudDrive` as they are needed. The Graph adapter owns HTTP, authentication, status mapping, and the shared request pause for Graph traffic. Pass continuation URLs through verbatim and keep them out of logs.
- Use stable OneDrive item IDs and parent IDs to walk the metadata tree. Use catalog inodes in FUSE. The catalog reserves inode 1 for the drive root.
- Commit a delta page and its checkpoint in the same SQLite transaction. Wait for the final page before removing deleted folders or pruning items absent from a full enumeration. Keep explicit pin directives by item ID across remote deletion and preserve local changes with their parent chains.
- Test Engine behavior through its public API with `FakeCloudDrive`, real SQLite and temporary directories, and a deterministic `Clock` when time affects the outcome. Exercise protocol details at the adapter boundary.

## Required checks

Run these from the workspace root before handing off a change.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Live FUSE safety

- Mount only in a dedicated temporary directory outside `$HOME`.
- Wrap every mount-touching command in `timeout`, including unmount and any cleanup that traverses the mountpoint.
- Set up cleanup before mounting. Unmount with `fusermount3 -u` and remove the test's synthetic thumbnail cache files and temporary files.
- Track the daemon by PID or its spawned process handle. Never use `pkill -f`.
