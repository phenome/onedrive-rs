//! Fake cloud worker for the isolated Nautilus retention integration test.
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, Config, DeltaPage, DeltaRequest, FakeCloudDrive, ItemKind, RemoteItem,
    RemoteThumbnail,
};

struct DesktopCloud {
    fake: FakeCloudDrive,
    reads: Mutex<fs::File>,
}

#[async_trait]
impl CloudDrive for DesktopCloud {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.fake.delta(request).await
    }

    async fn list_thumbnails(&self, folder: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        self.fake.list_thumbnails(folder).await
    }

    async fn download_thumbnail(&self, thumb: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        self.fake.download_thumbnail(thumb).await
    }

    async fn read_range(
        &self,
        id: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, CloudError> {
        let line = serde_json::to_string(&(id, offset, length)).unwrap();
        writeln!(self.reads.lock().unwrap(), "{line}").unwrap();
        self.fake.read_range(id, offset, length).await
    }
}

#[test]
#[ignore = "requires FUSE3, Nautilus 51, Xvfb, nautilus-python and ImageMagick"]
fn live_nautilus_retention() {
    let runner =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../desktop/nautilus/tests/retention.sh");
    let status = Command::new("timeout")
        .args(["-k", "10s", "55s", "bash"])
        .arg(runner)
        .arg(env::current_exe().unwrap())
        .arg(env!("CARGO_BIN_EXE_onedrive-rs"))
        .status()
        .unwrap();
    assert!(
        status.success(),
        "Nautilus retention fixture exited with {status}"
    );
}

#[test]
fn desktop_fixture_worker() {
    let Some(root) = std::env::var_os("ONEDRIVE_DESKTOP_TEST").map(PathBuf::from) else {
        return;
    };
    let fake = FakeCloudDrive::new(10);
    let folder = RemoteItem {
        item_id: "root".into(),
        parent_id: None,
        name: Some("root".into()),
        kind: ItemKind::Folder,
        size: 0,
        mtime: 1_700_000_000,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    };
    fake.upsert(folder.clone());
    fake.upsert(RemoteItem {
        item_id: "media".into(),
        parent_id: Some("root".into()),
        name: Some("media".into()),
        ..folder.clone()
    });
    let preview = fs::read(root.join("fixture.png")).unwrap();
    let mut original = preview.clone();
    original.resize(12 * 1024 * 1024, 0);
    for name in ["café; #%.png", "failed.png"] {
        fake.upsert(RemoteItem {
            item_id: name.into(),
            parent_id: Some("media".into()),
            name: Some(name.into()),
            kind: ItemKind::File,
            size: original.len() as u64,
            ..folder.clone()
        });
        fake.set_content(name, original.clone());
    }
    fake.set_thumbnail("café; #%.png", preview);
    let cloud = DesktopCloud {
        fake,
        reads: Mutex::new(
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(root.join("reads.jsonl"))
                .unwrap(),
        ),
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(onedrive_rs::daemon::run(
            Config::load().unwrap(),
            None,
            Arc::new(cloud),
        ))
        .unwrap();
}
