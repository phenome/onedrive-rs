use std::fs::{self, File};
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, DeltaPage, DeltaRequest, Engine, EngineError, FakeCloudDrive, ItemKind,
    RemoteItem, RemoteThumbnail, SystemClock,
};
use tokio::sync::Notify;

const MTIME: i64 = 1_700_000_001;
const MOUNT: &str = "/tmp/OneDrive #4";

fn item(id: &str, parent: Option<&str>, name: &str, kind: ItemKind) -> RemoteItem {
    RemoteItem {
        item_id: id.into(),
        parent_id: parent.map(str::to_owned),
        name: Some(name.into()),
        kind,
        size: 12 * 1024 * 1024,
        mtime: MTIME,
        etag: None,
        ctag: None,
        quickxor_hash: None,
    }
}

fn thumbnail(format: image::ImageFormat) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image::RgbImage::from_pixel(800, 400, image::Rgb([200, 40, 10]))
        .write_to(&mut bytes, format)
        .unwrap();
    bytes.into_inner()
}

struct Fixture {
    dir: tempfile::TempDir,
    drive: Arc<FakeCloudDrive>,
    engine: Engine,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let drive = Arc::new(FakeCloudDrive::new(100));
        drive.upsert(item("root", None, "root", ItemKind::Folder));
        let engine =
            Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
        Self { dir, drive, engine }
    }

    fn cache(&self) -> PathBuf {
        self.dir.path().join("thumbnails")
    }

    async fn readdir(&self, folder: &str) {
        self.engine
            .readdir(folder, Path::new(MOUNT), &self.cache())
            .await
            .unwrap();
    }
}

fn assert_png(root: &Path, uri: &str, mtime: i64, fail: bool) -> PathBuf {
    let filename = format!("{:x}.png", md5::compute(uri));
    let dir = if fail {
        "fail/gnome-thumbnail-factory"
    } else {
        "x-large"
    };
    let path = root.join(dir).join(filename);
    let mut png = png::Decoder::new(BufReader::new(File::open(&path).unwrap()))
        .read_info()
        .unwrap();
    // read_info stops before image data, so these assertions also check chunk order.
    let metadata = &png.info().uncompressed_latin1_text;
    for (key, value) in [("Thumb::URI", uri), ("Thumb::MTime", &mtime.to_string())] {
        assert!(
            metadata
                .iter()
                .any(|text| text.keyword == key && text.text == value)
        );
    }
    let mut bytes = vec![0; png.output_buffer_size().unwrap()];
    let frame = png.next_frame(&mut bytes).unwrap();
    if fail {
        assert_eq!(
            (frame.width, frame.height, frame.color_type),
            (1, 1, png::ColorType::Rgba)
        );
        assert_eq!(bytes, [0, 0, 0, 0]);
    } else {
        assert_eq!((frame.width, frame.height), (512, 256));
    }
    path
}

#[tokio::test]
async fn batches_jpeg_and_png_and_seeds_fail_markers_before_returning_entries() {
    let fixture = Fixture::new();
    for (id, name, format) in [
        ("jpeg", "café #?%.jpg", Some(image::ImageFormat::Jpeg)),
        ("png", "image.PNG", Some(image::ImageFormat::Png)),
        ("pdf", "manual.pdf", None),
        ("video", "movie.MOV", None),
        ("office", "slides.pptx", None),
    ] {
        fixture
            .drive
            .upsert(item(id, Some("root"), name, ItemKind::File));
        if let Some(format) = format {
            fixture.drive.set_thumbnail(id, thumbnail(format));
        }
    }
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    let prefix = "file:///tmp/OneDrive%20%234/";
    for (name, fail) in [
        ("caf%C3%A9%20%23%3F%25.jpg", false),
        ("image.PNG", false),
        ("manual.pdf", true),
        ("movie.MOV", true),
        ("slides.pptx", true),
    ] {
        assert_png(&fixture.cache(), &format!("{prefix}{name}"), MTIME, fail);
    }
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root"]);
    let mut downloads = fixture.drive.thumbnail_download_requests();
    downloads.sort();
    assert_eq!(downloads, ["jpeg", "png"]);
    assert!(fixture.drive.range_requests().is_empty());
}

#[tokio::test]
async fn non_media_browsing_leaves_thumbnail_cache_and_cloud_untouched() {
    let fixture = Fixture::new();
    for name in [
        "code.rs",
        "code.js",
        "data.json",
        "notes.txt",
        "node_modules",
    ] {
        fixture
            .drive
            .upsert(item(name, Some("root"), name, ItemKind::File));
    }
    fixture.drive.upsert(item(
        "folder",
        Some("root"),
        "archive.jpg",
        ItemKind::Folder,
    ));
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    assert!(!fs::exists(fixture.cache()).unwrap());
    assert!(fixture.drive.thumbnail_list_requests().is_empty());
    assert!(fixture.drive.thumbnail_download_requests().is_empty());
}

#[tokio::test]
async fn repeat_browsing_and_delta_refresh_only_seed_new_or_changed_entries() {
    let fixture = Fixture::new();
    let mut photo = item("photo", Some("root"), "photo.jpg", ItemKind::File);
    let mut missing = item("missing", Some("root"), "missing.png", ItemKind::File);
    for entry in [
        photo.clone(),
        missing.clone(),
        item("stable", Some("root"), "stable.png", ItemKind::File),
    ] {
        fixture.drive.upsert(entry);
    }
    let rendition = thumbnail(image::ImageFormat::Png);
    fixture.drive.set_thumbnail("photo", rendition.clone());
    fixture.drive.set_thumbnail("stable", rendition.clone());
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    let prefix = "file:///tmp/OneDrive%20%234/";
    let photo_path = assert_png(
        &fixture.cache(),
        &format!("{prefix}photo.jpg"),
        MTIME,
        false,
    );
    let missing_path = assert_png(
        &fixture.cache(),
        &format!("{prefix}missing.png"),
        MTIME,
        true,
    );
    let stable_path = assert_png(
        &fixture.cache(),
        &format!("{prefix}stable.png"),
        MTIME,
        false,
    );
    let stable_inode = std::os::unix::fs::MetadataExt::ino(&fs::metadata(&stable_path).unwrap());
    fixture.readdir("root").await;
    let reopened = Engine::open(
        fixture.dir.path().join("db"),
        fixture.drive.clone(),
        Arc::new(SystemClock),
    )
    .unwrap();
    reopened
        .readdir("root", Path::new(MOUNT), &fixture.cache())
        .await
        .unwrap();
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root"]);
    assert_eq!(fixture.drive.thumbnail_download_requests().len(), 2);

    photo.mtime += 1;
    missing.mtime += 1;
    fixture.drive.upsert(photo.clone());
    fixture.drive.upsert(missing.clone());
    fixture.drive.remove_thumbnail("photo");
    fixture.drive.set_thumbnail("missing", rendition.clone());
    fixture
        .drive
        .upsert(item("new", Some("root"), "new.jpg", ItemKind::File));
    fixture.drive.set_thumbnail("new", rendition);
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    assert_png(
        &fixture.cache(),
        &format!("{prefix}photo.jpg"),
        photo.mtime,
        true,
    );
    assert_png(
        &fixture.cache(),
        &format!("{prefix}missing.png"),
        missing.mtime,
        false,
    );
    assert_png(&fixture.cache(), &format!("{prefix}new.jpg"), MTIME, false);
    assert!(!photo_path.exists());
    assert!(!missing_path.exists());
    assert_eq!(
        std::os::unix::fs::MetadataExt::ino(&fs::metadata(stable_path).unwrap()),
        stable_inode,
    );
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root", "root"]);
    let mut downloads = fixture.drive.thumbnail_download_requests().split_off(2);
    downloads.sort();
    assert_eq!(downloads, ["missing", "new"]);
}

#[tokio::test]
async fn concurrent_reads_after_ancestor_and_file_rename_seed_the_new_uri_once() {
    let fixture = Fixture::new();
    let mut ancestor = item("ancestor", Some("root"), "old", ItemKind::Folder);
    let folder = item("folder", Some("ancestor"), "photos", ItemKind::Folder);
    let mut photo = item("photo", Some("folder"), "a.png", ItemKind::File);
    for entry in [ancestor.clone(), folder, photo.clone()] {
        fixture.drive.upsert(entry);
    }
    fixture
        .drive
        .set_thumbnail("photo", thumbnail(image::ImageFormat::Png));
    fixture.engine.sync().await.unwrap();
    fixture.readdir("folder").await;
    assert_png(
        &fixture.cache(),
        "file:///tmp/OneDrive%20%234/old/photos/a.png",
        MTIME,
        false,
    );

    ancestor.name = Some("new parent".into());
    photo.name = Some("b.png".into());
    fixture.drive.upsert(ancestor);
    fixture.drive.upsert(photo);
    fixture.engine.sync().await.unwrap();
    let cache = fixture.cache();
    let reads = (0..8).map(|_| fixture.engine.readdir("folder", Path::new(MOUNT), &cache));
    let results = futures_util::future::join_all(reads).await;
    for result in results {
        assert_eq!(result.unwrap()[0].name, "b.png");
    }
    assert_png(
        &cache,
        "file:///tmp/OneDrive%20%234/new%20parent/photos/b.png",
        MTIME,
        false,
    );
    assert_eq!(
        fixture.drive.thumbnail_list_requests(),
        ["folder", "folder"]
    );
    assert_eq!(
        fixture.drive.thumbnail_download_requests(),
        ["photo", "photo"]
    );
    assert!(fixture.drive.range_requests().is_empty());
}

#[tokio::test]
async fn corrupt_rendition_is_retried_without_creating_a_fail_marker() {
    let fixture = Fixture::new();
    fixture
        .drive
        .upsert(item("photo", Some("root"), "photo.png", ItemKind::File));
    fixture.drive.set_thumbnail("photo", vec![99]);
    fixture.engine.sync().await.unwrap();
    let result = fixture
        .engine
        .readdir("root", Path::new(MOUNT), &fixture.cache())
        .await;
    assert!(matches!(result, Err(EngineError::InvalidThumbnail(_))));
    assert!(!fixture.cache().exists());

    fixture
        .drive
        .set_thumbnail("photo", thumbnail(image::ImageFormat::Png));
    fixture.readdir("root").await;
    assert_png(
        &fixture.cache(),
        "file:///tmp/OneDrive%20%234/photo.png",
        MTIME,
        false,
    );
    assert_eq!(
        fixture.drive.thumbnail_download_requests(),
        ["photo", "photo"]
    );
}

struct PausedThumbnailDrive {
    inner: FakeCloudDrive,
    first: AtomicBool,
    requested: Notify,
    resume: Notify,
}

#[async_trait]
impl CloudDrive for PausedThumbnailDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.inner.delta(request).await
    }

    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        self.inner.list_thumbnails(folder_id).await
    }

    async fn download_thumbnail(&self, thumbnail: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        let bytes = self.inner.download_thumbnail(thumbnail).await?;
        if self.first.swap(false, Ordering::Relaxed) {
            self.requested.notify_one();
            self.resume.notified().await;
        }
        Ok(bytes)
    }
}

#[tokio::test]
async fn delta_during_download_discards_the_old_uri_and_mtime() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("thumbnails");
    let drive = Arc::new(PausedThumbnailDrive {
        inner: FakeCloudDrive::new(100),
        first: AtomicBool::new(true),
        requested: Notify::new(),
        resume: Notify::new(),
    });
    drive
        .inner
        .upsert(item("root", None, "root", ItemKind::Folder));
    let mut photo = item("photo", Some("root"), "old.png", ItemKind::File);
    drive.inner.upsert(photo.clone());
    drive
        .inner
        .set_thumbnail("photo", thumbnail(image::ImageFormat::Png));
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let read = engine.readdir("root", Path::new(MOUNT), &cache);
    let update = async {
        drive.requested.notified().await;
        photo.name = Some("new.png".into());
        photo.mtime += 1;
        drive.inner.upsert(photo);
        drive.inner.remove_thumbnail("photo");
        engine.sync().await.unwrap();
        drive.resume.notify_one();
    };
    let (entries, ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(read, update) })
            .await
            .expect("delta and directory reads should finish");
    assert_eq!(entries.unwrap()[0].name, "new.png");
    assert_png(
        &cache,
        "file:///tmp/OneDrive%20%234/new.png",
        MTIME + 1,
        true,
    );
    assert_eq!(fs::read_dir(cache.join("x-large")).unwrap().count(), 0);
    assert_eq!(drive.inner.thumbnail_list_requests(), ["root", "root"]);
}
