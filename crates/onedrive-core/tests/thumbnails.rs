use std::fs::{self, File};
use std::io::{BufReader, Cursor};
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use onedrive_core::{
    CloudDrive, CloudError, DeltaPage, DeltaRequest, Engine, EngineError, FakeCloudDrive, ItemKind,
    RemoteItem, RemoteThumbnail, SystemClock,
};
use rustix::fs::inotify;
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
        "code.ts",
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
    assert!(fixture.drive.range_requests().is_empty());
}

#[tokio::test]
async fn system_mime_globs_seed_glycin_formats_without_original_reads() {
    let fixture = Fixture::new();
    let names = [
        "a.QOI", "b.exr", "c.j2c", "d.j2k", "e.jpc", "f.jpg2", "g.vda", "h.tpic", "i.icb", "j.vst",
        "k.svg.gz", "l.dds", "m.jxl",
    ];
    for name in names {
        fixture
            .drive
            .upsert(item(name, Some("root"), name, ItemKind::File));
    }
    fixture
        .drive
        .set_thumbnail("a.QOI", thumbnail(image::ImageFormat::Jpeg));
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    for name in names {
        let uri = format!("file:///tmp/OneDrive%20%234/{name}");
        assert_png(&fixture.cache(), &uri, MTIME, name != "a.QOI");
    }
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root"]);
    assert_eq!(fixture.drive.thumbnail_download_requests(), ["a.QOI"]);
    assert!(fixture.drive.range_requests().is_empty());
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

#[tokio::test]
async fn corrupt_image_data_and_opaque_fail_markers_are_repaired() {
    let fixture = Fixture::new();
    for name in ["image.jpg", "marker.png"] {
        fixture
            .drive
            .upsert(item(name, Some("root"), name, ItemKind::File));
    }
    fixture
        .drive
        .set_thumbnail("image.jpg", thumbnail(image::ImageFormat::Jpeg));
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    let image_uri = "file:///tmp/OneDrive%20%234/image.jpg";
    let marker_uri = "file:///tmp/OneDrive%20%234/marker.png";
    let image = assert_png(&fixture.cache(), image_uri, MTIME, false);
    let marker = assert_png(&fixture.cache(), marker_uri, MTIME, true);
    fixture.readdir("root").await;
    let bytes = fs::read(&image).unwrap();
    let idat = bytes.windows(4).position(|bytes| bytes == b"IDAT").unwrap();
    fs::write(&image, &bytes[..idat + 5]).unwrap();
    let old = png::Decoder::new(BufReader::new(File::open(&marker).unwrap()))
        .read_info()
        .unwrap();
    let metadata = old.info().uncompressed_latin1_text.clone();
    drop(old);
    let mut encoder = png::Encoder::new(File::create(&marker).unwrap(), 1, 1);
    encoder.set_color(png::ColorType::Rgba);
    for entry in metadata {
        encoder.add_text_chunk(entry.keyword, entry.text).unwrap();
    }
    let mut writer = encoder.write_header().unwrap();
    writer.write_image_data(&[255; 4]).unwrap();
    writer.finish().unwrap();
    fixture.readdir("root").await;
    assert_png(&fixture.cache(), image_uri, MTIME, false);
    assert_png(&fixture.cache(), marker_uri, MTIME, true);
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root", "root"]);
}

#[tokio::test]
async fn warm_validation_tracks_replacement_and_same_size_edits() {
    let fixture = Fixture::new();
    let name = "cache.jpg";
    let uri = "file:///tmp/OneDrive%20%234/cache.jpg";
    fixture
        .drive
        .upsert(item(name, Some("root"), name, ItemKind::File));
    fixture
        .drive
        .set_thumbnail(name, thumbnail(image::ImageFormat::Jpeg));
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    let path = assert_png(&fixture.cache(), uri, MTIME, false);
    fixture.readdir("root").await;

    let watch =
        inotify::init(inotify::CreateFlags::NONBLOCK | inotify::CreateFlags::CLOEXEC).unwrap();
    inotify::add_watch(&watch, &path, inotify::WatchFlags::OPEN).unwrap();
    let mut buffer = [MaybeUninit::uninit(); 512];
    let mut events = inotify::Reader::new(watch, &mut buffer);
    fixture.readdir("root").await;
    assert_eq!(events.next().unwrap_err(), rustix::io::Errno::AGAIN);

    // Change the last IDAT's data without changing its stored CRC, length, or mtime.
    let original_mtime = fs::metadata(&path).unwrap().modified().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let idat = bytes
        .windows(4)
        .rposition(|bytes| bytes == b"IDAT")
        .unwrap();
    bytes[idat + 4] ^= 1;
    fs::write(&path, &bytes).unwrap();
    File::open(&path)
        .unwrap()
        .set_modified(original_mtime)
        .unwrap();
    fixture.readdir("root").await;
    assert_png(&fixture.cache(), uri, MTIME, false);
    fixture.readdir("root").await;

    // Replace the inode with matching size and mtime.
    let original_mtime = fs::metadata(&path).unwrap().modified().unwrap();
    let replacement = path.with_extension("tmp");
    fs::write(&replacement, bytes).unwrap();
    File::open(&replacement)
        .unwrap()
        .set_modified(original_mtime)
        .unwrap();
    fs::rename(replacement, &path).unwrap();
    fixture.readdir("root").await;
    assert_png(&fixture.cache(), uri, MTIME, false);
    assert_eq!(
        fixture.drive.thumbnail_list_requests(),
        ["root", "root", "root"]
    );
}

#[tokio::test]
async fn invalid_cache_shapes_and_metadata_are_repaired() {
    let fixture = Fixture::new();
    let rendition = thumbnail(image::ImageFormat::Jpeg);
    for name in [
        "dimensions.jpg",
        "bytes.jpg",
        "uri.jpg",
        "mtime.jpg",
        "size.jpg",
        "end.jpg",
        "marker.png",
        "malformed.png",
    ] {
        fixture
            .drive
            .upsert(item(name, Some("root"), name, ItemKind::File));
        if name.ends_with(".jpg") {
            fixture.drive.set_thumbnail(name, rendition.clone());
        }
    }
    fixture.engine.sync().await.unwrap();
    fixture.readdir("root").await;
    fixture.readdir("root").await;

    for (name, width, edit) in [
        ("dimensions.jpg", 513, None),
        ("marker.png", 2, None),
        (
            "uri.jpg",
            512,
            Some(("Thumb::URI", "file:///tmp/wrong.jpg")),
        ),
        ("mtime.jpg", 512, Some(("Thumb::MTime", "1700000002"))),
        ("size.jpg", 512, Some(("Thumb::Size", "1"))),
    ] {
        let uri = format!("file:///tmp/OneDrive%20%234/{name}");
        let path = assert_png(&fixture.cache(), &uri, MTIME, name == "marker.png");
        let reader = png::Decoder::new(BufReader::new(File::open(&path).unwrap()))
            .read_info()
            .unwrap();
        let mut metadata = reader.info().uncompressed_latin1_text.clone();
        if let Some((key, value)) = edit {
            metadata.retain(|entry| entry.keyword != key);
            metadata.push(png::text_metadata::TEXtChunk::new(key, value));
        }
        let mut encoder = png::Encoder::new(File::create(path).unwrap(), width, 1);
        encoder.set_color(png::ColorType::Rgba);
        for entry in metadata {
            encoder.add_text_chunk(entry.keyword, entry.text).unwrap();
        }
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&vec![0; width as usize * 4])
            .unwrap();
        writer.finish().unwrap();
    }
    for name in ["bytes.jpg", "end.jpg", "malformed.png"] {
        let uri = format!("file:///tmp/OneDrive%20%234/{name}");
        let path = assert_png(&fixture.cache(), &uri, MTIME, name == "malformed.png");
        let file = File::options().write(true).open(path).unwrap();
        let length = match name {
            "bytes.jpg" => 2 * 1024 * 1024 + 1,
            "end.jpg" => file.metadata().unwrap().len() - 12,
            _ => 4,
        };
        file.set_len(length).unwrap();
    }

    fixture.readdir("root").await;
    for name in [
        "dimensions.jpg",
        "bytes.jpg",
        "uri.jpg",
        "mtime.jpg",
        "size.jpg",
        "end.jpg",
        "marker.png",
        "malformed.png",
    ] {
        let uri = format!("file:///tmp/OneDrive%20%234/{name}");
        let path = assert_png(&fixture.cache(), &uri, MTIME, name.ends_with(".png"));
        assert!(fs::metadata(path).unwrap().len() < 2 * 1024 * 1024);
    }
    assert_eq!(fixture.drive.thumbnail_list_requests(), ["root", "root"]);
    assert_eq!(fixture.drive.thumbnail_download_requests().len(), 12);
}

struct InterruptedThumbnailDrive {
    inner: FakeCloudDrive,
    offline: AtomicBool,
    rendition_fails: AtomicBool,
}

#[async_trait]
impl CloudDrive for InterruptedThumbnailDrive {
    async fn delta(&self, request: DeltaRequest) -> Result<DeltaPage, CloudError> {
        self.inner.delta(request).await
    }

    async fn list_thumbnails(&self, folder_id: &str) -> Result<Vec<RemoteThumbnail>, CloudError> {
        if self.offline.load(Ordering::Relaxed) {
            return Err(CloudError::Unavailable);
        }
        self.inner.list_thumbnails(folder_id).await
    }

    async fn download_thumbnail(&self, rendition: &RemoteThumbnail) -> Result<Vec<u8>, CloudError> {
        if self.rendition_fails.load(Ordering::Relaxed) {
            return Err(CloudError::Unavailable);
        }
        self.inner.download_thumbnail(rendition).await
    }
}

#[tokio::test]
async fn seed_before_listing_withholds_unseeded_media_during_transient_errors() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("thumbnails");
    let drive = Arc::new(InterruptedThumbnailDrive {
        inner: FakeCloudDrive::new(100),
        offline: AtomicBool::new(false),
        rendition_fails: AtomicBool::new(false),
    });
    let inner = &drive.inner;
    inner.upsert(item("root", None, "root", ItemKind::Folder));
    for folder in ["seeded", "unseeded", "code"] {
        inner.upsert(item(folder, Some("root"), folder, ItemKind::Folder));
    }
    inner.upsert(item("good", Some("seeded"), "good.png", ItemKind::File));
    inner.upsert(item("retry", Some("unseeded"), "retry.qoi", ItemKind::File));
    inner.upsert(item(
        "missing",
        Some("unseeded"),
        "missing.png",
        ItemKind::File,
    ));
    inner.upsert(item("code-file", Some("code"), "script.ts", ItemKind::File));
    let rendition = thumbnail(image::ImageFormat::Jpeg);
    inner.set_thumbnail("good", rendition.clone());
    inner.set_thumbnail("retry", rendition);
    let engine = Engine::open(dir.path().join("db"), drive.clone(), Arc::new(SystemClock)).unwrap();
    engine.sync().await.unwrap();
    let list = |folder| engine.readdir(folder, Path::new(MOUNT), &cache);
    assert_eq!(list("seeded").await.unwrap().len(), 1);

    drive.offline.store(true, Ordering::Relaxed);
    assert_eq!(list("seeded").await.unwrap().len(), 1);
    assert_eq!(list("code").await.unwrap().len(), 1);
    assert!(matches!(
        list("unseeded").await,
        Err(EngineError::Cloud(CloudError::Unavailable))
    ));
    assert!(!cache.join("fail").exists());

    drive.offline.store(false, Ordering::Relaxed);
    drive.rendition_fails.store(true, Ordering::Relaxed);
    assert!(matches!(
        list("unseeded").await,
        Err(EngineError::Cloud(CloudError::Unavailable))
    ));
    assert_eq!(
        fs::read_dir(cache.join("fail/gnome-thumbnail-factory"))
            .unwrap()
            .count(),
        1
    );
    assert_png(
        &cache,
        "file:///tmp/OneDrive%20%234/unseeded/missing.png",
        MTIME,
        true,
    );

    drive.rendition_fails.store(false, Ordering::Relaxed);
    assert_eq!(list("unseeded").await.unwrap().len(), 2);
    assert_png(
        &cache,
        "file:///tmp/OneDrive%20%234/unseeded/retry.qoi",
        MTIME,
        false,
    );
    assert_eq!(
        inner.thumbnail_list_requests(),
        ["seeded", "unseeded", "unseeded"]
    );
    assert_eq!(inner.thumbnail_download_requests(), ["good", "retry"]);
    assert!(inner.range_requests().is_empty());
}
