use std::fs::{self, File};
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use onedrive_core::{Engine, FakeCloudDrive, ItemKind, RemoteItem, SystemClock};

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
