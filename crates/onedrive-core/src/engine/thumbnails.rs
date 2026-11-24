use std::collections::HashMap;
use std::env;
use std::fs::{self, File, Metadata};
use std::io::{self, Cursor, Read};
use std::num::NonZeroUsize;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use futures_util::{StreamExt, stream};
use image::{ImageReader, RgbaImage};
use lru::LruCache;
use rusqlite::Connection;
use rustix::fs::{Mode, OFlags};
use tempfile::{NamedTempFile, TempPath};
use tokio::sync::Mutex as AsyncMutex;

use super::{CatalogItem, Engine, EngineError, list_children, path_for};
use crate::{ItemKind, RemoteThumbnail};

const MAX_EDGE: u32 = 512;
const MAX_CACHED_PNG_BYTES: usize = 2 * 1024 * 1024;
const VALIDATION_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
const CONCURRENT_THUMBNAILS: usize = 16;
const THUMB_URI: &str = "Thumb::URI";
const THUMB_MTIME: &str = "Thumb::MTime";
const THUMB_SIZE: &str = "Thumb::Size";
const THUMB_CONTENT: &str = "OneDrive::Content";
const THUMBNAIL_DIR: &str = "x-large";
const FAILED_DIR: &str = "fail/gnome-thumbnail-factory";

pub(super) struct ThumbnailCache {
    folders: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    validated: Mutex<LruCache<PathBuf, (FileStamp, TargetMetadata)>>,
}

impl Default for ThumbnailCache {
    fn default() -> Self {
        Self {
            folders: Mutex::default(),
            validated: Mutex::new(LruCache::new(VALIDATION_CACHE_ENTRIES)),
        }
    }
}

impl ThumbnailCache {
    fn folder_lock(&self, folder_id: &str) -> Arc<AsyncMutex<()>> {
        let mut folders = self.folders.lock().unwrap();
        if let Some(lock) = folders.get(folder_id).and_then(Weak::upgrade) {
            return lock;
        }
        folders.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(AsyncMutex::new(()));
        folders.insert(folder_id.into(), Arc::downgrade(&lock));
        lock
    }

    fn is_seeded(&self, target: &Target, root: &Path) -> bool {
        let (thumbnail, marker) = target.cache_paths(root, false);
        let (path, stamp, failed) = match (cache_file(&thumbnail), cache_file(&marker)) {
            (Ok(Some(stamp)), Ok(None)) => (thumbnail, stamp, false),
            (Ok(None), Ok(Some(stamp))) => (marker, stamp, true),
            _ => return false,
        };
        if self
            .validated
            .lock()
            .unwrap()
            .get(&path)
            .is_some_and(|(cached, metadata)| *cached == stamp && *metadata == target.metadata)
        {
            return true;
        }
        if !valid_png(&path, stamp, &target.metadata, failed) {
            return false;
        }
        self.validated
            .lock()
            .unwrap()
            .put(path, (stamp, target.metadata.clone()));
        true
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    device: u64,
    inode: u64,
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl FileStamp {
    fn new(metadata: Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            len: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

fn cache_file(path: &Path) -> io::Result<Option<FileStamp>> {
    match fs::symlink_metadata(path) {
        // A dangling symlink or other nonregular file still occupies this cache path.
        Ok(metadata) => Ok(Some(FileStamp::new(metadata))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[derive(PartialEq, Eq)]
struct Target {
    item_id: String,
    metadata: TargetMetadata,
    filename: String,
}

#[derive(Clone, PartialEq, Eq)]
struct TargetMetadata {
    uri: String,
    mtime: String,
    content: String,
    size: String,
}

impl TargetMetadata {
    fn fields(&self) -> [(&'static str, &str); 3] {
        [
            (THUMB_URI, &self.uri),
            (THUMB_MTIME, &self.mtime),
            (THUMB_CONTENT, &self.content),
        ]
    }

    fn matches(&self, info: &png::Info<'_>) -> bool {
        let text = &info.uncompressed_latin1_text;
        self.fields().into_iter().all(|(key, value)| {
            let mut values = text.iter().filter(|entry| entry.keyword == key);
            values.next().is_some_and(|entry| entry.text == value) && values.next().is_none()
        }) && text
            .iter()
            .filter(|entry| entry.keyword == THUMB_SIZE)
            .all(|entry| entry.text == self.size)
    }
}

struct Directory {
    entries: Vec<CatalogItem>,
    targets: Vec<Target>,
}

impl Directory {
    fn read(db: &Connection, folder_id: &str, mountpoint: &Path) -> Result<Self, EngineError> {
        let snapshot = db.unchecked_transaction()?;
        let entries = list_children(&snapshot, folder_id)?;
        let mut targets = Vec::new();
        let mut media = entries
            .iter()
            .filter(|entry| is_thumbnailable(entry))
            .peekable();
        if media.peek().is_some() {
            let relative = path_for(&snapshot, folder_id)?.ok_or(EngineError::NotFound)?;
            let path = mountpoint.join(relative.strip_prefix("/").expect("absolute catalog path"));
            targets.extend(media.map(|entry| Target::new(entry, &path)));
        }
        Ok(Self { entries, targets })
    }
}

impl Engine {
    /// Return a directory snapshot after seeding thumbnails for its media entries.
    /// The mountpoint is the absolute path exposed to desktop clients.
    pub async fn readdir(
        &self,
        folder_id: &str,
        mountpoint: &Path,
        thumbnail_root: &Path,
    ) -> Result<Vec<CatalogItem>, EngineError> {
        let lock = self.thumbnails.folder_lock(folder_id);
        let _guard = lock.lock().await;
        self.seed_before_listing(folder_id, mountpoint, thumbnail_root)
            .await
    }

    // Withhold unseeded entries when seeding fails so desktop thumbnailers
    // cannot fall back to downloading originals. A later listing retries.
    async fn seed_before_listing(
        &self,
        folder_id: &str,
        mountpoint: &Path,
        thumbnail_root: &Path,
    ) -> Result<Vec<CatalogItem>, EngineError> {
        loop {
            let directory = Directory::read(&self.db(), folder_id, mountpoint)?;
            let unseeded: Vec<_> = directory
                .targets
                .iter()
                .filter(|target| !self.thumbnails.is_seeded(target, thumbnail_root))
                .collect();
            if unseeded.is_empty() {
                return Ok(directory.entries);
            }
            let renditions: HashMap<_, _> = self
                .cloud
                .list_thumbnails(folder_id)
                .await?
                .into_iter()
                .map(|thumbnail| (thumbnail.item_id.clone(), thumbnail))
                .collect();
            let results: Vec<_> = stream::iter(unseeded)
                .map(|target| {
                    self.prepare_thumbnail(target, renditions.get(&target.item_id), thumbnail_root)
                })
                .buffer_unordered(CONCURRENT_THUMBNAILS)
                .collect()
                .await;

            // Delta can change a name, ancestor path, or file generation during CDN I/O.
            // Keep the catalog locked from this check through publication.
            let db = self.db();
            let current = Directory::read(&db, folder_id, mountpoint)?;
            if current.targets != directory.targets {
                continue;
            }
            // Publish successes even if another download failed so the next visit
            // only requests the entries still missing their cache file.
            let mut error = None;
            for result in results {
                if let Err(failure) = result.and_then(PreparedThumbnail::publish) {
                    error.get_or_insert(failure);
                }
            }
            return error.map_or(Ok(current.entries), Err);
        }
    }

    async fn prepare_thumbnail(
        &self,
        target: &Target,
        rendition: Option<&RemoteThumbnail>,
        thumbnail_root: &Path,
    ) -> Result<PreparedThumbnail, EngineError> {
        let bytes = match rendition {
            Some(rendition) => Some(self.cloud.download_thumbnail(rendition).await?),
            None => None,
        };
        let failed = bytes.is_none();
        let (path, other) = target.cache_paths(thumbnail_root, failed);
        let metadata = target.metadata.clone();
        tokio::task::spawn_blocking(move || {
            let pixels = thumbnail_pixels(bytes)?;
            let directory = path.parent().expect("cache file has a parent directory");
            fs::create_dir_all(directory).map_err(EngineError::ThumbnailIo)?;
            let file = NamedTempFile::new_in(directory).map_err(EngineError::ThumbnailIo)?;
            let mut encoder = png::Encoder::new(file.as_file(), pixels.width(), pixels.height());
            encoder.set_color(png::ColorType::Rgba);
            for (key, value) in metadata.fields() {
                encoder.add_text_chunk(key.into(), value.into())?;
            }
            let mut writer = encoder.write_header()?;
            writer.write_image_data(pixels.as_raw())?;
            writer.finish()?;
            // Close the staging file while other renditions finish, so large folders
            // keep descriptors bounded by the number of active workers.
            Ok(PreparedThumbnail {
                staged: file.into_temp_path(),
                path,
                other,
            })
        })
        .await
        .map_err(|error| EngineError::ThumbnailIo(io::Error::other(error)))?
    }
}

struct PreparedThumbnail {
    staged: TempPath,
    path: PathBuf,
    other: PathBuf,
}

impl PreparedThumbnail {
    fn publish(self) -> Result<(), EngineError> {
        self.staged
            .persist(self.path)
            .map_err(|failure| EngineError::ThumbnailIo(failure.error))?;
        // A stale success file can hide a fresh fail marker in GIO.
        match fs::remove_file(self.other) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(EngineError::ThumbnailIo(error)),
        }
    }
}

// Only an absent rendition in the folder response creates a fail marker.
// Cloud errors and invalid payloads leave the item eligible for a retry.
fn thumbnail_pixels(bytes: Option<Vec<u8>>) -> Result<RgbaImage, EngineError> {
    let Some(bytes) = bytes else {
        return Ok(RgbaImage::new(1, 1));
    };
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(EngineError::ThumbnailIo)?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(4096);
    limits.max_image_height = Some(4096);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode()?;
    if image.width().max(image.height()) <= MAX_EDGE {
        Ok(image.into_rgba8())
    } else {
        Ok(image.thumbnail(MAX_EDGE, MAX_EDGE).into_rgba8())
    }
}

impl Target {
    fn new(entry: &CatalogItem, folder: &Path) -> Self {
        let uri = file_uri(&folder.join(&entry.name));
        let filename = format!("{:x}.png", md5::compute(&uri));
        // A different item or content generation can occupy the same URI and second.
        let content = format!(
            "{:x}:{:x}:{}",
            md5::compute(&entry.item_id),
            md5::compute(entry.ctag.as_deref().unwrap_or_default()),
            entry.size
        );
        Self {
            item_id: entry.item_id.clone(),
            metadata: TargetMetadata {
                uri,
                mtime: entry.mtime.to_string(),
                content,
                size: entry.size.to_string(),
            },
            filename,
        }
    }

    fn cache_paths(&self, root: &Path, failed: bool) -> (PathBuf, PathBuf) {
        let thumbnail = root.join(THUMBNAIL_DIR).join(&self.filename);
        let marker = root.join(FAILED_DIR).join(&self.filename);
        if failed {
            (marker, thumbnail)
        } else {
            (thumbnail, marker)
        }
    }
}

fn valid_png(path: &Path, stamp: FileStamp, metadata: &TargetMetadata, failed: bool) -> bool {
    if stamp.len > MAX_CACHED_PNG_BYTES as u64 {
        return false;
    }
    let Ok(fd) = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) else {
        return false;
    };
    let mut file = File::from(fd);
    if !file
        .metadata()
        .is_ok_and(|meta| meta.is_file() && FileStamp::new(meta) == stamp)
    {
        return false;
    }
    let mut bytes = Vec::with_capacity(stamp.len as usize);
    if (&mut file)
        .take(MAX_CACHED_PNG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() as u64 != stamp.len
    {
        return false;
    }

    let mut options = png::DecodeOptions::default();
    options.set_ignore_adler32(false);
    options.set_skip_ancillary_crc_failures(false);
    let mut decoder = png::Decoder::new_with_options(Cursor::new(bytes), options);
    decoder.set_limits(png::Limits {
        bytes: MAX_CACHED_PNG_BYTES,
    });
    let Ok(mut reader) = decoder.read_info() else {
        return false;
    };
    let info = reader.info();
    if info.width.max(info.height) > MAX_EDGE
        || info.animation_control.is_some()
        || !metadata.matches(info)
        || (failed
            && (info.width, info.height, info.color_type, info.bit_depth)
                != (1, 1, png::ColorType::Rgba, png::BitDepth::Eight))
    {
        return false;
    }
    let Some(buffer_size) = reader.output_buffer_size() else {
        return false;
    };
    let mut pixels = vec![0; buffer_size];
    reader.next_frame(&mut pixels).is_ok()
        && (!failed || pixels[3] == 0)
        && reader.finish().is_ok()
        && metadata.matches(reader.info())
        && matches!(cache_file(path), Ok(Some(current)) if current == stamp)
}

fn is_thumbnailable(entry: &CatalogItem) -> bool {
    if entry.kind != ItemKind::File || entry.name.as_bytes().contains(&0) {
        return false;
    }
    let (mime, _) = gio::content_type_guess(Some(&entry.name), None);
    if mime.starts_with("image/") || mime.starts_with("video/")
        // Shared MIME data also gives this image extension to Visio drawings.
        || mime == "application/vnd.visio"
    {
        return true;
    }

    let Some(extension) = Path::new(&entry.name)
        .extension()
        .and_then(|ext| ext.to_str())
    else {
        return false;
    };
    (mime == "application/octet-stream"
        && mime_guess::from_ext(extension)
            .iter()
            .any(|mime| matches!(mime.type_().as_str(), "image" | "video")))
        || matches!(
            extension.to_ascii_lowercase().as_str(),
            "pdf"
                | "doc"
                | "docx"
                | "docm"
                | "dot"
                | "dotx"
                | "dotm"
                | "xls"
                | "xlsx"
                | "xlsm"
                | "xlsb"
                | "xlt"
                | "xltx"
                | "xltm"
                | "ppt"
                | "pptx"
                | "pptm"
                | "pps"
                | "ppsx"
                | "ppsm"
                | "pot"
                | "potx"
                | "potm"
                | "odt"
                | "ods"
                | "odp"
                | "rtf"
                | "heic"
                | "heif"
                | "avif"
                | "nef"
                | "nrw"
                | "orf"
                | "pef"
                | "rw2"
                | "mrw"
                | "mef"
                | "arw"
                | "raf"
                | "dng"
                | "cr2"
                | "cr3"
        )
}

fn file_uri(path: &Path) -> String {
    use std::fmt::Write;

    let mut uri = String::from("file://");
    // GLib's file URI path escaping preserves these RFC 2396 path characters.
    for &byte in path.as_os_str().as_encoded_bytes() {
        if byte.is_ascii_alphanumeric() || b"/-_.!~*'():@&=+$,".contains(&byte) {
            uri.push(char::from(byte));
        } else {
            write!(uri, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    uri
}

pub(crate) fn default_thumbnail_root() -> io::Result<PathBuf> {
    env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| {
            env::var_os("HOME")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .map(|path| path.join(".cache"))
        })
        .map(|path| path.join("thumbnails"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no XDG cache directory"))
}
