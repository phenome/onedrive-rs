use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use futures_util::{StreamExt, stream};
use image::{ImageReader, RgbaImage};
use rusqlite::Connection;
use tempfile::NamedTempFile;
use tokio::sync::Mutex as AsyncMutex;

use super::{CatalogItem, Engine, EngineError, list_children, path_for};
use crate::{ItemKind, RemoteThumbnail};

const MAX_EDGE: u32 = 512;
const CONCURRENT_THUMBNAILS: usize = 16;
const THUMB_URI: &str = "Thumb::URI";
const THUMB_MTIME: &str = "Thumb::MTime";
const THUMB_CONTENT: &str = "OneDrive::Content";
const THUMBNAIL_DIR: &str = "x-large";
const FAILED_DIR: &str = "fail/gnome-thumbnail-factory";

#[derive(Default)]
pub(super) struct ThumbnailCache {
    folders: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
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
}

#[derive(PartialEq, Eq)]
struct Target {
    item_id: String,
    uri: String,
    mtime: String,
    content: String,
    filename: String,
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
        loop {
            let directory = Directory::read(&self.db(), folder_id, mountpoint)?;
            let unseeded: Vec<_> = directory
                .targets
                .iter()
                .filter(|target| !target.is_seeded(thumbnail_root))
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
        let metadata = [
            (THUMB_URI, target.uri.clone()),
            (THUMB_MTIME, target.mtime.clone()),
            (THUMB_CONTENT, target.content.clone()),
        ];
        tokio::task::spawn_blocking(move || {
            let pixels = thumbnail_pixels(bytes)?;
            let directory = path.parent().expect("cache file has a parent directory");
            fs::create_dir_all(directory).map_err(EngineError::ThumbnailIo)?;
            let file = NamedTempFile::new_in(directory).map_err(EngineError::ThumbnailIo)?;
            let mut encoder = png::Encoder::new(file.as_file(), pixels.width(), pixels.height());
            encoder.set_color(png::ColorType::Rgba);
            for (key, value) in metadata {
                encoder.add_text_chunk(key.into(), value)?;
            }
            let mut writer = encoder.write_header()?;
            writer.write_image_data(pixels.as_raw())?;
            writer.finish()?;
            Ok(PreparedThumbnail { file, path, other })
        })
        .await
        .map_err(|error| EngineError::ThumbnailIo(io::Error::other(error)))?
    }
}

struct PreparedThumbnail {
    file: NamedTempFile,
    path: PathBuf,
    other: PathBuf,
}

impl PreparedThumbnail {
    fn publish(self) -> Result<(), EngineError> {
        self.file
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
            mtime: entry.mtime.to_string(),
            uri,
            content,
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

    fn is_seeded(&self, root: &Path) -> bool {
        let (thumbnail, marker) = self.cache_paths(root, false);
        for (path, other) in [(&thumbnail, &marker), (&marker, &thumbnail)] {
            let Ok(file) = File::open(path) else { continue };
            if other.exists() {
                return false;
            }
            let Ok(reader) = png::Decoder::new(BufReader::new(file)).read_info() else {
                return false;
            };
            let metadata = &reader.info().uncompressed_latin1_text;
            return [
                (THUMB_URI, &self.uri),
                (THUMB_MTIME, &self.mtime),
                (THUMB_CONTENT, &self.content),
            ]
            .into_iter()
            .all(|(key, value)| {
                metadata
                    .iter()
                    .any(|text| text.keyword == key && &text.text == value)
            });
        }
        false
    }
}

fn is_thumbnailable(entry: &CatalogItem) -> bool {
    let Some(extension) = Path::new(&entry.name)
        .extension()
        .and_then(|ext| ext.to_str())
    else {
        return false;
    };
    entry.kind == ItemKind::File
        && (mime_guess::from_ext(extension)
            .iter()
            .any(|mime| matches!(mime.type_().as_str(), "image" | "video"))
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
            ))
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
