//! Tiny HTTP server that exposes a directory of JSONL files with:
//!
//!   - HTTP/1.1 Range request support (the json-analyzer client uses
//!     this to fetch only the bytes appended since its last poll).
//!   - CRC32 content-hash ETag on every response. Cached by
//!     (path, mtime, size) so we only recompute when the file actually
//!     changes — keeps the steady-state per-request cost in single-digit
//!     ms even for large files.
//!   - Threaded request handling so an in-flight large response doesn't
//!     block the next poll.
//!
//! Apples-to-apples Rust counterpart of `tools/stream_server.py`; the
//! two are kept feature-identical so the analyzer can't tell them apart
//! and the perf comparison is meaningful.

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

/// Cache key: (mtime, size, inode). Inode catches mv-style atomic-
/// rename replacements where the new file happens to share an mtime
/// and size with the cached one; SystemTime is nanosecond-precise on
/// modern Unix so coincidental mtime collisions within a second are
/// already covered.
struct CrcCache {
    inner: Mutex<HashMap<PathBuf, (SystemTime, u64, u64, u32)>>,
}

impl CrcCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Return (crc32, size, mtime) for `path`, computing and caching as
    /// needed. Cache invalidates on any mtime / size / inode change.
    fn get(&self, path: &Path) -> Option<(u32, u64, SystemTime)> {
        let meta = std::fs::metadata(path).ok()?;
        let mtime = meta.modified().ok()?;
        let size = meta.len();
        let inode = inode_of(&meta);
        {
            let guard = self.inner.lock().unwrap();
            if let Some((m, s, i, c)) = guard.get(path) {
                if *m == mtime && *s == size && *i == inode {
                    return Some((*c, size, mtime));
                }
            }
        }
        // Cache miss / stale: read + hash.
        let mut f = File::open(path).ok()?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = f.read(&mut buf).ok()?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let crc = hasher.finalize();
        let mut guard = self.inner.lock().unwrap();
        guard.insert(path.to_path_buf(), (mtime, size, inode, crc));
        Some((crc, size, mtime))
    }
}

#[cfg(unix)]
fn inode_of(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    meta.ino()
}

#[cfg(not(unix))]
fn inode_of(_meta: &std::fs::Metadata) -> u64 {
    // Windows doesn't expose inode in std; rely on (mtime, size) alone.
    // Worst case: same-tick atomic-rename with matching size goes
    // undetected — unlikely for a JSONL log.
    0
}

fn parse_range(spec: &str, total: u64) -> Option<(u64, u64)> {
    let s = spec.strip_prefix("bytes=")?.trim();
    if s.contains(',') {
        return None; // we don't do multipart
    }
    if let Some(stripped) = s.strip_prefix('-') {
        let n: u64 = stripped.parse().ok()?;
        if n == 0 || total == 0 {
            return None;
        }
        let start = total.saturating_sub(n);
        return Some((start, total - 1));
    }
    let (a, b) = s.split_once('-')?;
    let start: u64 = a.parse().ok()?;
    if start >= total {
        return None;
    }
    let end = if b.is_empty() {
        total - 1
    } else {
        b.parse::<u64>().ok()?.min(total - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

fn crc32_range(path: &Path, start: u64, length: u64) -> Option<u32> {
    let mut f = File::open(path).ok()?;
    f.seek(SeekFrom::Start(start)).ok()?;
    let mut reader = f.take(length);
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Some(hasher.finalize())
}

fn hdr(k: &str, v: &str) -> Header {
    Header::from_bytes(k.as_bytes(), v.as_bytes()).unwrap()
}

fn resolve(root: &Path, request_path: &str) -> Option<PathBuf> {
    let trimmed = request_path
        .split('?')
        .next()
        .unwrap_or("")
        .trim_start_matches('/');
    let candidate = root.join(trimmed);
    let canonical = std::fs::canonicalize(&candidate).ok()?;
    if !canonical.starts_with(root) {
        return None;
    }
    if !canonical.is_file() {
        return None;
    }
    Some(canonical)
}

fn handle_request(req: Request, root: &Path, cache: &CrcCache) {
    let method = req.method().clone();
    if !matches!(method, Method::Get | Method::Head) {
        let _ = req.respond(Response::empty(StatusCode(405)));
        return;
    }
    let path = match resolve(root, req.url()) {
        Some(p) => p,
        None => {
            let _ = req.respond(Response::empty(StatusCode(404)));
            return;
        }
    };
    let info = match cache.get(&path) {
        Some(i) => i,
        None => {
            let _ = req.respond(Response::empty(StatusCode(404)));
            return;
        }
    };
    let (crc, size, _mtime) = info;
    let etag = format!("\"crc32:{:08x}\"", crc);

    let range_value: Option<String> = req
        .headers()
        .iter()
        .find(|h| h.field.equiv("Range"))
        .map(|h| h.value.as_str().to_string());

    if matches!(method, Method::Head) {
        if let Some((start, end)) = range_value.as_deref().and_then(|r| parse_range(r, size)) {
            let length = end - start + 1;
            let range_crc = crc32_range(&path, start, length).unwrap_or(0);
            let resp = Response::empty(StatusCode(206))
                .with_header(hdr("Content-Length", &length.to_string()))
                .with_header(hdr("Content-Type", "application/octet-stream"))
                .with_header(hdr(
                    "Content-Range",
                    &format!("bytes {}-{}/{}", start, end, size),
                ))
                .with_header(hdr("Accept-Ranges", "bytes"))
                .with_header(hdr("ETag", &etag))
                .with_header(hdr("X-Content-CRC32", &format!("{:08x}", range_crc)))
                .with_header(hdr("Access-Control-Allow-Origin", "*"));
            let _ = req.respond(resp);
            return;
        } else if range_value.is_some() {
            let resp = Response::empty(StatusCode(416))
                .with_header(hdr("Content-Range", &format!("bytes */{}", size)))
                .with_header(hdr("ETag", &etag));
            let _ = req.respond(resp);
            return;
        }
        let resp = Response::empty(StatusCode(200))
            .with_header(hdr("Content-Length", &size.to_string()))
            .with_header(hdr("Content-Type", "application/octet-stream"))
            .with_header(hdr("Accept-Ranges", "bytes"))
            .with_header(hdr("ETag", &etag))
            .with_header(hdr("Access-Control-Allow-Origin", "*"));
        let _ = req.respond(resp);
        return;
    }

    match range_value.as_deref().and_then(|r| parse_range(r, size)) {
        Some((start, end)) => {
            let length = end - start + 1;
            let range_crc = crc32_range(&path, start, length).unwrap_or(0);
            let mut f = match File::open(&path) {
                Ok(f) => f,
                Err(_) => {
                    let _ = req.respond(Response::empty(StatusCode(500)));
                    return;
                }
            };
            if f.seek(SeekFrom::Start(start)).is_err() {
                let _ = req.respond(Response::empty(StatusCode(500)));
                return;
            }
            let reader = f.take(length);
            let resp = Response::new(
                StatusCode(206),
                vec![
                    hdr("Content-Type", "application/octet-stream"),
                    hdr("Content-Length", &length.to_string()),
                    hdr(
                        "Content-Range",
                        &format!("bytes {}-{}/{}", start, end, size),
                    ),
                    hdr("Accept-Ranges", "bytes"),
                    hdr("ETag", &etag),
                    hdr("X-Content-CRC32", &format!("{:08x}", range_crc)),
                    hdr("Access-Control-Allow-Origin", "*"),
                ],
                reader,
                Some(length as usize),
                None,
            );
            let _ = req.respond(resp);
        }
        None if range_value.is_some() => {
            // Header present but unsatisfiable.
            let resp = Response::empty(StatusCode(416))
                .with_header(hdr("Content-Range", &format!("bytes */{}", size)))
                .with_header(hdr("ETag", &etag));
            let _ = req.respond(resp);
        }
        None => {
            let f = match File::open(&path) {
                Ok(f) => f,
                Err(_) => {
                    let _ = req.respond(Response::empty(StatusCode(500)));
                    return;
                }
            };
            let resp = Response::new(
                StatusCode(200),
                vec![
                    hdr("Content-Type", "application/octet-stream"),
                    hdr("Content-Length", &size.to_string()),
                    hdr("Accept-Ranges", "bytes"),
                    hdr("ETag", &etag),
                    hdr("Access-Control-Allow-Origin", "*"),
                ],
                f,
                Some(size as usize),
                None,
            );
            let _ = req.respond(resp);
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        eprintln!(
            "usage: stream_server <root_dir> [port]\n\
             \n\
             Serves files under root_dir over HTTP with:\n\
             - HTTP/1.1 Range requests (200 / 206 / 416)\n\
             - CRC32 content-hash ETag (cached by mtime+size)\n"
        );
        std::process::exit(if args.is_empty() { 2 } else { 0 });
    }
    let root = PathBuf::from(&args[0])
        .canonicalize()
        .with_context(|| format!("root dir {} not found", args[0]))?;
    if !root.is_dir() {
        return Err(anyhow!("{} is not a directory", root.display()));
    }
    let port: u16 = args.get(1).map(|s| s.parse()).unwrap_or(Ok(8080))?;

    let server = Server::http(("0.0.0.0", port))
        .map_err(|e| anyhow!("bind failed on port {}: {}", port, e))?;
    eprintln!("serving {} on http://0.0.0.0:{}", root.display(), port);

    let cache = std::sync::Arc::new(CrcCache::new());
    // Spawn a small worker pool. tiny_http accepts connections on the
    // main thread; we hand each request off to a worker so a slow
    // response doesn't block the next.
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().max(2))
        .unwrap_or(4);
    let (tx, rx) = std::sync::mpsc::channel::<Request>();
    let rx = std::sync::Arc::new(Mutex::new(rx));
    for _ in 0..workers {
        let rx = rx.clone();
        let cache = cache.clone();
        let root = root.clone();
        std::thread::spawn(move || loop {
            let req = {
                let lock = rx.lock().unwrap();
                lock.recv()
            };
            match req {
                Ok(req) => handle_request(req, &root, &cache),
                Err(_) => break,
            }
        });
    }

    for req in server.incoming_requests() {
        if tx.send(req).is_err() {
            break;
        }
    }
    Ok(())
}
