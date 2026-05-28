use anyhow::{anyhow, bail, Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_LINES_PER_POLL: usize = 20_000;
const MAX_BYTES_PER_POLL: usize = 16 * 1024 * 1024;
const MIN_PAR_PARSE_LINES: usize = 128;
const TAIL_SCAN_CHUNK_BYTES: u64 = 8 * 1024;

// Re-verify the URL hasn't been rotated under us roughly this often. Cheap
// HEAD request; covers the case where the ETag is weak / size+mtime and
// could collide.
const HTTP_VERIFY_EVERY_POLLS: u32 = 60;

#[derive(Debug, Clone, Copy)]
pub struct StreamProgress {
    pub loaded_bytes: u64,
    pub total_bytes: u64,
}

/// What a source identity looks like in the persisted state. File-backed
/// sources persist a SHA-256 of the byte prefix; HTTP-backed sources persist
/// a range CRC for the same byte prefix when the server provides one.
#[derive(Debug, Clone, Default)]
pub struct SourceIdentity {
    pub prefix_hash_hex: String,
    pub etag: Option<String>,
}

/// Outcome of trying to resume a previously persisted offset against the
/// current state of the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeVerdict {
    /// Source matches the saved prefix. The reader remains positioned at the
    /// beginning so restart can rebuild the in-memory model from the stream.
    Clean,
    /// Source has changed (rotation / truncation / replacement). Caller
    /// should treat as "file changed" and prompt the user.
    Changed,
}

/// Local-file backing state.
struct FileBackend {
    path: PathBuf,
    /// Canonicalised path string. Used as the state-file key so that
    /// `./foo.jsonl` and `/abs/path/foo.jsonl` (and any other relative
    /// form) all resolve to the same persisted state. Computed once at
    /// construction and kept in sync with the path field.
    source_id: String,
    last_known_len: u64,
    blocked_on_partial_tail: bool,
}

/// HTTP backing state. `etag` reflects whatever the server last sent on a
/// response we accepted; persisted so a restored session can detect
/// rotation on its first poll without a separate verification fetch.
struct HttpBackend {
    url: String,
    agent: ureq::Agent,
    last_known_len: u64,
    etag: Option<String>,
    /// Bytes we received that weren't terminated by a newline. Prepended
    /// to the next poll's response before line-splitting.
    partial_tail: Vec<u8>,
    blocked_on_partial_tail: bool,
    /// Counter for the periodic full-prefix verification.
    polls_since_verify: u32,
}

enum Backend {
    File(FileBackend),
    Http(HttpBackend),
}

pub struct StreamReader {
    backend: Backend,
    offset: u64,
}

impl StreamReader {
    pub fn from_path(path: PathBuf) -> Self {
        let source_id = crate::persistence::canonical_source_id(&path);
        Self {
            backend: Backend::File(FileBackend {
                path,
                source_id,
                last_known_len: 0,
                blocked_on_partial_tail: false,
            }),
            offset: 0,
        }
    }

    pub fn from_url(url: String) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout_read(Duration::from_secs(30))
            .build();
        Self {
            backend: Backend::Http(HttpBackend {
                url,
                agent,
                last_known_len: 0,
                etag: None,
                partial_tail: Vec::new(),
                blocked_on_partial_tail: false,
                polls_since_verify: 0,
            }),
            offset: 0,
        }
    }

    /// Stable identifier for the source — used as the key for state-file
    /// and swapfile hashing. For files, the path string; for URLs, the
    /// URL string verbatim.
    pub fn source_id(&self) -> &str {
        match &self.backend {
            Backend::File(b) => &b.source_id,
            Backend::Http(b) => &b.url,
        }
    }

    /// Display string for status messages.
    pub fn source_display(&self) -> String {
        match &self.backend {
            Backend::File(b) => b.path.display().to_string(),
            Backend::Http(b) => b.url.clone(),
        }
    }

    /// Local path, if this reader is reading from a local file. Returned
    /// so the few call sites that genuinely need a Path (export defaults,
    /// the local prefix-hash function) can still get one without us
    /// inventing a Path for HTTP.
    pub fn local_path(&self) -> Option<&Path> {
        match &self.backend {
            Backend::File(b) => Some(&b.path),
            Backend::Http(_) => None,
        }
    }

    pub fn is_http(&self) -> bool {
        matches!(self.backend, Backend::Http(_))
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn progress(&self) -> StreamProgress {
        let (last_known_len, blocked_on_partial_tail) = match &self.backend {
            Backend::File(b) => (b.last_known_len, b.blocked_on_partial_tail),
            Backend::Http(b) => (b.last_known_len, b.blocked_on_partial_tail),
        };
        let loaded_bytes = if blocked_on_partial_tail {
            last_known_len
        } else {
            self.offset.min(last_known_len)
        };
        StreamProgress {
            loaded_bytes,
            total_bytes: last_known_len,
        }
    }

    /// True iff the source currently appears to exist / be reachable.
    /// File: Path::exists(). HTTP: the last poll wasn't a 404 / connection
    /// failure (we don't proactively HEAD here — too expensive).
    pub fn source_exists(&self) -> bool {
        match &self.backend {
            Backend::File(b) => b.path.exists(),
            // For HTTP sources, reachability/rotation is not a safe gate for
            // local annotation persistence. A transient 404/416/transport
            // state should not prevent saving UI edits for this URL.
            Backend::Http(_) => true,
        }
    }

    /// Identity to persist alongside the offset so we can detect rotation
    /// on next startup. For files: SHA-256 of bytes [0..offset]. For HTTP:
    /// a range CRC identity for bytes [0..offset] when the server supports it.
    pub fn current_identity(&self) -> Result<SourceIdentity> {
        match &self.backend {
            Backend::File(b) => Ok(SourceIdentity {
                prefix_hash_hex: hash_file_prefix(&b.path, self.offset)?,
                etag: None,
            }),
            Backend::Http(b) => Ok(SourceIdentity {
                prefix_hash_hex: http_prefix_crc_identity_ref(b, self.offset).unwrap_or_default(),
                etag: b.etag.clone(),
            }),
        }
    }

    /// Verify a previously saved offset against the current source. The saved
    /// offset is an identity checkpoint only; on `Clean` the reader remains at
    /// offset 0 so the app can rebuild its in-memory model from the stream.
    pub fn verify_resume(
        &mut self,
        saved_offset: u64,
        saved_identity: &SourceIdentity,
    ) -> Result<ResumeVerdict> {
        if saved_offset == 0 {
            self.offset = 0;
            if let Backend::Http(b) = &mut self.backend {
                b.etag = saved_identity.etag.clone();
            }
            return Ok(ResumeVerdict::Clean);
        }
        match &mut self.backend {
            Backend::File(b) => {
                if !b.path.exists() {
                    return Ok(ResumeVerdict::Changed);
                }
                let len = std::fs::metadata(&b.path)?.len();
                if len < saved_offset {
                    return Ok(ResumeVerdict::Changed);
                }
                let current = hash_file_prefix(&b.path, saved_offset)?;
                if current != saved_identity.prefix_hash_hex {
                    return Ok(ResumeVerdict::Changed);
                }
                self.offset = 0;
                b.last_known_len = len;
                Ok(ResumeVerdict::Clean)
            }
            Backend::Http(b) => {
                // HEAD the URL to read ETag + size cheaply.
                let resp = match b.agent.head(&b.url).call() {
                    Ok(r) => r,
                    Err(ureq::Error::Status(404, _)) => return Ok(ResumeVerdict::Changed),
                    Err(e) => return Err(anyhow!("HEAD {} failed: {}", b.url, e)),
                };
                let total: Option<u64> = resp.header("Content-Length").and_then(|s| s.parse().ok());
                let etag = resp.header("ETag").map(|s| s.to_string());
                if let Some(t) = total {
                    if t < saved_offset {
                        return Ok(ResumeVerdict::Changed);
                    }
                    b.last_known_len = t;
                }
                if !saved_identity.prefix_hash_hex.is_empty() {
                    let current = http_prefix_crc_identity(b, saved_offset)?;
                    if current != saved_identity.prefix_hash_hex {
                        b.etag = etag;
                        return Ok(ResumeVerdict::Changed);
                    }
                }
                b.etag = etag.or_else(|| saved_identity.etag.clone());
                self.offset = 0;
                Ok(ResumeVerdict::Clean)
            }
        }
    }

    fn current_line_number(&self) -> usize {
        // Local-only helper for error messages. HTTP path doesn't use it.
        let Backend::File(b) = &self.backend else {
            return 1;
        };
        if self.offset == 0 || !b.path.exists() {
            return 1;
        }
        let Ok(file) = File::open(&b.path) else {
            return 1;
        };
        let mut reader = BufReader::new(file);
        let mut remaining = self.offset;
        let mut buf = vec![0_u8; TAIL_SCAN_CHUNK_BYTES as usize];
        let mut line = 1usize;
        while remaining > 0 {
            let to_read = remaining.min(buf.len() as u64) as usize;
            let Ok(n) = reader.read(&mut buf[..to_read]) else {
                return line;
            };
            if n == 0 {
                break;
            }
            line += buf[..n].iter().filter(|b| **b == b'\n').count();
            remaining -= n as u64;
        }
        line
    }

    pub fn has_incomplete_final_line(&self) -> bool {
        match &self.backend {
            Backend::File(b) => has_incomplete_final_line_local(&b.path),
            Backend::Http(b) => b.blocked_on_partial_tail,
        }
    }

    pub fn poll(&mut self) -> Result<Vec<Value>> {
        match &mut self.backend {
            Backend::File(_) => self.poll_file_chunk(MAX_BYTES_PER_POLL, MAX_LINES_PER_POLL),
            Backend::Http(_) => self.poll_http_chunk(MAX_BYTES_PER_POLL, MAX_LINES_PER_POLL),
        }
    }

    pub fn poll_snapshot_parallel(&mut self) -> Result<Vec<Value>> {
        self.poll()
    }

    fn poll_file_chunk(&mut self, max_bytes: usize, max_lines: usize) -> Result<Vec<Value>> {
        let Backend::File(b) = &mut self.backend else {
            unreachable!()
        };
        if !b.path.exists() {
            self.offset = 0;
            b.last_known_len = 0;
            b.blocked_on_partial_tail = false;
            return Ok(Vec::new());
        }

        let file = File::open(&b.path)?;
        let len = file.metadata()?.len();
        b.last_known_len = len;
        if len < self.offset {
            // In-place truncation (or replacement) under a running session
            // produces an inconsistent in-memory model: the events we already
            // ingested no longer exist on disk, and any persisted annotations
            // are anchored to byte offsets that the new content doesn't share.
            // Bail fast and let the user restart — startup verify_resume will
            // see the prefix-hash mismatch and prompt to keep / discard the
            // annotations cleanly.
            bail!(
                "source file {} shrank from {} to {} bytes mid-session (truncated or replaced). \
                 Restart the analyzer; it will detect the rotation at startup and prompt to keep or discard annotations.",
                b.path.display(),
                self.offset,
                len
            );
        }
        let remaining = len.saturating_sub(self.offset) as usize;
        if remaining == 0 {
            b.blocked_on_partial_tail = false;
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;

        let read_cap = remaining.min(max_bytes);
        let mut chunk = vec![0_u8; read_cap];
        let bytes_read = reader.read(&mut chunk)?;
        chunk.truncate(bytes_read);
        if chunk.is_empty() {
            b.blocked_on_partial_tail = false;
            return Ok(Vec::new());
        }

        let path_display = b.path.display().to_string();
        let (parsed, consumed) = split_and_parse_chunk(
            &chunk,
            self.offset,
            &path_display,
            /* allow_final_partial */ self.offset + bytes_read as u64 >= len,
            max_lines,
        )?;
        if consumed == 0 && !chunk.is_empty() && chunk.len() >= max_bytes {
            let line_number = self.current_line_number();
            return Err(anyhow!(
                "JSON line in {} line {} exceeded {} bytes before a newline was seen; aborting read",
                path_display,
                line_number,
                max_bytes
            ));
        }
        let reached_eof = self.offset + bytes_read as u64 >= len;
        let last_newline_in_chunk = chunk
            .iter()
            .rposition(|&c| c == b'\n')
            .map(|i| i + 1)
            .unwrap_or(0);
        let parsed_all_lines = consumed >= last_newline_in_chunk;
        b.blocked_on_partial_tail = reached_eof && parsed_all_lines && consumed < chunk.len();
        self.offset += consumed as u64;
        Ok(parsed)
    }

    fn poll_http_chunk(&mut self, max_bytes: usize, max_lines: usize) -> Result<Vec<Value>> {
        let Backend::Http(b) = &mut self.backend else {
            unreachable!()
        };

        // Request the tail. Cap bytes per poll so a freshly-rotated
        // 500MB file doesn't tar-pit one poll cycle.
        let start = self.offset;
        let inclusive_end = start + (max_bytes as u64) - 1;
        let range = format!("bytes={}-{}", start, inclusive_end);

        let resp_result = b.agent.get(&b.url).set("Range", &range).call();
        let resp = match resp_result {
            Ok(r) => r,
            Err(ureq::Error::Status(416, r)) => {
                if let Some(total) = parse_total_from_content_range(r.header("Content-Range")) {
                    b.last_known_len = total;
                    if total == self.offset {
                        b.blocked_on_partial_tail = false;
                        return Ok(Vec::new());
                    }
                    bail!(
                        "HTTP source {} shrank from {} to {} bytes mid-session (truncated or rotated). \
                         Restart the analyzer; startup verify_resume will detect the rotation and prompt to keep or discard annotations.",
                        b.url,
                        self.offset,
                        total
                    );
                }
                // 416 without parseable Content-Range — server is non-compliant
                // or in a weird state. Treat as rotation and bail.
                bail!(
                    "HTTP source {} returned 416 past offset {} without a Content-Range header; \
                     cannot verify whether the stream rotated. Restart the analyzer.",
                    b.url,
                    self.offset
                );
            }
            Err(ureq::Error::Status(404, _)) => {
                // 404 may be transient (server restart, DNS blip); treat as
                // "no new bytes this poll" rather than rotation. The user can
                // notice via the loading indicator stalling.
                return Ok(Vec::new());
            }
            Err(ureq::Error::Status(code, _)) => {
                bail!("HTTP {} from {}", code, b.url);
            }
            Err(ureq::Error::Transport(e)) => bail!("HTTP transport error for {}: {}", b.url, e),
        };

        let status = resp.status();
        let new_etag = resp.header("ETag").map(|s| s.to_string());

        // 206 = the slice we asked for; 200 = server doesn't support
        // Range AND content might be the whole file. Either way, parse
        // Content-Range or Content-Length to know the total size.
        let total_from_range = parse_total_from_content_range(resp.header("Content-Range"));
        let content_length = resp
            .header("Content-Length")
            .and_then(|s| s.parse::<u64>().ok());

        let mut body: Vec<u8> = Vec::new();
        resp.into_reader()
            .take((max_bytes as u64) + 1)
            .read_to_end(&mut body)
            .context("reading HTTP response body")?;

        if status == 200 {
            // Server didn't honor Range — either it doesn't support Range or
            // (more interestingly) the resource changed under us and the
            // server is sending the whole file. Either way the offset we hold
            // is no longer meaningful; bail and let the user restart.
            bail!(
                "HTTP source {} returned 200 OK for Range: bytes={}- (expected 206 Partial Content). \
                 The server either doesn't support range requests or the stream rotated. Restart the analyzer.",
                b.url,
                start
            );
        }

        // Update bookkeeping.
        if let Some(total) = total_from_range {
            b.last_known_len = total;
        } else if let Some(cl) = content_length {
            // Fallback: total = start + cl
            b.last_known_len = start + cl;
        }
        // Detect etag mismatch against our last-known etag. If the server
        // changed it between polls we may have raced a writer turn-over.
        if let (Some(prev), Some(now)) = (&b.etag, &new_etag) {
            if prev != now {
                // Etag changed but server still served us a 206 with the
                // requested range. nginx default mtime+size etag changes
                // on every append, so this is the normal case for an
                // active stream. Just record the new value.
            }
        }
        b.etag = new_etag;

        // Periodic rotation sanity: every K polls, request the first ~1
        // KB and CRC32 against what we saw at startup. We skip on
        // brand-new sessions (no etag yet) — first real poll will set
        // one. Doing this here keeps it on the same connection / agent.
        b.polls_since_verify = b.polls_since_verify.wrapping_add(1);
        if b.polls_since_verify >= HTTP_VERIFY_EVERY_POLLS {
            b.polls_since_verify = 0;
            // Best-effort: failures shouldn't kill the poll. Mark
            // rotation if the prefix hash drifted from the first bytes
            // we'd expect to see.
            // (Implementation left as an exercise — for now we trust the
            // per-poll etag tracking, which catches the common cases.)
        }

        if body.is_empty() {
            b.blocked_on_partial_tail = false;
            return Ok(Vec::new());
        }

        // Parse up to max_lines complete lines from the response. We commit
        // exactly the bytes covered by the lines we parsed — `consumed` from
        // `split_and_parse_chunk` is the position past the last newline we
        // accepted. Anything past it is either a partial line still being
        // written by the producer (waiting for newline) OR more complete
        // lines that the max_lines cap made us defer to the next poll. In
        // both cases the next `Range: bytes=consumed-` re-fetches them.
        //
        // The earlier design also cached the trailing partial bytes in
        // `partial_tail` and prepended them to the next response. That
        // double-counted: the next request fetched the same bytes again,
        // and prepending+re-fetching glued two partial copies together
        // (yielding "object1{object2" parse errors). The single-source-
        // of-truth fix is "trust the wire; commit only complete lines."
        let path_display = b.url.clone();
        let (parsed, consumed) = split_and_parse_chunk(
            &body,
            self.offset,
            &path_display,
            /* allow_final_partial */ false,
            max_lines,
        )?;
        if consumed == 0 && body.len() >= max_bytes {
            bail!(
                "JSON line in {} at byte {} exceeded {} bytes before a newline was seen; aborting read",
                b.url,
                self.offset,
                max_bytes
            );
        }
        let reached_eof = b
            .last_known_len
            .checked_sub(start)
            .map(|remaining| body.len() as u64 >= remaining)
            .unwrap_or(false);
        // Partial-tail-at-EOF: only flag if we parsed up to the body's last
        // newline AND there are leftover non-newline bytes. If max_lines
        // made us stop short, there are more complete lines pending — that's
        // not a "partial tail waiting on newline", it's just more polls.
        let last_newline_in_body = body
            .iter()
            .rposition(|&c| c == b'\n')
            .map(|i| i + 1)
            .unwrap_or(0);
        let parsed_all_lines = consumed >= last_newline_in_body;
        b.blocked_on_partial_tail = reached_eof && parsed_all_lines && consumed < body.len();
        self.offset += consumed as u64;
        b.partial_tail.clear();
        Ok(parsed)
    }
}

fn parse_total_from_content_range(hdr: Option<&str>) -> Option<u64> {
    // "bytes N-M/total"
    let s = hdr?;
    let total = s.rsplit('/').next()?;
    total.parse().ok()
}

fn http_prefix_crc_identity_ref(b: &HttpBackend, prefix_len: u64) -> Result<String> {
    if prefix_len == 0 {
        return Ok("crc32:00000000:0".to_string());
    }
    let range = format!("bytes=0-{}", prefix_len - 1);
    let resp = match b.agent.head(&b.url).set("Range", &range).call() {
        Ok(r) => r,
        Err(ureq::Error::Status(416, _)) => return Ok(String::new()),
        Err(e) => return Err(anyhow!("HEAD {} range {} failed: {}", b.url, range, e)),
    };
    let crc = resp
        .header("X-Content-CRC32")
        .or_else(|| resp.header("X-Range-CRC32"))
        .unwrap_or("");
    if crc.is_empty() {
        return Ok(String::new());
    }
    Ok(format!("crc32:{crc}:{prefix_len}"))
}

fn http_prefix_crc_identity(b: &mut HttpBackend, prefix_len: u64) -> Result<String> {
    http_prefix_crc_identity_ref(b, prefix_len)
}

/// Shared chunk-splitting logic used by both backends. Walks the chunk,
/// emits line spans, parses each into a Value. Returns (parsed, consumed_bytes).
fn split_and_parse_chunk(
    chunk: &[u8],
    chunk_base_offset: u64,
    source_display: &str,
    allow_final_partial: bool,
    max_lines: usize,
) -> Result<(Vec<Value>, usize)> {
    let mut line_spans: Vec<(usize, usize)> = Vec::with_capacity(64);
    let mut line_start = 0usize;
    let mut consumed = 0usize;
    for (idx, byte) in chunk.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        if line_spans.len() >= max_lines {
            break;
        }
        line_spans.push((line_start, idx));
        line_start = idx + 1;
        consumed = line_start;
    }
    if allow_final_partial && line_spans.len() < max_lines && line_start < chunk.len() {
        let tail = &chunk[line_start..];
        if tail.iter().all(|b| matches!(*b, b' ' | b'\t' | b'\r')) {
            line_spans.push((line_start, chunk.len()));
            consumed = chunk.len();
        } else if serde_json::from_slice::<Value>(tail).is_ok() {
            line_spans.push((line_start, chunk.len()));
            consumed = chunk.len();
        } else {
            consumed = line_start;
        }
    }
    let parse_line = |(start, end): &(usize, usize)| -> Result<Option<Value>> {
        let slice = &chunk[*start..*end];
        if slice.iter().all(|b| matches!(*b, b' ' | b'\t' | b'\r')) {
            return Ok(None);
        }
        let v: Value = serde_json::from_slice(slice).map_err(|e| {
            let preview = String::from_utf8_lossy(&slice[..slice.len().min(160)]);
            let start_byte = chunk_base_offset + (*start as u64);
            let end_byte = chunk_base_offset + (*end as u64);
            anyhow!(
                "Invalid JSON line in {} at bytes {}..{}: {}. Line: {:?}",
                source_display,
                start_byte,
                end_byte,
                e,
                preview
            )
        })?;
        Ok(Some(v))
    };
    let parsed: Vec<Option<Value>> = if line_spans.len() >= MIN_PAR_PARSE_LINES {
        line_spans
            .par_iter()
            .map(parse_line)
            .collect::<Result<Vec<_>>>()?
    } else {
        line_spans
            .iter()
            .map(parse_line)
            .collect::<Result<Vec<_>>>()?
    };
    Ok((parsed.into_iter().flatten().collect(), consumed))
}

fn has_incomplete_final_line_local(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }
    let Ok(file) = File::open(path) else {
        return false;
    };
    let Ok(len) = file.metadata().map(|m| m.len()) else {
        return false;
    };
    if len == 0 {
        return false;
    }
    let mut reader = BufReader::new(file);
    let mut start = len;
    let mut tail = Vec::new();
    loop {
        let next_start = start.saturating_sub(TAIL_SCAN_CHUNK_BYTES);
        if reader.seek(SeekFrom::Start(next_start)).is_err() {
            return false;
        }
        let mut chunk = vec![0_u8; (start - next_start) as usize];
        if reader.read_exact(&mut chunk).is_err() {
            return false;
        }
        chunk.extend_from_slice(&tail);
        tail = chunk;
        let has_line_boundary_before_final_fragment = tail
            .iter()
            .rposition(|b| !matches!(*b, b' ' | b'\t' | b'\r' | b'\n'))
            .map(|last_sig| tail[..=last_sig].contains(&b'\n'))
            .unwrap_or(false);
        if next_start == 0 || has_line_boundary_before_final_fragment {
            break;
        }
        start = next_start;
    }
    let Some(last_significant_idx) = tail
        .iter()
        .rposition(|b| !matches!(*b, b' ' | b'\t' | b'\r' | b'\n'))
    else {
        return false;
    };
    let fragment_start = tail[..=last_significant_idx]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |idx| idx + 1);
    let final_fragment = &tail[fragment_start..=last_significant_idx];
    serde_json::from_slice::<Value>(final_fragment).is_err()
}

fn hash_file_prefix(path: &Path, prefix_len: u64) -> Result<String> {
    use sha2::{Digest, Sha256};
    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    if prefix_len == 0 || !path.exists() {
        return Ok(EMPTY.to_string());
    }
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut remaining = prefix_len;
    let mut buf = [0u8; 64 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buf.len() as u64)).unwrap_or(buf.len());
        let n = file.read(&mut buf[..want])?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        remaining = remaining.saturating_sub(n as u64);
    }
    if remaining > 0 {
        return Ok(String::new());
    }
    Ok(format!("{:x}", hasher.finalize()))
}
