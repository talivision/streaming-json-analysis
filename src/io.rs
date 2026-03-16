use anyhow::{anyhow, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

const MAX_LINES_PER_POLL: usize = 20_000;
const MAX_BYTES_PER_POLL: usize = 16 * 1024 * 1024;
const MAX_FILES_PER_POLL: usize = 2_000;
const MIN_PAR_PARSE_LINES: usize = 128;

#[derive(Debug, Clone, Copy)]
pub struct StreamProgress {
    pub loaded_bytes: u64,
    pub total_bytes: u64,
}

pub struct StreamReader {
    path: PathBuf,
    is_directory: bool,
    offset: u64,
    last_known_len: u64,
    dir_seen: HashSet<PathBuf>,
    dir_loaded_bytes: u64,
    dir_total_bytes: u64,
}

impl StreamReader {
    pub fn new(path: PathBuf) -> Self {
        let is_directory = path.is_dir();
        Self {
            path,
            is_directory,
            offset: 0,
            last_known_len: 0,
            dir_seen: HashSet::new(),
            dir_loaded_bytes: 0,
            dir_total_bytes: 0,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn progress(&self) -> StreamProgress {
        if self.is_directory {
            StreamProgress {
                loaded_bytes: self.dir_loaded_bytes.min(self.dir_total_bytes),
                total_bytes: self.dir_total_bytes,
            }
        } else {
            StreamProgress {
                loaded_bytes: self.offset.min(self.last_known_len),
                total_bytes: self.last_known_len,
            }
        }
    }

    fn current_line_number(&self) -> usize {
        if self.is_directory || self.offset == 0 || !self.path.exists() {
            return 1;
        }
        let Ok(file) = File::open(&self.path) else {
            return 1;
        };
        let mut reader = BufReader::new(file);
        let mut remaining = self.offset;
        let mut buf = vec![0_u8; 8192];
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
        if self.is_directory || !self.path.exists() {
            return false;
        }
        let Ok(file) = File::open(&self.path) else {
            return false;
        };
        let Ok(len) = file.metadata().map(|m| m.len()) else {
            return false;
        };
        if len == 0 {
            return false;
        }
        let mut reader = BufReader::new(file);
        let start = len.saturating_sub(8192);
        if reader.seek(SeekFrom::Start(start)).is_err() {
            return false;
        }
        let mut tail = Vec::new();
        if reader.read_to_end(&mut tail).is_err() {
            return false;
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

    pub fn poll(&mut self) -> Result<Vec<Value>> {
        if self.is_directory {
            return self.poll_directory_parallel();
        }
        self.poll_file_chunk()
    }

    pub fn poll_snapshot_parallel(&mut self) -> Result<Vec<Value>> {
        if self.is_directory {
            return self.poll_directory_parallel();
        }
        self.poll_file_chunk()
    }

    fn poll_file_chunk(&mut self) -> Result<Vec<Value>> {
        if !self.path.exists() {
            self.offset = 0;
            self.last_known_len = 0;
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let len = file.metadata()?.len();
        self.last_known_len = len;
        if len < self.offset {
            self.offset = 0;
        }
        let remaining = len.saturating_sub(self.offset) as usize;
        if remaining == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;

        let read_cap = remaining.min(MAX_BYTES_PER_POLL);
        let mut chunk = vec![0_u8; read_cap];
        let bytes_read = reader.read(&mut chunk)?;
        chunk.truncate(bytes_read);
        if chunk.is_empty() {
            return Ok(Vec::new());
        }

        let mut line_spans = Vec::with_capacity(MAX_LINES_PER_POLL.min(4_096));
        let mut line_start = 0usize;
        let mut consumed = 0usize;
        for (idx, byte) in chunk.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            if line_spans.len() >= MAX_LINES_PER_POLL {
                break;
            }
            line_spans.push((line_start, idx));
            line_start = idx + 1;
            consumed = line_start;
        }

        let at_eof = self.offset + bytes_read as u64 >= len;
        if line_spans.len() < MAX_LINES_PER_POLL && line_start < chunk.len() && at_eof {
            let tail = &chunk[line_start..];
            // Whitespace-only trailing tails can be ignored.
            if tail.iter().all(|b| matches!(*b, b' ' | b'\t' | b'\r')) {
                line_spans.push((line_start, chunk.len()));
                consumed = chunk.len();
            // A parseable final JSON value can be ingested immediately.
            } else if serde_json::from_slice::<Value>(tail).is_ok() {
                line_spans.push((line_start, chunk.len()));
                consumed = chunk.len();
            // Otherwise leave the trailing line unread and retry it next poll.
            } else {
                consumed = line_start;
            }
        }
        if consumed == 0 && !chunk.is_empty() && chunk.len() >= MAX_BYTES_PER_POLL {
            let line_number = self.current_line_number();
            return Err(anyhow!(
                "JSON line in {} line {} exceeded {} bytes before a newline was seen; aborting read",
                self.path.display(),
                line_number,
                MAX_BYTES_PER_POLL
            ));
        }
        let chunk_base_offset = self.offset;
        self.offset += consumed as u64;
        let path_display = self.path.display().to_string();
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
                    path_display,
                    start_byte,
                    end_byte,
                    e,
                    preview
                )
            })?;
            Ok(Some(v))
        };

        let parsed: Vec<Option<Value>> = if line_spans.len() >= MIN_PAR_PARSE_LINES {
            line_spans.par_iter().map(parse_line).collect::<Result<Vec<_>>>()?
        } else {
            line_spans.iter().map(parse_line).collect::<Result<Vec<_>>>()?
        };
        Ok(parsed.into_iter().flatten().collect())
    }

    fn poll_directory_parallel(&mut self) -> Result<Vec<Value>> {
        if !self.path.exists() {
            self.dir_seen.clear();
            self.dir_loaded_bytes = 0;
            self.dir_total_bytes = 0;
            return Ok(Vec::new());
        }

        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            files.push(path);
        }
        files.sort();
        self.dir_total_bytes = files
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum::<u64>();

        let mut new_files = Vec::new();
        for path in files {
            if self.dir_seen.contains(&path) {
                continue;
            }
            new_files.push(path);
            if new_files.len() >= MAX_FILES_PER_POLL {
                break;
            }
        }
        if new_files.is_empty() {
            self.dir_loaded_bytes = self.dir_total_bytes;
            return Ok(Vec::new());
        }

        let mut parsed = new_files
            .par_iter()
            .map(parse_directory_file)
            .collect::<Result<Vec<_>>>()?;
        parsed.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });

        let mut out = Vec::new();
        for (_ts_key, path_key, _seq, values, bytes) in parsed {
            if self.dir_seen.insert(path_key) {
                self.dir_loaded_bytes = self.dir_loaded_bytes.saturating_add(bytes);
            }
            out.extend(values);
        }
        Ok(out)
    }
}

fn parse_directory_file(path: &PathBuf) -> Result<(f64, PathBuf, usize, Vec<Value>, u64)> {
    let bytes = std::fs::read(path)?;
    let file_len = bytes.len() as u64;
    let text = String::from_utf8_lossy(&bytes);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok((f64::INFINITY, path.clone(), 0, Vec::new(), file_len));
    }

    if trimmed.starts_with('[') {
        let arr: Vec<Value> = serde_json::from_str(trimmed)
            .map_err(|e| anyhow!("Invalid JSON array in {}: {e}", path.display()))?;
        let ts_key = arr
            .iter()
            .filter_map(extract_timestamp_millis)
            .min_by(|a, b| a.total_cmp(b))
            .unwrap_or(f64::INFINITY);
        return Ok((ts_key, path.clone(), 0, arr, file_len));
    }

    let mut values = Vec::new();
    if trimmed.starts_with('{') && !trimmed.contains('\n') {
        let v: Value = serde_json::from_str(trimmed)
            .map_err(|e| anyhow!("Invalid JSON object in {}: {e}", path.display()))?;
        let ts_key = extract_timestamp_millis(&v).unwrap_or(f64::INFINITY);
        return Ok((ts_key, path.clone(), 0, vec![v], file_len));
    }

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line)
            .map_err(|e| anyhow!("Invalid JSON line in {}: {e}", path.display()))?;
        values.push(v);
    }
    let ts_key = values
        .iter()
        .filter_map(extract_timestamp_millis)
        .min_by(|a, b| a.total_cmp(b))
        .unwrap_or(f64::INFINITY);
    Ok((ts_key, path.clone(), 0, values, file_len))
}

fn extract_timestamp_millis(v: &Value) -> Option<f64> {
    let raw = v.get("_timestamp")?;
    if let Some(n) = raw.as_i64() {
        return Some(n as f64);
    }
    if let Some(n) = raw.as_u64() {
        return Some(n as f64);
    }
    if let Some(n) = raw.as_f64() {
        return Some(n);
    }
    None
}
