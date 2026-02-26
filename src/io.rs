use anyhow::{anyhow, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

const MAX_LINES_PER_POLL: usize = 20_000;
const MAX_BYTES_PER_POLL: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES_PER_POLL: usize = 8 * 1024 * 1024;
const MAX_SNAPSHOT_LINES_PER_POLL: usize = 8_000;
const MIN_PAR_PARSE_LINES: usize = 128;

#[derive(Debug, Clone, Copy)]
pub struct StreamProgress {
    pub loaded_bytes: u64,
    pub total_bytes: u64,
}

pub struct StreamReader {
    path: PathBuf,
    offset: u64,
    last_known_len: u64,
}

impl StreamReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            last_known_len: 0,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn progress(&self) -> StreamProgress {
        StreamProgress {
            loaded_bytes: self.offset.min(self.last_known_len),
            total_bytes: self.last_known_len,
        }
    }

    pub fn poll(&mut self) -> Result<Vec<Value>> {
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

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;

        let remaining = len.saturating_sub(self.offset) as usize;
        if remaining == 0 {
            return Ok(Vec::new());
        }

        let read_cap = remaining.min(MAX_BYTES_PER_POLL);
        let mut chunk = vec![0_u8; read_cap];
        let bytes_read = reader.read(&mut chunk)?;
        chunk.truncate(bytes_read);

        if chunk.is_empty() {
            return Ok(Vec::new());
        }

        let at_eof = self.offset + bytes_read as u64 >= len;
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

        if line_spans.len() < MAX_LINES_PER_POLL && at_eof && line_start < chunk.len() {
            line_spans.push((line_start, chunk.len()));
            consumed = chunk.len();
        }

        // Avoid stalling forever on a very long unterminated line.
        if consumed == 0 && !chunk.is_empty() {
            consumed = chunk.len();
        }

        self.offset += consumed as u64;

        let parse_line = |(start, end): &(usize, usize)| -> Result<Option<Value>> {
            let slice = &chunk[*start..*end];
            if slice.iter().all(|b| matches!(*b, b' ' | b'\t' | b'\r')) {
                return Ok(None);
            }
            match serde_json::from_slice::<Value>(slice) {
                Ok(v) => Ok(Some(v)),
                Err(e) => {
                    let preview = String::from_utf8_lossy(&slice[..slice.len().min(120)]);
                    Err(anyhow!(
                        "Invalid JSON line — single object spread across multiple lines? {e}. Line: {preview:?}"
                    ))
                }
            }
        };

        let opts: Vec<Option<Value>> = if line_spans.len() >= MIN_PAR_PARSE_LINES {
            line_spans.par_iter().map(parse_line).collect::<Result<Vec<_>>>()?
        } else {
            line_spans.iter().map(parse_line).collect::<Result<Vec<_>>>()?
        };
        Ok(opts.into_iter().flatten().collect())
    }

    pub fn poll_snapshot_parallel(&mut self) -> Result<Vec<Value>> {
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
        if self.offset >= len {
            return Ok(Vec::new());
        }

        let snapshot_remaining = len.saturating_sub(self.offset) as usize;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;

        let read_cap = snapshot_remaining.min(MAX_SNAPSHOT_BYTES_PER_POLL);
        let mut chunk = Vec::with_capacity(read_cap);
        reader.take(read_cap as u64).read_to_end(&mut chunk)?;
        if chunk.is_empty() {
            return Ok(Vec::new());
        }

        let mut line_spans = Vec::with_capacity(MAX_SNAPSHOT_LINES_PER_POLL.min(4_096));
        let mut line_start = 0usize;
        let mut consumed = 0usize;
        for (idx, byte) in chunk.iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            if line_spans.len() >= MAX_SNAPSHOT_LINES_PER_POLL {
                break;
            }
            line_spans.push((line_start, idx));
            line_start = idx + 1;
            consumed = line_start;
        }

        let at_snapshot_end = read_cap == snapshot_remaining;
        if line_start < chunk.len() && at_snapshot_end {
            line_spans.push((line_start, chunk.len()));
            consumed = chunk.len();
        }
        if consumed == 0 {
            // Avoid stalling forever on a very long unterminated line.
            consumed = chunk.len();
        }
        self.offset += consumed as u64;

        let parse_line = |(start, end): &(usize, usize)| -> Result<Option<Value>> {
            let slice = &chunk[*start..*end];
            if slice.iter().all(|b| matches!(*b, b' ' | b'\t' | b'\r')) {
                return Ok(None);
            }
            match serde_json::from_slice::<Value>(slice) {
                Ok(v) => Ok(Some(v)),
                Err(e) => {
                    let preview = String::from_utf8_lossy(&slice[..slice.len().min(120)]);
                    Err(anyhow!(
                        "Invalid JSON line — single object spread across multiple lines? {e}. Line: {preview:?}"
                    ))
                }
            }
        };

        let parsed: Vec<Option<Value>> = if line_spans.len() >= MIN_PAR_PARSE_LINES {
            line_spans.par_iter().map(parse_line).collect::<Result<Vec<_>>>()?
        } else {
            line_spans.iter().map(parse_line).collect::<Result<Vec<_>>>()?
        };
        Ok(parsed.into_iter().flatten().collect())
    }
}
