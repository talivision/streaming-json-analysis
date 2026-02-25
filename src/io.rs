use anyhow::Result;
use rayon::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

const MAX_LINES_PER_POLL: usize = 2_000;
const MIN_PAR_PARSE_LINES: usize = 256;

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

        let mut raw_lines = Vec::with_capacity(MAX_LINES_PER_POLL);
        let mut line = String::new();
        loop {
            if raw_lines.len() >= MAX_LINES_PER_POLL {
                break;
            }
            line.clear();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            self.offset += bytes as u64;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            raw_lines.push(trimmed.to_string());
        }
        let out = if raw_lines.len() >= MIN_PAR_PARSE_LINES {
            raw_lines
                .par_iter()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .collect()
        } else {
            raw_lines
                .iter()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .collect()
        };
        Ok(out)
    }
}
