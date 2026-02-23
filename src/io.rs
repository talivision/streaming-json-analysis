use anyhow::Result;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

pub struct StreamReader {
    path: PathBuf,
    offset: u64,
}

impl StreamReader {
    pub fn new(path: PathBuf) -> Self {
        Self { path, offset: 0 }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn poll(&mut self) -> Result<Vec<Value>> {
        if !self.path.exists() {
            self.offset = 0;
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let len = file.metadata()?.len();
        if len < self.offset {
            self.offset = 0;
        }

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset))?;

        let mut out = Vec::new();
        let mut line = String::new();
        loop {
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
            if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                out.push(v);
            }
        }
        Ok(out)
    }
}
