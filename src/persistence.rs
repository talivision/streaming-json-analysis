use crate::domain::ActionPeriod;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const STATE_VERSION: u32 = 1;

#[derive(Debug)]
pub struct RestoredState {
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub current_label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    stream_path: String,
    saved_len: u64,
    prefix_hash_hex: String,
    periods: Vec<ActionPeriod>,
    renames: Vec<TypeRename>,
    current_label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TypeRename {
    type_id: String,
    name: String,
}

pub fn load_state(stream_path: &Path) -> Result<Option<RestoredState>> {
    let state_path = state_path_for_stream(stream_path)?;
    if !state_path.exists() {
        return Ok(None);
    }

    let bytes = std::fs::read(&state_path).with_context(|| {
        format!(
            "failed to read persisted state from {}",
            state_path.display()
        )
    })?;
    let state: PersistedState =
        serde_json::from_slice(&bytes).context("invalid persisted state")?;

    if state.version != STATE_VERSION {
        return Ok(None);
    }
    if state.stream_path != stream_path.to_string_lossy() {
        return Ok(None);
    }

    if !stream_path.exists() {
        return Ok((state.saved_len == 0).then_some(RestoredState {
            periods: state.periods,
            renames: state
                .renames
                .into_iter()
                .map(|r| (r.type_id, r.name))
                .collect(),
            current_label: state.current_label,
        }));
    }

    let len = std::fs::metadata(stream_path)?.len();
    if len < state.saved_len {
        return Ok(None);
    }

    let current_prefix_hash = hash_prefix(stream_path, state.saved_len)?;
    if current_prefix_hash != state.prefix_hash_hex {
        return Ok(None);
    }

    Ok(Some(RestoredState {
        periods: state.periods,
        renames: state
            .renames
            .into_iter()
            .map(|r| (r.type_id, r.name))
            .collect(),
        current_label: state.current_label,
    }))
}

pub fn save_state(
    stream_path: &Path,
    saved_len: u64,
    periods: &[ActionPeriod],
    renames: &[(String, String)],
    current_label: &str,
) -> Result<()> {
    let state_path = state_path_for_stream(stream_path)?;
    if let Some(parent) = state_path.parent() {
        create_dir_all(parent)?;
    }

    let prefix_hash_hex = hash_prefix(stream_path, saved_len)?;
    let state = PersistedState {
        version: STATE_VERSION,
        stream_path: stream_path.to_string_lossy().to_string(),
        saved_len,
        prefix_hash_hex,
        periods: periods.to_vec(),
        renames: renames
            .iter()
            .map(|(type_id, name)| TypeRename {
                type_id: type_id.clone(),
                name: name.clone(),
            })
            .collect(),
        current_label: current_label.to_string(),
    };
    let payload = serde_json::to_vec(&state).context("failed to serialize persisted state")?;

    let mut file = File::create(&state_path)
        .with_context(|| format!("failed to create {}", state_path.display()))?;
    file.write_all(&payload)?;
    Ok(())
}

fn state_path_for_stream(stream_path: &Path) -> Result<PathBuf> {
    let mut hasher = Sha256::new();
    hasher.update(stream_path.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    let id = format!("{:x}", digest);

    Ok(base_state_dir()?.join(format!("{}.json", id)))
}

fn base_state_dir() -> Result<PathBuf> {
    if let Some(dir) = env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(dir).join("json-analyzer"));
    }
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("json-analyzer"));
    }
    Ok(PathBuf::from("/tmp/json-analyzer"))
}

fn hash_prefix(path: &Path, prefix_len: u64) -> Result<String> {
    if prefix_len == 0 || !path.exists() {
        return Ok(EMPTY_SHA256.to_string());
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

// SHA-256 of empty input
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
