use crate::domain::{ActionPeriod, DataFilters, PathOverride};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvent {
    pub ts: f64,
    pub obj: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionExport {
    pub stream_path: String,
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub known_unrelated_types: Vec<String>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    pub current_label: String,
    pub event_filters: DataFilters,
    pub stashed_event_filters: Option<DataFilters>,
    pub types_filter: String,
    pub profile: Option<SourceProfile>,
    pub events: Vec<SessionEvent>,
    pub baseline_events: Vec<SessionEvent>,
}

impl SessionExport {
    pub fn new(stream_path: String) -> Self {
        Self {
            stream_path,
            periods: Vec::new(),
            renames: Vec::new(),
            known_unrelated_types: Vec::new(),
            normalized_field_overrides: Vec::new(),
            current_label: String::new(),
            event_filters: DataFilters::default(),
            stashed_event_filters: None,
            types_filter: String::new(),
            profile: None,
            events: Vec::new(),
            baseline_events: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceProfile {
    pub renames: Vec<(String, String)>,
    pub known_unrelated_types: Vec<String>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    pub negative_filters: DataFilters,
    pub whitelist_terms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NormalizedFieldOverride {
    pub type_id: String,
    pub path: String,
    pub mode: PathOverride,
}

#[derive(Debug)]
pub struct RestoredState {
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub known_unrelated_types: Vec<String>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    pub current_label: String,
    pub event_filters: DataFilters,
    pub stashed_event_filters: Option<DataFilters>,
    pub types_filter: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    stream_path: String,
    saved_len: u64,
    prefix_hash_hex: String,
    periods: Vec<ActionPeriod>,
    renames: Vec<TypeRename>,
    #[serde(default)]
    known_unrelated_types: Vec<String>,
    #[serde(default)]
    normalized_field_overrides: Vec<NormalizedFieldOverride>,
    current_label: String,
    event_filters: DataFilters,
    stashed_event_filters: Option<DataFilters>,
    types_filter: String,
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
            known_unrelated_types: state.known_unrelated_types,
            normalized_field_overrides: state.normalized_field_overrides,
            current_label: state.current_label,
            event_filters: state.event_filters,
            stashed_event_filters: state.stashed_event_filters,
            types_filter: state.types_filter,
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
        known_unrelated_types: state.known_unrelated_types,
        normalized_field_overrides: state.normalized_field_overrides,
        current_label: state.current_label,
        event_filters: state.event_filters,
        stashed_event_filters: state.stashed_event_filters,
        types_filter: state.types_filter,
    }))
}

/// Writes a state file that `load_state` will always reject, ensuring no state
/// can be restored for this stream path in the next session.
pub fn invalidate_state(stream_path: &Path) -> Result<()> {
    let state_path = state_path_for_stream(stream_path)?;
    if !state_path.exists() {
        return Ok(());
    }
    if let Some(parent) = state_path.parent() {
        create_dir_all(parent)?;
    }
    let state = PersistedState {
        version: STATE_VERSION,
        stream_path: stream_path.to_string_lossy().to_string(),
        saved_len: 0,
        prefix_hash_hex: String::new(), // never matches any real SHA-256 output
        periods: vec![],
        renames: vec![],
        known_unrelated_types: vec![],
        normalized_field_overrides: vec![],
        current_label: String::new(),
        event_filters: DataFilters::default(),
        stashed_event_filters: None,
        types_filter: String::new(),
    };
    let payload = serde_json::to_vec(&state).context("failed to serialize invalidated state")?;
    let mut file = File::create(&state_path)
        .with_context(|| format!("failed to create {}", state_path.display()))?;
    file.write_all(&payload)?;
    Ok(())
}

pub fn save_state(
    stream_path: &Path,
    saved_len: u64,
    periods: &[ActionPeriod],
    renames: &[(String, String)],
    known_unrelated_types: &[String],
    normalized_field_overrides: &[NormalizedFieldOverride],
    current_label: &str,
    event_filters: &DataFilters,
    stashed_event_filters: Option<&DataFilters>,
    types_filter: &str,
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
        known_unrelated_types: known_unrelated_types.to_vec(),
        normalized_field_overrides: normalized_field_overrides.to_vec(),
        current_label: current_label.to_string(),
        event_filters: event_filters.clone(),
        stashed_event_filters: stashed_event_filters.cloned(),
        types_filter: types_filter.to_string(),
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

pub fn export_session(path: &Path, session: &SessionExport) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec_pretty(session).context("failed to serialize session export")?;
    let mut file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(&payload)?;
    Ok(())
}

pub fn import_session(path: &Path) -> Result<SessionExport> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read session export {}", path.display()))?;
    let session: SessionExport =
        serde_json::from_slice(&bytes).context("invalid session export payload")?;
    Ok(session)
}

pub fn load_profile(path: &Path) -> Result<SourceProfile> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read profile {}", path.display()))?;
    let profile: SourceProfile =
        serde_json::from_slice(&bytes).context("invalid source profile payload")?;
    Ok(profile)
}

pub fn save_profile(path: &Path, profile: &SourceProfile) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec_pretty(profile).context("failed to serialize profile")?;
    let mut file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(&payload)?;
    Ok(())
}
