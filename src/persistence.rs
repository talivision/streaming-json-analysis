use crate::domain::{ActionPeriod, DataFilters, MergeGroup, PathOverride};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const STATE_VERSION: u32 = 2;

// ---------------------------------------------------------------------------
// Session / profile exports (offline bundles — unchanged shape)
// ---------------------------------------------------------------------------

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
    #[serde(default)]
    pub merge_groups: Vec<MergeGroup>,
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
            merge_groups: Vec::new(),
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
    #[serde(default)]
    pub merge_groups: Vec<MergeGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NormalizedFieldOverride {
    pub type_id: String,
    pub path: String,
    pub mode: PathOverride,
}

// ---------------------------------------------------------------------------
// Persisted state: one file per stream, single writer guaranteed by the
// swapfile (see `Swapfile`). No locking, no merging — the running process is
// the sole authority over its `<sha>.state.json` until it exits.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedState {
    pub version: u32,
    pub stream_path: String,
    pub saved_len: u64,
    pub prefix_hash_hex: String,
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub known_unrelated_types: Vec<String>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    /// Triaged events identified by (ts, type_id). Vec indices are not stable
    /// across restarts, so we serialise a value identity instead.
    pub triaged_events: Vec<(f64, String)>,
    pub current_label: String,
    pub event_filters: DataFilters,
    pub stashed_event_filters: Option<DataFilters>,
    pub types_filter: String,
    /// User-curated structural type groupings.
    #[serde(default)]
    pub merge_groups: Vec<MergeGroup>,
}

impl PersistedState {
    pub fn empty(stream_path: String) -> Self {
        Self {
            version: STATE_VERSION,
            stream_path,
            saved_len: 0,
            prefix_hash_hex: String::new(),
            periods: Vec::new(),
            renames: Vec::new(),
            known_unrelated_types: Vec::new(),
            normalized_field_overrides: Vec::new(),
            triaged_events: Vec::new(),
            current_label: String::new(),
            event_filters: DataFilters::default(),
            stashed_event_filters: None,
            types_filter: String::new(),
            merge_groups: Vec::new(),
        }
    }
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
    pub triaged_events: Vec<(f64, String)>,
    pub merge_groups: Vec<MergeGroup>,
}

/// Outcome of attempting to load the per-stream state on startup.
pub enum StateLoadResult {
    /// File identity confirmed — full state can be restored.
    Clean(RestoredState),
    /// Stream content changed since the last checkpoint. Periods reference
    /// timestamps from the previous file and shouldn't be applied blindly.
    Changed(RestoredState),
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatePaths {
    pub state: PathBuf,
    pub swap: PathBuf,
    pub dir: PathBuf,
    /// Stable per-stream prefix (SHA-256 hex).
    pub id: String,
}

fn canonical_for_hashing(p: &Path) -> PathBuf {
    if let Ok(c) = std::fs::canonicalize(p) {
        return c;
    }
    if p.is_absolute() {
        return p.to_path_buf();
    }
    if let Ok(cwd) = std::env::current_dir() {
        return cwd.join(p);
    }
    p.to_path_buf()
}

fn hash_path_string(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    format!("{:x}", h.finalize())
}

/// One-time best-effort migration:
/// - Pre-canonicalisation builds hashed the literal input string. The fix to
///   canonicalise the path before hashing changes the SHA, which would
///   orphan all pre-fix state. Rename across when the canonical-keyed file
///   is missing.
/// - Pre-split builds wrote a single `<sha>.state.json` (the format we're
///   returning to). Pre-rewrite builds wrote a `<sha>.shared.json` plus a
///   `<sha>.local.json`. If the new combined file is missing, fold them.
pub fn migrate_legacy_state_paths(stream_path: &Path) -> Result<()> {
    let canonical = canonical_for_hashing(stream_path);
    let literal_id = hash_path_string(&stream_path.to_string_lossy());
    let canonical_id = hash_path_string(&canonical.to_string_lossy());
    let dir = base_state_dir()?;
    if !dir.exists() {
        return Ok(());
    }

    // Step 1: bring literal-keyed files across to canonical-keyed names so
    // the rest of the migration only has to consider one id.
    if literal_id != canonical_id {
        for ext in &[
            "state.json",
            "swap.json",
            "shared.json",
            "local.json",
            "shared.lock",
        ] {
            let legacy = dir.join(format!("{}.{}", literal_id, ext));
            let modern = dir.join(format!("{}.{}", canonical_id, ext));
            if legacy.exists() && !modern.exists() {
                let _ = std::fs::rename(&legacy, &modern);
            }
        }
    }

    // Step 2: fold shared.json + local.json into state.json if the new file
    // is missing. Leaves the legacy files in place so a roll-back to an older
    // build still has something to read.
    let state_path = dir.join(format!("{}.state.json", canonical_id));
    if !state_path.exists() {
        let shared_path = dir.join(format!("{}.shared.json", canonical_id));
        let local_path = dir.join(format!("{}.local.json", canonical_id));
        if shared_path.exists() || local_path.exists() {
            if let Ok(merged) = fold_split_state(&shared_path, &local_path, stream_path) {
                let payload =
                    serde_json::to_vec(&merged).context("failed to serialize migrated state")?;
                let _ = atomic_write(&state_path, &payload);
            }
        }
    }

    // Best-effort cleanup of leftover lock / presence files from the old
    // multi-client build. Failure is non-fatal.
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let lock_name = format!("{}.shared.lock", canonical_id);
        let presence_prefix = format!("{}.presence.", canonical_id);
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == lock_name
                || (name.starts_with(&presence_prefix) && name.ends_with(".json"))
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    Ok(())
}

fn fold_split_state(
    shared: &Path,
    local: &Path,
    stream_path: &Path,
) -> Result<PersistedState> {
    let stream_path_str = stream_path.to_string_lossy().to_string();
    let mut state = PersistedState::empty(stream_path_str);
    if shared.exists() {
        let bytes = std::fs::read(shared)?;
        if !bytes.is_empty() {
            // Old SharedState shape: same fields, no cursor / UI bits.
            #[derive(Deserialize)]
            struct LegacyShared {
                #[serde(default)]
                periods: Vec<ActionPeriod>,
                #[serde(default)]
                renames: Vec<(String, String)>,
                #[serde(default)]
                normalized_field_overrides: Vec<NormalizedFieldOverride>,
                #[serde(default)]
                triaged_events: Vec<(f64, String)>,
                #[serde(default)]
                merge_groups: Vec<MergeGroup>,
            }
            if let Ok(s) = serde_json::from_slice::<LegacyShared>(&bytes) {
                state.periods = s.periods;
                state.renames = s.renames;
                state.normalized_field_overrides = s.normalized_field_overrides;
                state.triaged_events = s.triaged_events;
                state.merge_groups = s.merge_groups;
            }
        }
    }
    if local.exists() {
        let bytes = std::fs::read(local)?;
        if !bytes.is_empty() {
            #[derive(Deserialize)]
            struct LegacyLocal {
                #[serde(default)]
                saved_len: u64,
                #[serde(default)]
                prefix_hash_hex: String,
                #[serde(default)]
                current_label: String,
                #[serde(default)]
                event_filters: DataFilters,
                #[serde(default)]
                stashed_event_filters: Option<DataFilters>,
                #[serde(default)]
                types_filter: String,
                #[serde(default)]
                known_unrelated_types: Vec<String>,
            }
            if let Ok(l) = serde_json::from_slice::<LegacyLocal>(&bytes) {
                state.saved_len = l.saved_len;
                state.prefix_hash_hex = l.prefix_hash_hex;
                state.current_label = l.current_label;
                state.event_filters = l.event_filters;
                state.stashed_event_filters = l.stashed_event_filters;
                state.types_filter = l.types_filter;
                state.known_unrelated_types = l.known_unrelated_types;
            }
        }
    }
    Ok(state)
}

pub fn state_paths_for_stream(stream_path: &Path) -> Result<StatePaths> {
    let canonical = canonical_for_hashing(stream_path);
    let mut hasher = Sha256::new();
    hasher.update(canonical.to_string_lossy().as_bytes());
    let id = format!("{:x}", hasher.finalize());
    let dir = base_state_dir()?;
    Ok(StatePaths {
        state: dir.join(format!("{}.state.json", id)),
        swap: dir.join(format!("{}.swap.json", id)),
        dir,
        id,
    })
}

fn base_state_dir() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        if let Some(dir) = env::var_os("LOCALAPPDATA") {
            return Ok(PathBuf::from(dir).join("json-analyzer"));
        }
        if let Some(dir) = env::var_os("APPDATA") {
            return Ok(PathBuf::from(dir).join("json-analyzer"));
        }
        if let Some(dir) = env::var_os("TEMP") {
            return Ok(PathBuf::from(dir).join("json-analyzer"));
        }
        if let Some(dir) = env::var_os("TMP") {
            return Ok(PathBuf::from(dir).join("json-analyzer"));
        }
    }
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

// ---------------------------------------------------------------------------
// Atomic write helper
// ---------------------------------------------------------------------------

/// Atomically replaces `target` with `payload`. Writes payload to `.tmp`,
/// fsyncs, renames over `target`, then best-effort fsyncs the parent dir so
/// the rename survives a crash.
pub fn atomic_write(target: &Path, payload: &[u8]) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("target {} has no parent", target.display()))?;
    create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;

    let tmp = {
        let mut t = target.to_path_buf();
        let name = target
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("target {} has no filename", target.display()))?
            .to_owned();
        let mut tmp_name = name;
        tmp_name.push(".tmp");
        t.set_file_name(tmp_name);
        t
    };

    {
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            opts.mode(0o600);
        }
        let mut file = opts
            .open(&tmp)
            .with_context(|| format!("failed to open {}", tmp.display()))?;
        file.write_all(payload)
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync {}", tmp.display()))?;
    }

    std::fs::rename(&tmp, target).with_context(|| {
        format!("failed to rename {} -> {}", tmp.display(), target.display())
    })?;

    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// State IO
// ---------------------------------------------------------------------------

pub fn read_state(stream_path: &Path) -> Result<Option<PersistedState>> {
    let paths = state_paths_for_stream(stream_path)?;
    if !paths.state.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&paths.state)
        .with_context(|| format!("failed to read state {}", paths.state.display()))?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let state: PersistedState =
        serde_json::from_slice(&bytes).context("invalid state payload")?;
    if state.version != STATE_VERSION {
        return Ok(None);
    }
    Ok(Some(state))
}

pub fn save_state(stream_path: &Path, state: &PersistedState) -> Result<()> {
    let paths = state_paths_for_stream(stream_path)?;
    let mut adjusted = state.clone();
    adjusted.version = STATE_VERSION;
    if adjusted.stream_path.is_empty() {
        adjusted.stream_path = stream_path.to_string_lossy().to_string();
    }
    let payload = serde_json::to_vec(&adjusted).context("failed to serialize state")?;
    atomic_write(&paths.state, &payload)
}

/// Marks the state so that the next `load_full_state` will treat the stream
/// as gone (saved_len=0, sentinel hash) — used when the stream file
/// disappears mid-session.
pub fn invalidate_state(stream_path: &Path) -> Result<()> {
    let paths = state_paths_for_stream(stream_path)?;
    if !paths.state.exists() {
        return Ok(());
    }
    let mut state = PersistedState::empty(stream_path.to_string_lossy().to_string());
    if let Ok(Some(prev)) = read_state(stream_path) {
        state.current_label = prev.current_label;
        state.event_filters = prev.event_filters;
        state.stashed_event_filters = prev.stashed_event_filters;
        state.types_filter = prev.types_filter;
        state.known_unrelated_types = prev.known_unrelated_types;
        state.renames = prev.renames;
        state.normalized_field_overrides = prev.normalized_field_overrides;
        state.merge_groups = prev.merge_groups;
        state.triaged_events = prev.triaged_events;
    }
    state.saved_len = 0;
    state.prefix_hash_hex = String::new();
    save_state(stream_path, &state)
}

pub fn load_full_state(stream_path: &Path) -> Result<Option<StateLoadResult>> {
    let Some(state) = read_state(stream_path)? else {
        return Ok(None);
    };

    let restored = RestoredState {
        periods: state.periods.clone(),
        renames: state.renames.clone(),
        known_unrelated_types: state.known_unrelated_types.clone(),
        normalized_field_overrides: state.normalized_field_overrides.clone(),
        current_label: state.current_label.clone(),
        event_filters: state.event_filters.clone(),
        stashed_event_filters: state.stashed_event_filters.clone(),
        types_filter: state.types_filter.clone(),
        triaged_events: state.triaged_events.clone(),
        merge_groups: state.merge_groups.clone(),
    };

    // If we never advanced the cursor, treat as clean restore so we can still
    // apply renames/filters to a brand-new file at the same path.
    if state.saved_len == 0 {
        return Ok(Some(StateLoadResult::Clean(restored)));
    }

    if !stream_path.exists() {
        return Ok(Some(StateLoadResult::Changed(restored)));
    }

    let len = std::fs::metadata(stream_path)?.len();
    if len < state.saved_len {
        return Ok(Some(StateLoadResult::Changed(restored)));
    }

    let current_prefix_hash = hash_prefix(stream_path, state.saved_len)?;
    if current_prefix_hash != state.prefix_hash_hex {
        return Ok(Some(StateLoadResult::Changed(restored)));
    }

    Ok(Some(StateLoadResult::Clean(restored)))
}

// ---------------------------------------------------------------------------
// Swapfile: kernel-arbitrated "another instance is editing this" guard.
//
// We open `<sha>.swap.json`, take an exclusive advisory lock on the
// underlying file via `File::try_lock` (flock(2) on Unix, LockFileEx on
// Windows), and keep the handle alive for the lifetime of the process.
// The kernel drops the lock when our File is closed — clean exit, panic,
// OOM, SIGKILL, BSOD — so there's no "stale swap" concept and no PID
// liveness check to do ourselves. The bytes inside the file are purely
// informational, used only to populate the user-facing conflict message.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapfileRecord {
    pub pid: u32,
    pub hostname: String,
    pub stream_path: String,
    pub created_at_secs: u64,
}

#[derive(Debug)]
pub struct SwapfileConflict {
    pub swap_path: PathBuf,
    pub record: SwapfileRecord,
}

#[derive(Debug)]
pub enum SwapfileError {
    Held(SwapfileConflict),
    Io(anyhow::Error),
}

impl std::fmt::Display for SwapfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwapfileError::Held(c) => write!(
                f,
                "swapfile {} held by pid {} on {}",
                c.swap_path.display(),
                c.record.pid,
                c.record.hostname
            ),
            SwapfileError::Io(e) => write!(f, "swapfile io error: {e}"),
        }
    }
}

impl std::error::Error for SwapfileError {}

/// Owns the swapfile lock for the lifetime of the process. Drop releases
/// the lock (kernel-side) and removes the file.
#[derive(Debug)]
pub struct Swapfile {
    path: PathBuf,
    // Held to keep the kernel-side advisory lock alive. Dropped automatically
    // when the Swapfile is dropped; never read directly.
    _file: File,
}

impl Swapfile {
    /// Try to acquire the swapfile for `stream_path`. If another live
    /// process already holds the lock, returns `SwapfileError::Held`.
    /// `force = true` orphans the existing lock (unlinks the file so we
    /// get a fresh inode) and proceeds anyway; use only when you really
    /// do intend to run a second instance alongside the first.
    pub fn acquire(stream_path: &Path, force: bool) -> std::result::Result<Self, SwapfileError> {
        let paths = state_paths_for_stream(stream_path).map_err(SwapfileError::Io)?;
        create_dir_all(&paths.dir)
            .with_context(|| format!("failed to create {}", paths.dir.display()))
            .map_err(SwapfileError::Io)?;

        if force {
            // Unlink the existing swap so our open() lands on a fresh
            // inode. The original holder keeps its fd and its lock on the
            // now-orphaned inode; both processes then proceed
            // independently (which is what `--force` is for).
            let _ = std::fs::remove_file(&paths.swap);
        }

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            opts.mode(0o600);
        }
        let file = opts
            .open(&paths.swap)
            .with_context(|| format!("failed to open {}", paths.swap.display()))
            .map_err(SwapfileError::Io)?;

        match file.try_lock() {
            Ok(()) => {
                // We own the lock. Replace whatever bytes a previously
                // crashed holder left in the file with our own record.
                let record = SwapfileRecord {
                    pid: std::process::id(),
                    hostname: current_hostname(),
                    stream_path: stream_path.to_string_lossy().to_string(),
                    created_at_secs: now_secs(),
                };
                let payload = serde_json::to_vec(&record)
                    .context("failed to serialize swapfile record")
                    .map_err(SwapfileError::Io)?;
                file.set_len(0)
                    .with_context(|| format!("failed to truncate {}", paths.swap.display()))
                    .map_err(SwapfileError::Io)?;
                {
                    let mut writer = &file;
                    writer
                        .write_all(&payload)
                        .with_context(|| format!("failed to write {}", paths.swap.display()))
                        .map_err(SwapfileError::Io)?;
                }
                file.sync_all()
                    .with_context(|| format!("failed to fsync {}", paths.swap.display()))
                    .map_err(SwapfileError::Io)?;
                Ok(Self {
                    path: paths.swap,
                    _file: file,
                })
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                // Another live process owns the lock. Read the file
                // contents best-effort for the user-facing message; if
                // the read fails we still know the lock is held, we just
                // can't say who by.
                drop(file);
                let record = read_swapfile(&paths.swap).unwrap_or_else(|| SwapfileRecord {
                    pid: 0,
                    hostname: "unknown".to_string(),
                    stream_path: stream_path.to_string_lossy().to_string(),
                    created_at_secs: 0,
                });
                Err(SwapfileError::Held(SwapfileConflict {
                    swap_path: paths.swap,
                    record,
                }))
            }
            Err(std::fs::TryLockError::Error(e)) => Err(SwapfileError::Io(
                anyhow::Error::from(e).context("failed to lock swapfile"),
            )),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Swapfile {
    fn drop(&mut self) {
        // Dropping `_file` releases the kernel-side lock automatically.
        // Unlink so a clean exit doesn't leave the (now unlocked) file
        // around with a stale PID record inside. If someone raced to
        // re-acquire between our unlock and our unlink they got a fresh
        // inode; our remove targets a path we no longer hold — harmless.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn read_swapfile(path: &Path) -> Option<SwapfileRecord> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn current_hostname() -> String {
    if let Ok(h) = env::var("HOSTNAME") {
        if !h.is_empty() {
            return h;
        }
    }
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: gethostname writes up to len bytes into buf and
        // NUL-terminates on success; we then read up to the NUL.
        let rc = unsafe { libc_gethostname(buf.as_mut_ptr() as *mut _, buf.len()) };
        if rc == 0 {
            if let Some(nul) = buf.iter().position(|&b| b == 0) {
                if let Ok(s) = std::str::from_utf8(&buf[..nul]) {
                    if !s.is_empty() {
                        return s.to_string();
                    }
                }
            }
        }
    }
    "unknown".to_string()
}

#[cfg(unix)]
extern "C" {
    #[link_name = "gethostname"]
    fn libc_gethostname(name: *mut std::os::raw::c_char, len: usize) -> i32;
}

// ---------------------------------------------------------------------------
// File identity helpers
// ---------------------------------------------------------------------------

pub fn hash_stream_prefix(path: &Path, prefix_len: u64) -> Result<String> {
    hash_prefix(path, prefix_len)
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

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// ---------------------------------------------------------------------------
// Session / profile export (offline bundles — written via atomic_write).
// ---------------------------------------------------------------------------

pub fn export_session(path: &Path, session: &SessionExport) -> Result<()> {
    let payload =
        serde_json::to_vec_pretty(session).context("failed to serialize session export")?;
    atomic_write(path, &payload)
}

pub fn import_session(path: &Path) -> Result<SessionExport> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read session export {}", path.display()))?;
    let session: SessionExport =
        serde_json::from_slice(&bytes).context("invalid session export payload")?;
    Ok(session)
}

pub fn load_profile(path: &Path) -> Result<SourceProfile> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read profile {}", path.display()))?;
    let profile: SourceProfile =
        serde_json::from_slice(&bytes).context("invalid source profile payload")?;
    Ok(profile)
}

pub fn save_profile(path: &Path, profile: &SourceProfile) -> Result<()> {
    let payload = serde_json::to_vec_pretty(profile).context("failed to serialize profile")?;
    atomic_write(path, &payload)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("json-analyzer-test-{pid}-{nanos}-{name}"));
        p
    }

    #[test]
    fn atomic_write_replaces_existing_content() {
        let target = tmp_path("atomic_replace.bin");
        std::fs::write(&target, b"old").unwrap();
        atomic_write(&target, b"new contents").unwrap();
        let read_back = std::fs::read(&target).unwrap();
        assert_eq!(read_back, b"new contents");
        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn atomic_write_creates_missing_parent() {
        let mut dir = std::env::temp_dir();
        dir.push(format!("json-analyzer-test-mkdir-{}", std::process::id()));
        let target = dir.join("sub").join("file.bin");
        let _ = std::fs::remove_dir_all(&dir);
        atomic_write(&target, b"hello").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_sets_mode_0600_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let target = tmp_path("perms.bin");
        atomic_write(&target, b"x").unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected 0600, got {:o}", mode);
        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn state_round_trips() {
        let stream = tmp_path("stream-state.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let mut state = PersistedState::empty(stream.to_string_lossy().to_string());
        state.current_label = "myop".to_string();
        state.types_filter = "http".to_string();
        state.renames.push(("abc".to_string(), "Login".to_string()));
        save_state(&stream, &state).unwrap();
        let loaded = read_state(&stream).unwrap().unwrap();
        assert_eq!(loaded.current_label, "myop");
        assert_eq!(loaded.types_filter, "http");
        assert_eq!(loaded.renames, vec![("abc".to_string(), "Login".to_string())]);
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
        let _ = std::fs::remove_file(&paths.swap);
    }

    /// Two acquires firing in parallel against the same fresh path must
    /// produce exactly one winner. Demonstrates / regresses the TOCTOU
    /// race in the naive read-then-write implementation.
    #[test]
    fn swapfile_simultaneous_acquires_have_exactly_one_winner() {
        use std::sync::{Arc, Barrier};
        let stream = tmp_path("stream-swap-race.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&paths.swap);

        let trials = 50;
        let mut total_winners: usize = 0;
        let mut total_held: usize = 0;
        let mut total_other_err: usize = 0;
        for _ in 0..trials {
            let _ = std::fs::remove_file(&paths.swap);
            let barrier = Arc::new(Barrier::new(2));
            let s1 = stream.clone();
            let s2 = stream.clone();
            let b1 = barrier.clone();
            let b2 = barrier.clone();
            let h1 = std::thread::spawn(move || {
                b1.wait();
                Swapfile::acquire(&s1, false)
            });
            let h2 = std::thread::spawn(move || {
                b2.wait();
                Swapfile::acquire(&s2, false)
            });
            let r1 = h1.join().unwrap();
            let r2 = h2.join().unwrap();
            let (ok1, ok2) = (r1.is_ok(), r2.is_ok());
            let winners = (ok1 as usize) + (ok2 as usize);
            total_winners += winners;
            for r in [&r1, &r2] {
                match r {
                    Err(SwapfileError::Held(_)) => total_held += 1,
                    Err(SwapfileError::Io(_)) => total_other_err += 1,
                    Ok(_) => {}
                }
            }
            assert_eq!(
                winners, 1,
                "race produced {winners} winners (expected 1): r1.ok={ok1} r2.ok={ok2}"
            );
            drop(r1);
            drop(r2);
        }
        // Sanity: every losing attempt should have been a Held conflict,
        // not an IO error.
        assert_eq!(total_winners, trials, "{trials} trials, {total_winners} winners");
        assert_eq!(total_held, trials, "every loser should be Held");
        assert_eq!(total_other_err, 0);

        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
    }

    #[test]
    fn swapfile_releases_on_drop() {
        let stream = tmp_path("stream-swap.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&paths.swap);

        let swap = Swapfile::acquire(&stream, false).expect("first acquire");
        assert!(swap.path().exists(), "swapfile should be created");
        drop(swap);
        assert!(!paths.swap.exists(), "swapfile should be removed on drop");

        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
    }

    #[test]
    fn swapfile_detects_live_holder() {
        let stream = tmp_path("stream-swap-live.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&paths.swap);

        let _first = Swapfile::acquire(&stream, false).expect("first acquire");
        match Swapfile::acquire(&stream, false) {
            Err(SwapfileError::Held(c)) => {
                assert_eq!(c.record.pid, std::process::id());
                assert_eq!(c.record.hostname, current_hostname());
            }
            other => panic!("expected Held conflict, got {other:?}"),
        }
        drop(_first);
        // After the first guard drops, the kernel releases the lock and
        // the file is unlinked — a second acquire must now succeed.
        let _second = Swapfile::acquire(&stream, false).expect("post-drop acquire");
        drop(_second);
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
    }

    #[test]
    fn swapfile_force_overrides_live_holder() {
        // With std-locking semantics the only way to be "held" is by a live
        // process. `--force` must still succeed against that: it unlinks
        // the swap so we get a fresh inode whose lock is independent of the
        // original holder's. Both processes then run concurrently — which
        // is the documented meaning of `--force`.
        let stream = tmp_path("stream-swap-force.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&paths.swap);

        let first = Swapfile::acquire(&stream, false).expect("first acquire");
        // Without force, the second must be rejected.
        assert!(matches!(
            Swapfile::acquire(&stream, false),
            Err(SwapfileError::Held(_))
        ));
        // With force, the second succeeds — it operates on a fresh inode
        // independent of `first`'s lock.
        let forced = Swapfile::acquire(&stream, true).expect("force acquire");
        let after = read_swapfile(&paths.swap).expect("record must exist");
        assert_eq!(after.pid, std::process::id());
        drop(forced);
        drop(first);
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
    }

    #[test]
    fn swapfile_unlocked_file_is_reclaimed() {
        // After a crash the swap file may still exist on disk (we couldn't
        // run Drop) but no process holds the lock. A fresh acquire must
        // pick up that file, lock it, and overwrite the stale record.
        let stream = tmp_path("stream-swap-stale.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let _ = std::fs::remove_file(&paths.swap);

        // Write a leftover record without taking the lock — same effect as
        // a previous instance that died without running Drop.
        let stale = SwapfileRecord {
            pid: 999_999,
            hostname: "ghost".to_string(),
            stream_path: stream.to_string_lossy().to_string(),
            created_at_secs: 0,
        };
        std::fs::write(&paths.swap, serde_json::to_vec(&stale).unwrap()).unwrap();

        let swap = Swapfile::acquire(&stream, false).expect("unlocked file reclaims cleanly");
        let after = read_swapfile(&paths.swap).expect("record must exist");
        assert_eq!(after.pid, std::process::id());
        assert_eq!(after.hostname, current_hostname());
        drop(swap);
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.state);
    }
}
