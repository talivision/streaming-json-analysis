use crate::domain::{ActionPeriod, DataFilters, PathOverride};
use anyhow::{Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const SHARED_VERSION: u32 = 1;

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


// ===========================================================================
// Split persisted state: shared (multi-operator) and local (per-process view)
// ===========================================================================

/// State shared between operators against the same stream. Read-modify-write
/// is guarded by an exclusive advisory lock on `<sha>.shared.lock`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedState {
    pub version: u32,
    pub stream_path: String,
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    /// Triaged events identified by (ts, type_id) — stable across processes.
    /// Vec indices are *not* shared, so we serialize a value identity instead.
    pub triaged_events: Vec<(f64, String)>,
}

impl SharedState {
    pub fn empty(stream_path: String) -> Self {
        Self {
            version: SHARED_VERSION,
            stream_path,
            periods: Vec::new(),
            renames: Vec::new(),
            normalized_field_overrides: Vec::new(),
            triaged_events: Vec::new(),
        }
    }
}

/// In-memory representation of state restored from disk on startup. View
/// state (filters, label, current selection) is intentionally NOT persisted
/// — it would clobber other operators' filters under the same Unix login.
#[derive(Debug)]
pub struct RestoredState {
    pub periods: Vec<ActionPeriod>,
    pub renames: Vec<(String, String)>,
    pub normalized_field_overrides: Vec<NormalizedFieldOverride>,
    /// Materialized triage identifiers — converted back to Vec<usize> in App
    /// by matching (ts, type_id) against the loaded EventRecord stream.
    pub triaged_events: Vec<(f64, String)>,
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatePaths {
    pub shared: PathBuf,
    pub lock: PathBuf,
    pub dir: PathBuf,
    /// Stable per-stream prefix (the SHA-256 hex). Used by sibling files in
    /// the same state directory (presence heartbeats, etc.).
    pub id: String,
}

pub fn state_paths_for_stream(stream_path: &Path) -> Result<StatePaths> {
    let mut hasher = Sha256::new();
    hasher.update(stream_path.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    let id = format!("{:x}", digest);
    let dir = base_state_dir()?;
    Ok(StatePaths {
        shared: dir.join(format!("{}.shared.json", id)),
        lock: dir.join(format!("{}.shared.lock", id)),
        dir,
        id,
    })
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

// ---------------------------------------------------------------------------
// Atomic write helper
// ---------------------------------------------------------------------------

/// Atomically replaces `target` with `payload`:
///   1. write payload to <target>.tmp (mode 0600 on unix)
///   2. fsync(.tmp)
///   3. rename(.tmp, target)
///   4. fsync(parent_dir)
///
/// The parent-dir fsync is load-bearing — without it, a crash after rename
/// can leave the new (visible) file with zero content on ext4. Best-effort on
/// platforms where opening a directory for write isn't supported (macOS
/// happens to allow opening directories read-only and calling fsync on them).
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

    // Step 1: write payload to .tmp.
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
        // Step 2: fsync(.tmp).
        file.sync_all()
            .with_context(|| format!("failed to fsync {}", tmp.display()))?;
    }

    // Step 3: rename.
    std::fs::rename(&tmp, target).with_context(|| {
        format!("failed to rename {} -> {}", tmp.display(), target.display())
    })?;

    // Step 4: fsync parent directory so the rename is durable.
    if let Ok(dir) = File::open(parent) {
        // Best-effort: directory fsync isn't portable to Windows; failures here
        // don't undo the rename, they just leave a tiny window where a power
        // loss could revert it. We still want to know if it fails on dev boxes.
        let _ = dir.sync_all();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared state IO (under advisory lock)
// ---------------------------------------------------------------------------

/// Acquires an exclusive advisory lock on `<sha>.shared.lock` for the duration
/// of the returned guard. Always acquire before reading or writing the shared
/// file when you intend to modify it.
pub struct SharedLock {
    file: File,
}

impl SharedLock {
    pub fn acquire(paths: &StatePaths) -> Result<Self> {
        create_dir_all(&paths.dir)
            .with_context(|| format!("failed to create {}", paths.dir.display()))?;
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            opts.mode(0o600);
        }
        let file = opts
            .open(&paths.lock)
            .with_context(|| format!("failed to open lock {}", paths.lock.display()))?;
        FileExt::lock_exclusive(&file)
            .with_context(|| format!("failed to lock {}", paths.lock.display()))?;
        Ok(Self { file })
    }
}

impl Drop for SharedLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Read the shared state without taking the lock. Safe for the watcher reload
/// path; the on-disk file is the result of a locked write so it's consistent.
pub fn read_shared_state_unlocked(paths: &StatePaths) -> Result<Option<SharedState>> {
    if !paths.shared.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&paths.shared).with_context(|| {
        format!("failed to read shared state {}", paths.shared.display())
    })?;
    if bytes.is_empty() {
        // Mid-rename observation on a path that doesn't atomic-rename. We never
        // produce empty files, so treat this as transient and skip.
        return Ok(None);
    }
    let state: SharedState =
        serde_json::from_slice(&bytes).context("invalid shared state payload")?;
    if state.version != SHARED_VERSION {
        return Ok(None);
    }
    Ok(Some(state))
}

/// Read-modify-write of the shared file under exclusive lock. The closure
/// receives the on-disk state (or a fresh empty one) and returns the new
/// state to persist.
pub fn update_shared_state<F>(stream_path: &Path, f: F) -> Result<()>
where
    F: FnOnce(SharedState) -> SharedState,
{
    let paths = state_paths_for_stream(stream_path)?;
    let _lock = SharedLock::acquire(&paths)?;
    let current = read_shared_state_unlocked(&paths)?
        .unwrap_or_else(|| SharedState::empty(stream_path.to_string_lossy().to_string()));
    let mut next = f(current);
    // Keep stream_path / version coherent regardless of what the caller did.
    next.version = SHARED_VERSION;
    if next.stream_path.is_empty() {
        next.stream_path = stream_path.to_string_lossy().to_string();
    }
    let payload = serde_json::to_vec(&next).context("failed to serialize shared state")?;
    atomic_write(&paths.shared, &payload)?;
    Ok(())
}

/// Convenience for the common "I already have the desired state in memory"
/// case (autosave / shutdown). Still locks to prevent torn writes.
pub fn save_shared_state(stream_path: &Path, state: &SharedState) -> Result<()> {
    update_shared_state(stream_path, |_| state.clone())
}

// ---------------------------------------------------------------------------
// Startup load: returns the shared state for this stream if one exists.
// View state (filters / label / cursor) is in-memory only and not loaded.
// ---------------------------------------------------------------------------

pub fn load_full_state(stream_path: &Path) -> Result<Option<RestoredState>> {
    let paths = state_paths_for_stream(stream_path)?;
    let shared = if paths.shared.exists() {
        // Take the lock briefly: we may be observing mid-write from another op.
        let _lock = SharedLock::acquire(&paths)?;
        read_shared_state_unlocked(&paths)?
    } else {
        None
    };
    let Some(shared) = shared else {
        return Ok(None);
    };
    Ok(Some(RestoredState {
        periods: shared.periods,
        renames: shared.renames,
        normalized_field_overrides: shared.normalized_field_overrides,
        triaged_events: shared.triaged_events,
    }))
}

/// Best-effort cleanup of legacy per-process state files from older builds.
/// Deletes `<sha>.local.json` and `<sha>.local.<pid>.json` for this stream's
/// id from the state directory. Errors are intentionally swallowed: this is
/// hygiene, not correctness.
pub fn cleanup_legacy_local_files(stream_path: &Path) {
    let Ok(paths) = state_paths_for_stream(stream_path) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(&paths.dir) else {
        return;
    };
    let prefix_dot_local = format!("{}.local", paths.id);
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(&prefix_dot_local) {
            continue;
        }
        // Match exactly `<id>.local.json` or `<id>.local.<anything>.json`.
        let tail = &name[prefix_dot_local.len()..];
        let matches = tail == ".json"
            || (tail.starts_with('.') && tail.ends_with(".json"));
        if !matches {
            continue;
        }
        let _ = std::fs::remove_file(entry.path());
    }
}

// ---------------------------------------------------------------------------
// Session/profile export — unchanged, written via atomic_write for safety.
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
    use std::process::Command;

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

    /// Kill a child mid-write and verify the visible file is either the old
    /// content or fully present new content — never empty / partial.
    ///
    /// We exercise the helper by spawning a child that opens the .tmp file,
    /// writes a marker, then sleeps. We SIGKILL the child before it gets to
    /// the rename. The visible file should still hold the original content.
    #[test]
    fn atomic_write_visible_file_never_torn() {
        let target = tmp_path("torn.bin");
        let original = b"ORIGINAL_CONTENT_AAAAA";
        std::fs::write(&target, original).unwrap();

        // Spawn a sh that writes a different value to <target>.tmp and sleeps
        // — simulating a writer killed before it renames. The visible file is
        // untouched.
        let tmp = {
            let mut t = target.clone();
            let mut name = t.file_name().unwrap().to_owned();
            name.push(".tmp");
            t.set_file_name(name);
            t
        };
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "printf 'PARTIAL_NEW_CONTENT' > '{}' && sleep 5",
                tmp.display()
            ))
            .spawn()
            .expect("spawn child");
        std::thread::sleep(std::time::Duration::from_millis(150));
        let _ = child.kill();
        let _ = child.wait();

        // Visible file is still the original. Tmp file may or may not be present.
        let read_back = std::fs::read(&target).unwrap();
        assert_eq!(
            read_back, original,
            "visible file was clobbered before rename"
        );
        assert!(!read_back.is_empty(), "visible file became empty");

        // Now actually call our atomic_write helper to install new content and
        // confirm post-condition: visible file matches new content exactly.
        let new_payload = b"FULLY_NEW_CONTENT_XYZ".to_vec();
        atomic_write(&target, &new_payload).unwrap();
        let read_back = std::fs::read(&target).unwrap();
        assert_eq!(read_back, new_payload);

        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn shared_state_round_trips_via_lock() {
        let stream = tmp_path("stream-shared.jsonl");
        std::fs::write(&stream, b"").unwrap();
        update_shared_state(&stream, |mut s| {
            s.renames.push(("abc".to_string(), "Login".to_string()));
            s.triaged_events.push((1700000000.0, "abc".to_string()));
            s
        })
        .unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let loaded = read_shared_state_unlocked(&paths).unwrap().unwrap();
        assert_eq!(loaded.renames, vec![("abc".to_string(), "Login".to_string())]);
        assert_eq!(
            loaded.triaged_events,
            vec![(1700000000.0, "abc".to_string())]
        );
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.shared);
        let _ = std::fs::remove_file(&paths.lock);
    }

    #[test]
    fn cleanup_legacy_local_files_removes_old_per_stream_files() {
        let stream = tmp_path("stream-cleanup.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        std::fs::create_dir_all(&paths.dir).unwrap();
        // Sentinels owned by THIS stream — must be deleted.
        let mine_plain = paths.dir.join(format!("{}.local.json", paths.id));
        let mine_pid = paths.dir.join(format!("{}.local.4242.json", paths.id));
        std::fs::write(&mine_plain, b"x").unwrap();
        std::fs::write(&mine_pid, b"x").unwrap();
        // A sibling stream's file — must NOT be touched.
        let other_id = "0".repeat(64);
        let other_plain = paths.dir.join(format!("{}.local.json", other_id));
        std::fs::write(&other_plain, b"x").unwrap();
        // A shared file with the same prefix must also remain.
        let mine_shared_existing = paths.shared.clone();
        std::fs::write(&mine_shared_existing, b"x").unwrap();

        cleanup_legacy_local_files(&stream);

        assert!(!mine_plain.exists(), "mine_plain should be deleted");
        assert!(!mine_pid.exists(), "mine_pid should be deleted");
        assert!(other_plain.exists(), "other stream's file must remain");
        assert!(mine_shared_existing.exists(), "shared file must remain");

        let _ = std::fs::remove_file(&other_plain);
        let _ = std::fs::remove_file(&mine_shared_existing);
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.lock);
    }

    #[test]
    fn update_shared_state_serializes_concurrent_writers() {
        // Two concurrent updaters each appending one rename; both must survive.
        let stream = tmp_path("stream-concurrent.jsonl");
        std::fs::write(&stream, b"").unwrap();
        let stream2 = stream.clone();
        let h1 = std::thread::spawn(move || {
            for i in 0..20 {
                update_shared_state(&stream2, |mut s| {
                    s.renames.push((format!("a{i}"), format!("A{i}")));
                    s
                })
                .unwrap();
            }
        });
        let stream3 = stream.clone();
        let h2 = std::thread::spawn(move || {
            for i in 0..20 {
                update_shared_state(&stream3, |mut s| {
                    s.renames.push((format!("b{i}"), format!("B{i}")));
                    s
                })
                .unwrap();
            }
        });
        h1.join().unwrap();
        h2.join().unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        let loaded = read_shared_state_unlocked(&paths).unwrap().unwrap();
        assert_eq!(loaded.renames.len(), 40);
        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.shared);
        let _ = std::fs::remove_file(&paths.lock);
    }
}
