//! Per-process presence heartbeats so multi-operator sessions can show who
//! else is connected to the same stream.
//!
//! Each process writes `<sha>.presence.<pid>.json` to the state directory
//! every `HEARTBEAT_INTERVAL` and deletes its own file on drop. Peers are
//! discovered by scanning the directory and filtering on a recent
//! `last_heartbeat_secs` so stale (crashed) entries time out without manual
//! cleanup.

use crate::persistence::{atomic_write, state_paths_for_stream};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// A peer's heartbeat is considered live for this long after its last write.
/// Generous so a paused process (debugger, SIGSTOP) doesn't immediately
/// disappear from the status bar.
const PEER_STALE_AFTER_SECS: u64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PresenceFile {
    username: String,
    hostname: String,
    pid: u32,
    started_at_secs: u64,
    last_heartbeat_secs: u64,
}

/// Returned by `start_heartbeat`. While alive, a background thread writes the
/// process's presence file every `HEARTBEAT_INTERVAL`. On drop, the thread is
/// signalled to stop and the presence file is removed (belt-and-braces — peers
/// also time out heartbeats older than `PEER_STALE_AFTER_SECS`).
pub struct PresenceHandle {
    dir: PathBuf,
    /// SHA-256 hex of the stream path — presence files from OTHER streams
    /// share the same `dir` and must be filtered out.
    stream_id: String,
    self_file: PathBuf,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

impl PresenceHandle {
    /// Scan the state directory for `<sha>.presence.*.json` files, parse each,
    /// drop entries whose `last_heartbeat_secs` is older than the staleness
    /// window, group by username, and return `(username, count)` pairs sorted
    /// by username. Count > 1 means the same Unix user has multiple TUI
    /// processes connected to this stream.
    pub fn current_peers(&self) -> Vec<(String, usize)> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let now = now_secs();
        let mut by_user: HashMap<String, usize> = HashMap::new();
        let prefix = format!("{}.presence.", self.stream_id);
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with(&prefix) || !name.ends_with(".json") {
                continue;
            }
            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            let Ok(file) = serde_json::from_slice::<PresenceFile>(&bytes) else {
                continue;
            };
            if now.saturating_sub(file.last_heartbeat_secs) > PEER_STALE_AFTER_SECS {
                continue;
            }
            *by_user.entry(file.username).or_insert(0) += 1;
        }
        let mut list: Vec<(String, usize)> = by_user.into_iter().collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }
}

impl Drop for PresenceHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
        let _ = std::fs::remove_file(&self.self_file);
    }
}

/// Start the heartbeat thread for the given stream. Returns Ok(None) if the
/// state directory can't be derived (very rare — usually only in unit tests
/// with bogus paths).
pub fn start_heartbeat(stream_path: &Path) -> Result<PresenceHandle> {
    let paths = state_paths_for_stream(stream_path)?;
    std::fs::create_dir_all(&paths.dir)
        .with_context(|| format!("failed to create {}", paths.dir.display()))?;
    let pid = std::process::id();
    let self_file = paths.dir.join(format!("{}.presence.{}.json", paths.id, pid));
    let username = current_username();
    let hostname = current_hostname();
    let started = now_secs();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let self_file_clone = self_file.clone();

    let join = thread::Builder::new()
        .name("presence-heartbeat".into())
        .spawn(move || {
            // Write an initial heartbeat immediately so peers see us within
            // their first poll, not after one full HEARTBEAT_INTERVAL.
            let _ = write_heartbeat(
                &self_file_clone,
                &username,
                &hostname,
                pid,
                started,
                now_secs(),
            );
            // Sleep in small slices so shutdown is quick even if the interval
            // is long.
            let slice = Duration::from_millis(200);
            let mut elapsed = Duration::ZERO;
            while !stop_clone.load(Ordering::Relaxed) {
                if elapsed >= HEARTBEAT_INTERVAL {
                    let _ = write_heartbeat(
                        &self_file_clone,
                        &username,
                        &hostname,
                        pid,
                        started,
                        now_secs(),
                    );
                    elapsed = Duration::ZERO;
                }
                thread::sleep(slice);
                elapsed += slice;
            }
        })
        .context("failed to spawn presence heartbeat thread")?;

    Ok(PresenceHandle {
        dir: paths.dir,
        stream_id: paths.id,
        self_file,
        stop,
        join: Some(join),
    })
}

fn write_heartbeat(
    path: &Path,
    username: &str,
    hostname: &str,
    pid: u32,
    started_at_secs: u64,
    last_heartbeat_secs: u64,
) -> Result<()> {
    let payload = PresenceFile {
        username: username.to_string(),
        hostname: hostname.to_string(),
        pid,
        started_at_secs,
        last_heartbeat_secs,
    };
    let bytes = serde_json::to_vec(&payload).context("failed to serialize presence file")?;
    atomic_write(path, &bytes)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn current_username() -> String {
    std::env::var("USER")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("LOGNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "unknown".to_string())
}

fn current_hostname() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

/// Matches `<id>.presence.<anything>.json`. Anything else (shared, lock,
/// legacy local) is ignored.
#[cfg(test)]
fn is_presence_filename(name: &str) -> bool {
    let Some(rest) = name.strip_suffix(".json") else {
        return false;
    };
    // Need at least <id>.presence.<x>
    let parts: Vec<&str> = rest.split('.').collect();
    parts.len() >= 3 && parts[parts.len() - 2] == "presence"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_stream(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("json-presence-{pid}-{nanos}-{name}.jsonl"));
        std::fs::write(&p, b"").unwrap();
        p
    }

    #[test]
    fn heartbeat_writes_and_drop_removes_file() {
        let stream = tmp_stream("alive");
        let handle = start_heartbeat(&stream).expect("start");
        // Wait briefly for the immediate heartbeat write.
        std::thread::sleep(Duration::from_millis(200));
        assert!(handle.self_file.exists(), "presence file should exist");

        // Self should appear in the peer list.
        let peers = handle.current_peers();
        assert!(!peers.is_empty(), "expected at least our own username");

        drop(handle);
        // After drop, the file should be gone.
        let stream2 = stream.clone();
        let paths = state_paths_for_stream(&stream2).unwrap();
        let pid = std::process::id();
        let path = paths.dir.join(format!("{}.presence.{}.json", paths.id, pid));
        assert!(!path.exists(), "presence file should be removed on drop");

        let _ = std::fs::remove_file(&stream);
    }

    #[test]
    fn stale_entries_are_excluded() {
        let stream = tmp_stream("stale");
        let paths = state_paths_for_stream(&stream).unwrap();
        std::fs::create_dir_all(&paths.dir).unwrap();

        // Write a fake stale presence file directly.
        let stale_path = paths
            .dir
            .join(format!("{}.presence.999999.json", paths.id));
        let stale = PresenceFile {
            username: "ghost".to_string(),
            hostname: "h".to_string(),
            pid: 999_999,
            started_at_secs: 0,
            last_heartbeat_secs: 0, // ancient
        };
        std::fs::write(&stale_path, serde_json::to_vec(&stale).unwrap()).unwrap();

        let handle = start_heartbeat(&stream).expect("start");
        std::thread::sleep(Duration::from_millis(200));

        let peers = handle.current_peers();
        assert!(
            !peers.iter().any(|(u, _)| u == "ghost"),
            "stale peer should be filtered out, got {:?}",
            peers
        );

        drop(handle);
        let _ = std::fs::remove_file(&stale_path);
        let _ = std::fs::remove_file(&stream);
    }

    #[test]
    fn is_presence_filename_matches_expected_shape() {
        assert!(is_presence_filename("abc.presence.42.json"));
        assert!(is_presence_filename("abc.presence.0.json"));
        assert!(!is_presence_filename("abc.shared.json"));
        assert!(!is_presence_filename("abc.local.json"));
        assert!(!is_presence_filename("abc.presence.json"));
        assert!(!is_presence_filename("abc.presence.42.txt"));
    }
}
