//! Watcher thread that notifies the main loop when the shared state file
//! changes on disk (e.g. another operator's write).
//!
//! Implementation notes:
//! - Watch the *parent directory* non-recursively. Watching the file itself
//!   loses the watch after every atomic rename (inode changes on inotify;
//!   FSEvents also silently drops it).
//! - Filter incoming events by filename so unrelated state files for other
//!   streams don't trigger reloads.
//! - Debounce 150 ms to coalesce the CREATE+MOVED_TO+MODIFY burst that inotify
//!   emits per rename, and to absorb FSEvents coalescing.
//! - Fall back to `PollWatcher` (1 s poll) when `recommended_watcher` fails,
//!   e.g. on NFS or in some sandboxes. One stat per second on a sub-10 KB file
//!   is negligible.
//! - The watcher will see this process's own writes; we don't try to filter
//!   them out. Self-reload is sub-millisecond and bounded by autosave cadence,
//!   so it's cheaper than tracking "I wrote at time T."

use crate::persistence::StatePaths;
use anyhow::Result;
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::new_debouncer;
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

/// Message sent from the watcher thread to the main loop.
#[derive(Debug)]
pub enum WatchMessage {
    /// The shared state file changed; main loop should reload.
    Reload,
}

const DEBOUNCE: Duration = Duration::from_millis(150);
const POLL_FALLBACK_INTERVAL: Duration = Duration::from_secs(1);

/// Spawns a watcher thread keyed off `paths.shared`. Sends `Reload` on
/// `tx` whenever the shared file changes (debounced). The returned handle
/// owns the watcher; dropping it stops the watch. Returns Ok(None) on
/// platforms where neither recommended nor poll watcher can be created.
pub fn spawn_shared_state_watcher(
    paths: StatePaths,
    tx: Sender<WatchMessage>,
) -> Result<Option<WatcherHandle>> {
    // Filename we care about, e.g. "<sha>.shared.json".
    let target_filename = paths
        .shared
        .file_name()
        .map(|s| s.to_owned())
        .ok_or_else(|| anyhow::anyhow!("shared state path has no filename"))?;
    let parent = paths.dir.clone();
    // Ensure the directory exists so we can attach a watch to it.
    std::fs::create_dir_all(&parent).ok();

    // Thread that owns the debouncer + watcher and forwards Reload messages.
    let (raw_tx, raw_rx) = mpsc::channel();
    let target_for_filter = target_filename.clone();

    // Try recommended watcher; fall back to poll if that errors.
    let mut debouncer = match new_debouncer(DEBOUNCE, None, raw_tx.clone()) {
        Ok(d) => d,
        Err(err) => {
            // Recommended-watcher init failed (NFS, sandboxed FS, etc.).
            // Build a poll-based debouncer instead.
            eprintln!(
                "warning: native watcher init failed ({err}); falling back to PollWatcher"
            );
            let cfg = notify::Config::default().with_poll_interval(POLL_FALLBACK_INTERVAL);
            let (raw_tx2, raw_rx2) = mpsc::channel();
            // Replace the channels with the poll-based ones.
            // (We re-bind raw_rx via the outer return path.)
            return spawn_poll_fallback(parent, target_filename, raw_tx2, raw_rx2, tx, cfg);
        }
    };
    debouncer
        .watch(&parent, RecursiveMode::NonRecursive)
        .map_err(|e| anyhow::anyhow!("watcher: failed to watch {}: {e}", parent.display()))?;

    let join = thread::spawn(move || {
        forward_events(raw_rx, tx, target_for_filter);
    });

    Ok(Some(WatcherHandle {
        _debouncer: WatcherKind::Recommended(debouncer),
        _join: Some(join),
    }))
}

fn spawn_poll_fallback(
    parent: PathBuf,
    target_filename: std::ffi::OsString,
    _raw_tx: Sender<
        std::result::Result<Vec<notify_debouncer_full::DebouncedEvent>, Vec<notify::Error>>,
    >,
    _raw_rx: mpsc::Receiver<
        std::result::Result<Vec<notify_debouncer_full::DebouncedEvent>, Vec<notify::Error>>,
    >,
    tx: Sender<WatchMessage>,
    cfg: notify::Config,
) -> Result<Option<WatcherHandle>> {
    // Build a debouncer that internally uses a PollWatcher by overriding the
    // backend via the `config` parameter of new_debouncer_opt.
    let (raw_tx, raw_rx) = mpsc::channel();
    let mut debouncer = notify_debouncer_full::new_debouncer_opt::<_, notify::PollWatcher, _>(
        DEBOUNCE,
        None,
        raw_tx,
        notify_debouncer_full::RecommendedCache::new(),
        cfg,
    )
    .map_err(|e| anyhow::anyhow!("PollWatcher init failed: {e}"))?;
    debouncer
        .watch(&parent, RecursiveMode::NonRecursive)
        .map_err(|e| anyhow::anyhow!("PollWatcher: failed to watch {}: {e}", parent.display()))?;
    let join = thread::spawn(move || {
        forward_events(raw_rx, tx, target_filename);
    });
    Ok(Some(WatcherHandle {
        _debouncer: WatcherKind::Poll(debouncer),
        _join: Some(join),
    }))
}

fn forward_events(
    raw_rx: mpsc::Receiver<
        std::result::Result<Vec<notify_debouncer_full::DebouncedEvent>, Vec<notify::Error>>,
    >,
    tx: Sender<WatchMessage>,
    target_filename: std::ffi::OsString,
) {
    while let Ok(batch) = raw_rx.recv() {
        let events = match batch {
            Ok(ev) => ev,
            Err(errs) => {
                for e in errs {
                    eprintln!("warning: watcher error: {e}");
                }
                continue;
            }
        };
        let mut should_reload = false;
        for ev in events {
            if !is_interesting_kind(&ev.event.kind) {
                continue;
            }
            for path in &ev.event.paths {
                if path.file_name().map(|n| n == target_filename).unwrap_or(false) {
                    should_reload = true;
                    break;
                }
            }
            if should_reload {
                break;
            }
        }
        if should_reload {
            if tx.send(WatchMessage::Reload).is_err() {
                break;
            }
        }
    }
}

fn is_interesting_kind(kind: &EventKind) -> bool {
    // Care about creates (rename target), modifies, and removes (a recreate is
    // imminent). Ignore Access-only events.
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) | EventKind::Any
    )
}

#[allow(dead_code)]
enum WatcherKind {
    // The debouncer holds the underlying inotify/FSEvents subscription open
    // for as long as it exists; dropping it stops the watch. We never read
    // the inner value, we just need ownership to live alongside the join
    // handle.
    Recommended(
        notify_debouncer_full::Debouncer<
            notify::RecommendedWatcher,
            notify_debouncer_full::RecommendedCache,
        >,
    ),
    Poll(
        notify_debouncer_full::Debouncer<
            notify::PollWatcher,
            notify_debouncer_full::RecommendedCache,
        >,
    ),
}

pub struct WatcherHandle {
    _debouncer: WatcherKind,
    _join: Option<thread::JoinHandle<()>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{atomic_write, state_paths_for_stream};
    use std::time::Instant;

    #[test]
    fn watcher_fires_on_shared_file_change() {
        let mut stream = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        stream.push(format!("json-watcher-test-{}-{nanos}.jsonl", std::process::id()));
        std::fs::write(&stream, b"").unwrap();
        let paths = state_paths_for_stream(&stream).unwrap();
        std::fs::create_dir_all(&paths.dir).unwrap();
        // Ensure no stale file is present.
        let _ = std::fs::remove_file(&paths.shared);

        let (tx, rx) = mpsc::channel();
        let _handle = spawn_shared_state_watcher(paths.clone(), tx)
            .unwrap()
            .expect("watcher should start");

        // Give the watcher a moment to subscribe.
        std::thread::sleep(Duration::from_millis(200));

        // Write to the shared file via the same atomic-write path the app uses.
        atomic_write(&paths.shared, b"{\"version\":1,\"stream_path\":\"x\",\"periods\":[],\"renames\":[],\"normalized_field_overrides\":[],\"triaged_events\":[]}").unwrap();

        // Expect at least one Reload within a generous deadline.
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut got = false;
        while Instant::now() < deadline {
            if let Ok(WatchMessage::Reload) = rx.recv_timeout(Duration::from_millis(200)) {
                got = true;
                break;
            }
        }
        assert!(got, "watcher did not deliver a Reload event");

        let _ = std::fs::remove_file(&stream);
        let _ = std::fs::remove_file(&paths.shared);
        let _ = std::fs::remove_file(&paths.lock);
    }
}
