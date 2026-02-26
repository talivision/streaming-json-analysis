use json_analyzer::io::StreamReader;
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

fn temp_stream_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let seq = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!(
        "json_analyzer_stream_reader_{pid}_{nanos}_{seq}.jsonl"
    ))
}

#[test]
fn stream_reader_fails_fast_on_invalid_json_line() {
    let path = temp_stream_path();
    fs::write(
        &path,
        "{\"event\":\"a\"}\nnot-json\n\n{\"event\":\"b\",\"n\":1}\n",
    )
    .expect("write initial file");

    let mut reader = StreamReader::new(path.clone());
    let result = reader.poll();
    assert!(result.is_err(), "poll must fail on invalid JSON line");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("multiple lines") || msg.contains("Invalid JSON"),
        "error should mention multi-line: {msg}"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_polls_incrementally() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\",\"n\":1}\n")
        .expect("write initial file");

    let mut reader = StreamReader::new(path.clone());
    let first = reader.poll().expect("first poll succeeds");
    assert_eq!(first.len(), 2);
    assert_eq!(first[0], json!({"event":"a"}));
    assert_eq!(first[1], json!({"event":"b","n":1}));

    let second = reader.poll().expect("second poll succeeds");
    assert!(second.is_empty());

    let mut f = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open for append");
    f.write_all(b"{\"event\":\"c\"}\n")
        .expect("append new line");
    let third = reader.poll().expect("third poll succeeds");
    assert_eq!(third.len(), 1);
    assert_eq!(third[0], json!({"event":"c"}));

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_resets_offset_after_truncate() {
    let path = temp_stream_path();
    fs::write(&path, "{\"id\":1}\n{\"id\":2}\n").expect("write initial");

    let mut reader = StreamReader::new(path.clone());
    let first = reader.poll().expect("first poll");
    assert_eq!(first.len(), 2);

    fs::write(&path, "{\"id\":9}\n").expect("truncate and rewrite");
    let second = reader.poll().expect("second poll");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0], json!({"id":9}));

    let _ = fs::remove_file(path);
}
