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

fn temp_stream_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let seq = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!(
        "json_analyzer_stream_reader_dir_{pid}_{nanos}_{seq}"
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

#[test]
fn stream_reader_reads_directory_in_timestamp_order() {
    let dir = temp_stream_dir();
    fs::create_dir_all(&dir).expect("create test dir");

    fs::write(dir.join("a.json"), "{\"_timestamp\": 2000, \"id\": 2}\n").expect("write a");
    fs::write(dir.join("b.json"), "{\"_timestamp\": 1000, \"id\": 1}\n").expect("write b");

    let mut reader = StreamReader::new(dir.clone());
    let rows = reader.poll().expect("directory poll succeeds");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], json!({"_timestamp":1000,"id":1}));
    assert_eq!(rows[1], json!({"_timestamp":2000,"id":2}));

    let second = reader.poll().expect("second directory poll succeeds");
    assert!(second.is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn stream_reader_streams_large_directory_across_polls() {
    let dir = temp_stream_dir();
    fs::create_dir_all(&dir).expect("create test dir");

    // MAX_FILES_PER_POLL is 2000 in src/io.rs; use more than that to verify streaming.
    let total_files = 2_105usize;
    for i in 0..total_files {
        let path = dir.join(format!("f{:04}.json", i));
        fs::write(
            path,
            format!("{{\"_timestamp\": {}, \"id\": {}}}\n", i, i),
        )
        .expect("write file");
    }

    let mut reader = StreamReader::new(dir.clone());
    let first = reader.poll().expect("first directory poll succeeds");
    assert_eq!(first.len(), 2_000);
    assert_eq!(first.first().cloned(), Some(json!({"_timestamp":0,"id":0})));
    assert_eq!(
        first.last().cloned(),
        Some(json!({"_timestamp":1999,"id":1999}))
    );

    let second = reader.poll().expect("second directory poll succeeds");
    assert_eq!(second.len(), 105);
    assert_eq!(
        second.first().cloned(),
        Some(json!({"_timestamp":2000,"id":2000}))
    );
    assert_eq!(
        second.last().cloned(),
        Some(json!({"_timestamp":2104,"id":2104}))
    );

    let third = reader.poll().expect("third directory poll succeeds");
    assert!(third.is_empty());

    let _ = fs::remove_dir_all(dir);
}
