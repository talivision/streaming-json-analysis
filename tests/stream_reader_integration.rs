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
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\",\"n\":1}\n").expect("write initial file");

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
fn stream_reader_waits_for_partial_jsonl_line_until_newline_arrives() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"par").expect("write partial file");

    let mut reader = StreamReader::new(path.clone());
    let first = reader.poll().expect("first poll succeeds");
    assert_eq!(first, vec![json!({"event":"a"})]);
    assert!(
        reader.has_incomplete_final_line(),
        "reader should retain the incomplete tail"
    );

    let mut f = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open for append");
    f.write_all(b"tial\",\"n\":1}\n")
        .expect("finish partial line");

    let second = reader.poll().expect("second poll succeeds");
    assert_eq!(second, vec![json!({"event":"partial","n":1})]);
    assert!(!reader.has_incomplete_final_line());

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_does_not_report_incomplete_tail_for_unread_complete_jsonl_lines() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\"}\n{\"event\":\"c\"}\n")
        .expect("write complete jsonl file");

    let mut reader = StreamReader::new(path.clone());
    let first = reader.poll().expect("first poll succeeds");
    assert_eq!(
        first,
        vec![
            json!({"event":"a"}),
            json!({"event":"b"}),
            json!({"event":"c"})
        ]
    );
    assert!(!reader.has_incomplete_final_line());

    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\"}\n{\"event\":\"c\"}\n")
        .expect("rewrite complete jsonl file");
    let mut reader = StreamReader::new(path.clone());
    reader.poll().expect("poll succeeds");
    fs::write(
        &path,
        "{\"event\":\"a\"}\n{\"event\":\"b\"}\n{\"event\":\"c\"}\n{\"event\":\"d\"}\n",
    )
    .expect("append complete newline-terminated record");
    assert!(
        !reader.has_incomplete_final_line(),
        "unread complete lines should not count as an incomplete final line"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_reports_unterminated_final_json_object() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\"}").expect("write unterminated file");

    let mut reader = StreamReader::new(path.clone());
    let rows = reader.poll().expect("poll succeeds");
    assert_eq!(rows, vec![json!({"event":"a"}), json!({"event":"b"})]);
    assert!(!reader.has_incomplete_final_line());

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_handles_large_final_json_object_beyond_tail_scan_window() {
    let path = temp_stream_path();
    let payload = json!({
        "event": "large",
        "blob": "x".repeat(100 * 1024)
    });
    fs::write(&path, serde_json::to_string(&payload).expect("serialize payload"))
        .expect("write large final object");

    let mut reader = StreamReader::new(path.clone());
    let rows = reader.poll().expect("poll succeeds");
    assert_eq!(rows, vec![payload]);
    assert!(
        !reader.has_incomplete_final_line(),
        "complete large EOF object should not be treated as incomplete"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_does_not_flag_large_newline_terminated_final_line_as_incomplete() {
    let path = temp_stream_path();
    let payload = json!({
        "event": "large-newline-terminated",
        "blob": "x".repeat(100 * 1024)
    });
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string(&payload).expect("serialize payload")),
    )
    .expect("write large newline-terminated object");

    let mut reader = StreamReader::new(path.clone());
    let rows = reader.poll().expect("poll succeeds");
    assert_eq!(rows, vec![payload]);
    assert!(
        !reader.has_incomplete_final_line(),
        "newline-terminated large final object should not be treated as incomplete"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_reports_unparseable_trailing_fragment() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n{\"event\":\"b\"").expect("write partial final object");

    let mut reader = StreamReader::new(path.clone());
    let rows = reader.poll().expect("poll succeeds");
    assert_eq!(rows, vec![json!({"event":"a"})]);
    assert!(reader.has_incomplete_final_line());

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_skips_whitespace_only_tail_at_eof() {
    let path = temp_stream_path();
    fs::write(&path, "{\"event\":\"a\"}\n   \t\r").expect("write whitespace tail");

    let mut reader = StreamReader::new(path.clone());
    let rows = reader.poll().expect("poll succeeds");
    assert_eq!(rows, vec![json!({"event":"a"})]);
    assert!(!reader.has_incomplete_final_line());
    assert!(reader.poll().expect("second poll succeeds").is_empty());

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_fails_fast_on_oversized_line_without_newline() {
    let path = temp_stream_path();
    let giant = format!("{{\"payload\":\"{}\"", "x".repeat(16 * 1024 * 1024));
    fs::write(&path, giant).expect("write oversized partial line");

    let mut reader = StreamReader::new(path.clone());
    let err = reader.poll().expect_err("oversized line should fail fast");
    let msg = err.to_string();
    assert!(
        msg.contains("exceeded"),
        "error should mention overflow: {msg}"
    );
    assert!(
        msg.contains("line 1"),
        "error should include line number: {msg}"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn stream_reader_streams_large_directory_across_polls() {
    let dir = temp_stream_dir();
    fs::create_dir_all(&dir).expect("create test dir");

    // MAX_FILES_PER_POLL is 2000 in src/io.rs; use more than that to verify streaming.
    let total_files = 2_105usize;
    for i in 0..total_files {
        let path = dir.join(format!("f{:04}.json", i));
        fs::write(path, format!("{{\"_timestamp\": {}, \"id\": {}}}\n", i, i)).expect("write file");
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
