mod common;

use common::sample_record;
use std::fs::read;

use rivet::{
    error::StorageError,
    storage::{
        log::Log,
        record::{Record, RecordLimits},
    },
};

use crate::common::record_with_fields;

#[test]
fn opening_new_log_creates_empty_file() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("new-log.log");

    Log::open(&path, RecordLimits::default()).expect("opening a missing log path should succeed");

    let file_bytes = read(&path).expect("newly created log should be readable");

    assert!(
        path.exists(),
        "opening a missing log path should create the file"
    );

    assert!(
        file_bytes.is_empty(),
        "opening a missing log path should create an empty file"
    );
}

#[test]
fn reopening_log_preserves_existing_bytes() {
    let record = sample_record(0);

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("reopen-preserves-bytes.log");

    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let original_file_bytes = read(&path).expect("test log should be readable");

    drop(log);

    Log::open(&path, RecordLimits::default()).expect("reopening the log should succeed");

    let reopened_file_bytes =
        read(&path).expect("existing log should remain readable after reopening");

    assert_eq!(
        original_file_bytes, reopened_file_bytes,
        "reopening the log should preserve the existing file bytes"
    );
}

#[test]
fn append_one_record_writes_expected_encoded_bytes() {
    let record = sample_record(0);
    let expected_bytes = record
        .encode(&RecordLimits::default())
        .expect("record should encode");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("single-record.log");

    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let file_bytes = read(&path).expect("test log should be readable");

    assert_eq!(
        expected_bytes, file_bytes,
        "appending a record should write the expected encoded bytes"
    );
}

#[test]
fn later_appends_grow_file_and_preserve_original_prefix() {
    let record1 = sample_record(0);
    let record2 = record_with_fields(
        0,
        1_600_000_000,
        Some(vec![10, 20, 30, 40, 50]),
        vec![1, 2, 3, 5, 6, 7],
    );

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("append-preserves-prefix.log");

    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending a record should succeed");

    let file_bytes1 = read(&path).expect("test log should be readable");

    log.append(&record2)
        .expect("appending a record should succeed");

    let file_bytes2 = read(&path).expect("test log should be readable");

    assert!(
        file_bytes2.len() > file_bytes1.len(),
        "appending a record should grow the file"
    );

    assert!(
        file_bytes2.starts_with(&file_bytes1),
        "appending a record should preserve the original file prefix"
    );

    assert!(
        file_bytes2.ends_with(
            &record2
                .encode(&RecordLimits::default())
                .expect("record should encode")
        ),
        "appending a record should write the expected encoded bytes at the end"
    );
}

#[test]
fn append_after_reopening_preserves_previous_records() {
    let record1 = sample_record(0);
    let record2 = record_with_fields(
        0,
        1_600_000_000,
        Some(vec![10, 20, 30, 40, 50]),
        vec![1, 2, 3, 5, 6, 7],
    );

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("append-after-reopen.log");

    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending a record should succeed");

    let file_bytes1 = read(&path).expect("test log should be readable");

    drop(log);

    log = Log::open(&path, RecordLimits::default()).expect("reopening the log should succeed");

    log.append(&record2)
        .expect("appending a record should succeed");

    let file_bytes2 = read(&path).expect("test log should be readable");

    assert!(
        file_bytes2.len() > file_bytes1.len(),
        "appending a record should grow the file"
    );

    assert!(
        file_bytes2.starts_with(&file_bytes1),
        "appending a record should preserve the original file prefix"
    );

    assert!(
        file_bytes2.ends_with(
            &record2
                .encode(&RecordLimits::default())
                .expect("record should encode")
        ),
        "appending a record should write the expected encoded bytes at the end"
    );
}

#[test]
fn codec_rejection_leaves_file_unchanged() {
    let record1 = sample_record(0);
    let record2 = record_with_fields(0, 1_600_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3, 4]);
    let limits = RecordLimits::new(3, 3);

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("codec-rejection.log");

    let mut log = Log::open(&path, limits).expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending a record should succeed");

    let file_bytes1 = read(&path).expect("test log should be readable");

    log.append(&record2)
        .expect_err("appending an oversized record should fail");

    let file_bytes2 = read(&path).expect("test log should be readable");

    assert_eq!(
        file_bytes1, file_bytes2,
        "appending an oversized record should leave the file unchanged"
    );
}

#[test]
fn opening_invalid_path_preserves_io_error_source() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let missing_subdir = dir.path().join("missing-subdir");
    let path = missing_subdir.as_path().join("codec-rejection.log");

    let error = Log::open(&path, RecordLimits::default())
        .expect_err("opening a log beneath a missing parent directory should fail");

    assert!(
        matches!(error, StorageError::Io(_)),
        "opening a log beneath a missing parent directory should yield StorageError::Io"
    );
}

#[test]
fn scanning_empty_log_yields_no_records() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let log = Log::open(&path, RecordLimits::default()).expect("opening a missing log path should succeed");

    let mut scanner = log.scan()
        .expect("scanning an empty log should succeed");

    assert!(scanner.next().is_none(), "scanning an empty log should yield no records");
    assert!(scanner.next().is_none(), "scanning an empty log a second time should yield no records");
    assert!(scanner.next().is_none(), "scanning an empty log a third time should yield no records");
}

#[test]
fn scanning_one_record_preserves_all_fields() {
    todo!("Append one record and scan it; compare the full record and verify the scan ends");
}

#[test]
fn scanning_many_records_preserves_offset_order_and_contents() {
    todo!(
        "Append consecutive offsets with varied sizes, absent and empty keys, and binary payloads; compare the scan in order"
    );
}

#[test]
fn scanning_reopened_log_preserves_all_records() {
    todo!("Append several records, drop and reopen the log, then compare every scanned record");
}

#[test]
fn append_after_scanning_writes_at_end() {
    todo!(
        "Append and scan, then append again; verify the original bytes remain and both records scan correctly"
    );
}

#[test]
fn scanning_partial_header_returns_incomplete_header() {
    todo!(
        "Write a valid record followed by a partial header; verify the scan reports the wrapped IncompleteHeader error"
    );
}

#[test]
fn scanning_partial_body_or_checksum_returns_incomplete_body() {
    todo!(
        "Exercise truncation within the body and checksum; verify the wrapped IncompleteBody error"
    );
}

#[test]
fn scanning_corrupt_record_preserves_codec_error() {
    todo!(
        "Corrupt an encoded record's payload; verify the scan reports wrapped InvalidChecksum and exposes its source"
    );
}
