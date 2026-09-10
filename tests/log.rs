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
    let log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    let mut scanner = log.scan().expect("scanning an empty log should succeed");

    assert!(
        scanner.next().is_none(),
        "scanning an empty log should yield no records"
    );
    assert!(
        scanner.next().is_none(),
        "scanning an empty log a second time should yield no records"
    );
    assert!(
        scanner.next().is_none(),
        "scanning an empty log a third time should yield no records"
    );
}

#[test]
fn scanning_one_record_preserves_all_fields() {
    let record = sample_record(0);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let scanned_record = log
        .scan()
        .expect("scanning a log with one record should succeed")
        .next()
        .expect("scanning a log with one record should yield a record")
        .expect("scanning a log with one record should yield a valid record");

    assert_eq!(record, scanned_record);
}

#[test]
fn scanning_many_records_preserves_offset_order_and_contents() {
    let records = vec![
        // Absent key, empty payload.
        Record::new(0, 1_000, None, vec![]),
        // Present but empty key, one-byte payload.
        Record::new(1, 1_001, Some(vec![]), vec![0x00]),
        // Nonempty key, binary payload including invalid UTF-8.
        Record::new(
            2,
            1_002,
            Some(b"customer-123".to_vec()),
            vec![0x00, 0xFF, 0x80, 0x0A, 0x42],
        ),
        // Absent key, larger payload containing every possible byte.
        Record::new(3, 1_003, None, (0u8..=255).collect()),
        // Binary key, short payload after the larger record.
        Record::new(4, 1_004, Some(vec![0x00, 0xFE, 0x80]), vec![0xAB, 0xCD]),
    ];
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    records.iter().for_each(|record| {
        log.append(record)
            .expect("appending a record should succeed");
    });

    let scanner = log
        .scan()
        .expect("scanning a log with many records should succeed");

    assert_eq!(
        records,
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log with many records should yield valid records"),
        "scanning a log with many records should yield the same records in the same order"
    );
}

#[test]
fn scanning_reopened_log_preserves_all_records() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record3 = sample_record(2);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending first record should succeed");
    log.append(&record2)
        .expect("appending second record should succeed");
    log.append(&record3)
        .expect("appending third record should succeed");

    drop(log);

    log = Log::open(&path, RecordLimits::default())
        .expect("opening an existing log path should succeed");

    let scanner = log.scan().expect("scanning a reopened log should succeed");

    assert_eq!(
        vec![record1, record2, record3],
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a reopened log should yield valid records"),
        "scanning a reopened log should yield the same records in the same order"
    );
}

#[test]
fn append_after_scanning_writes_at_end() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending first record should succeed");

    let mut scanner = log
        .scan()
        .expect("scanning a log with one record should succeed");

    let scanned_record1 = scanner
        .next()
        .expect("scanning a log with one record should yield a record")
        .expect("scanning a log with one record should yield a valid record");

    assert_eq!(record1, scanned_record1);

    drop(scanner);

    let file_bytes_after_record_1 =
        read(&path).expect("reading the log file after appending the first record should succeed");

    log.append(&record2)
        .expect("appending second record should succeed");

    let scanner = log
        .scan()
        .expect("scanning a log with two records should succeed");

    let file_bytes_after_record_2 =
        read(&path).expect("reading the log file after appending the second record should succeed");

    assert_eq!(
        vec![record1, record2],
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log with two records should yield valid records"),
        "scanning a log with two records should yield the same records in the same order"
    );
    assert!(
        file_bytes_after_record_2.starts_with(&file_bytes_after_record_1),
        "the log file after appending the second record should start with the contents after the first record"
    );
    assert!(
        file_bytes_after_record_2.len() > file_bytes_after_record_1.len(),
        "the log file after appending the second record should be larger than after the first record"
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
