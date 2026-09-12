mod common;

use common::sample_record;
use rivet::error::CodecError;
use std::error::Error;
use std::fs::write;
use std::fs::{OpenOptions, read};
use std::io::Write;

use rivet::{
    error::StorageError,
    storage::{
        log::Log,
        record::{Record, RecordLimits},
    },
};

#[test]
fn opening_new_log_creates_empty_file() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("new-log.log");

    Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

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

    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let original_file_bytes = read(&path).expect("reading the log file should succeed");

    drop(log);

    Log::open_active(&path, 0, RecordLimits::default()).expect("reopening the log should succeed");

    let reopened_file_bytes =
        read(&path).expect("existing log should remain readable after reopening");

    assert_eq!(
        reopened_file_bytes, original_file_bytes,
        "reopening the log should preserve the existing file bytes"
    );
}

#[test]
fn append_one_record_writes_expected_encoded_bytes() {
    let record = sample_record(0);
    let expected_bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("single-record.log");

    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let file_bytes = read(&path).expect("reading the log file should succeed");

    assert_eq!(
        file_bytes, expected_bytes,
        "appending a record should write the expected encoded bytes"
    );
}

#[test]
fn later_appends_grow_file_and_preserve_original_prefix() {
    let record1 = sample_record(0);
    let record2 = Record::new(
        0,
        1_600_000_000,
        Some(vec![10, 20, 30, 40, 50]),
        vec![1, 2, 3, 5, 6, 7],
    );

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("append-preserves-prefix.log");

    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending the first record should succeed");

    let file_bytes1 = read(&path).expect("reading the log file should succeed");

    log.append(&record2)
        .expect("appending the second record should succeed");

    let file_bytes2 = read(&path).expect("reading the log file should succeed");

    assert!(
        file_bytes2.len() > file_bytes1.len(),
        "appending the second record should grow the file"
    );

    assert!(
        file_bytes2.starts_with(&file_bytes1),
        "appending the second record should preserve the original file prefix"
    );

    assert!(
        file_bytes2.ends_with(
            &record2
                .encode(&RecordLimits::default())
                .expect("encoding the record should succeed")
        ),
        "appending the second record should write the expected encoded bytes at the end"
    );
}

#[test]
fn append_after_reopening_preserves_previous_records() {
    let record1 = sample_record(0);
    let record2 = Record::new(
        0,
        1_600_000_000,
        Some(vec![10, 20, 30, 40, 50]),
        vec![1, 2, 3, 5, 6, 7],
    );

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("append-after-reopen.log");

    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending the first record should succeed");

    let file_bytes1 = read(&path).expect("reading the log file should succeed");

    drop(log);

    (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");

    log.append(&record2)
        .expect("appending the second record should succeed");

    let file_bytes2 = read(&path).expect("reading the log file should succeed");

    assert!(
        file_bytes2.len() > file_bytes1.len(),
        "appending the second record should grow the file"
    );

    assert!(
        file_bytes2.starts_with(&file_bytes1),
        "appending the second record should preserve the original file prefix"
    );

    assert!(
        file_bytes2.ends_with(
            &record2
                .encode(&RecordLimits::default())
                .expect("encoding the record should succeed")
        ),
        "appending the second record should write the expected encoded bytes at the end"
    );
}

#[test]
fn codec_rejection_leaves_file_unchanged() {
    let record1 = sample_record(0);
    let record2 = Record::new(0, 1_600_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3, 4]);
    let limits = RecordLimits::new(3, 3);

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("codec-rejection.log");

    let (mut log, _) =
        Log::open_active(&path, 0, limits).expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending a record should succeed");

    let file_bytes1 = read(&path).expect("reading the log file should succeed");

    log.append(&record2)
        .expect_err("appending an oversized record should fail");

    let file_bytes2 = read(&path).expect("reading the log file should succeed");

    assert_eq!(
        file_bytes2, file_bytes1,
        "appending an oversized record should leave the file unchanged"
    );
}

#[test]
fn opening_invalid_path_preserves_io_error_source() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let missing_subdir = dir.path().join("missing-subdir");
    let path = missing_subdir.as_path().join("invalid-path.log");

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening a log beneath a missing parent directory should fail");

    assert!(
        matches!(error, StorageError::Io(_)),
        "opening a log beneath a missing parent directory should yield StorageError::Io"
    );
    assert!(
        error.source().is_some(),
        "the source of the error should be exposed"
    );
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some(),
        "the source of the error should be an underlying std::io::Error"
    );
}

#[test]
fn scanning_empty_log_yields_no_records() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("empty.log");
    let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    let mut scanner = log.scan().expect("scanning an empty log should succeed");

    assert!(
        scanner.next().is_none(),
        "scanning an empty log should yield no records"
    );
    assert!(
        scanner.next().is_none(),
        "the exhausted scanner should still yield None on the second call"
    );
    assert!(
        scanner.next().is_none(),
        "the exhausted scanner should still yield None on the third call"
    );
}

#[test]
fn scanning_one_record_preserves_all_fields() {
    let record = sample_record(0);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("scan-single-record.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let scanned_record = log
        .scan()
        .expect("scanning a log with one record should succeed")
        .next()
        .expect("scanning a log with one record should yield a record")
        .expect("scanning a log with one record should yield a valid record");

    assert_eq!(scanned_record, record);
}

#[test]
fn scanning_many_records_preserves_offset_order_and_contents() {
    let records = vec![
        Record::new(0, 1_000, None, vec![]),
        Record::new(1, 1_001, Some(vec![]), vec![0x00]),
        Record::new(
            2,
            1_002,
            Some(b"customer-123".to_vec()),
            vec![0x00, 0xFF, 0x80, 0x0A, 0x42],
        ),
        Record::new(3, 1_003, None, (0u8..=255).collect()),
        Record::new(4, 1_004, Some(vec![0x00, 0xFE, 0x80]), vec![0xAB, 0xCD]),
    ];
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("scan-many-records.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    records.iter().for_each(|record| {
        log.append(record)
            .expect("appending a record should succeed");
    });

    let scanner = log
        .scan()
        .expect("scanning a log with many records should succeed");

    assert_eq!(
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log with many records should yield valid records"),
        records,
        "scanning a log with many records should yield the same records in the same order"
    );
}

#[test]
fn scanning_reopened_log_preserves_all_records() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record3 = sample_record(2);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("scan-reopened.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending the first record should succeed");
    log.append(&record2)
        .expect("appending the second record should succeed");
    log.append(&record3)
        .expect("appending the third record should succeed");

    drop(log);

    (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");

    let scanner = log.scan().expect("scanning a reopened log should succeed");

    assert_eq!(
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a reopened log should yield valid records"),
        vec![record1, record2, record3],
        "scanning a reopened log should yield the same records in the same order"
    );
}

#[test]
fn append_after_scanning_writes_at_end() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("append-after-scan.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending the first record should succeed");

    let mut scanner = log
        .scan()
        .expect("scanning a log with one record should succeed");

    let scanned_record1 = scanner
        .next()
        .expect("scanning a log with one record should yield a record")
        .expect("scanning a log with one record should yield a valid record");

    assert_eq!(scanned_record1, record1);

    drop(scanner);

    let file_bytes_after_record_1 =
        read(&path).expect("reading the log file after appending the first record should succeed");

    log.append(&record2)
        .expect("appending the second record should succeed");

    let scanner = log
        .scan()
        .expect("scanning a log with two records should succeed");

    let file_bytes_after_record_2 =
        read(&path).expect("reading the log file after appending the second record should succeed");

    assert_eq!(
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log with two records should yield valid records"),
        vec![record1, record2],
        "scanning a log with two records should yield the same records in the same order"
    );
    assert!(
        file_bytes_after_record_2.starts_with(&file_bytes_after_record_1),
        "appending after scanning should preserve the original file prefix"
    );
    assert!(
        file_bytes_after_record_2.len() > file_bytes_after_record_1.len(),
        "appending after scanning should grow the file"
    );
}

#[test]
fn opening_truncates_partial_trailing_header() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding the first record should succeed");
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding the second record should succeed");
    let partial_record2_bytes = record2_bytes[..10].to_vec();
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("partial-header.log");
    let bytes = [record1_bytes.as_slice(), partial_record2_bytes.as_slice()].concat();

    write(&path, &bytes)
        .expect("writing a complete record followed by a partial header should succeed");

    let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening should recover the log by truncating the partial trailing header");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the recovered log should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    assert!(
        scanner.next().is_none(),
        "the recovered log should end after the last complete record"
    );

    assert_eq!(
        std::fs::read(&path).expect("reading the recovered file should succeed"),
        record1_bytes,
        "recovery should preserve the complete record and remove only the incomplete tail"
    );
}

#[test]
fn opening_truncates_partial_trailing_body() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding the first record should succeed");
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding the second record should succeed");
    let partial_record2_bytes = record2_bytes[..34].to_vec();
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("partial-body.log");
    let bytes = [record1_bytes.as_slice(), partial_record2_bytes.as_slice()].concat();

    write(&path, &bytes)
        .expect("writing a complete record followed by a partial body should succeed");

    let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening should recover the log by truncating the partial trailing body");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the recovered log should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    assert!(
        scanner.next().is_none(),
        "the recovered log should end after the last complete record"
    );

    assert_eq!(
        std::fs::read(&path).expect("reading the recovered file should succeed"),
        record1_bytes,
        "recovery should preserve the complete record and remove only the incomplete tail"
    );
}

#[test]
fn opening_truncates_partial_trailing_checksum() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding the first record should succeed");
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding the second record should succeed");
    let partial_record2_bytes = record2_bytes[..38].to_vec();
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("partial-checksum.log");
    let bytes = [record1_bytes.as_slice(), partial_record2_bytes.as_slice()].concat();

    write(&path, &bytes)
        .expect("writing a complete record followed by a partial checksum should succeed");

    let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening should recover the log by truncating the partial trailing checksum");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the recovered log should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    assert!(
        scanner.next().is_none(),
        "the recovered log should end after the last complete record"
    );

    assert_eq!(
        std::fs::read(&path).expect("reading the recovered file should succeed"),
        record1_bytes,
        "recovery should preserve the complete record and remove only the incomplete tail"
    );
}

#[test]
fn opening_rejects_invalid_checksum_without_modifying_file() {
    let record = sample_record(0);
    let mut bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the first record should succeed");

    bytes[33] ^= 0x01;

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("corrupt.log");

    write(&path, &bytes).expect("writing the corrupt record should succeed");

    let error = Log::open_active(&path, 0, RecordLimits::default()).expect_err(
        "opening a log with a complete record containing an invalid checksum should fail",
    );

    assert!(
        matches!(
            error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidChecksum,
                ..
            }
        ),
        "opening should report CodecError::InvalidChecksum"
    );

    assert!(
        matches!(
            error.source().unwrap().downcast_ref::<CodecError>(),
            Some(CodecError::InvalidChecksum)
        ),
        "expected the source of the error to be a CodecError::InvalidChecksum"
    );

    assert_eq!(
        std::fs::read(&path).expect("reading the corrupt file should succeed"),
        bytes,
        "failed recovery must leave the corrupt file unchanged"
    );
}

#[test]
fn first_corrupt_record_reports_path_and_byte_position_zero() {
    let record = sample_record(0);
    let mut record_bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");
    let payload_start = record_bytes.len() - record.payload().len() - 4;
    record_bytes[payload_start] ^= 0xFF; // Mutate the payload without changing the checksum

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("corrupt.log");

    write(&path, &record_bytes).expect("writing the corrupt record should succeed");

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening a log with a corrupt record should fail");

    assert!(
        matches!(
            &error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidChecksum,
                path: error_path,
                byte_position: 0,
            } if error_path == &path
        ),
        "expected a corrupt record error with an invalid checksum"
    );
}

#[test]
fn mid_log_corrupt_record_reports_path_and_record_start() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");
    let record2 = sample_record(1);
    let mut record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");
    let payload_start = record2_bytes.len() - record2.payload().len() - 4;
    record2_bytes[payload_start] ^= 0xFF; // Mutate the payload without changing the checksum

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("corrupt.log");

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("opening the log for appending should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the good record should succeed");
    file.write_all(&record2_bytes)
        .expect("writing the corrupt record should succeed");

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening a log with a corrupt record should fail");

    let expected_byte_position =
        u64::try_from(record1_bytes.len()).expect("conversion should succeed");

    assert!(
        matches!(
            &error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidChecksum,
                path: error_path,
                byte_position: actual_byte_position,
            } if error_path == &path && actual_byte_position == &expected_byte_position
        ),
        "expected a corrupt record error with an invalid checksum"
    );
}

#[test]
fn codec_rejection_allows_later_valid_appends() {
    let limits = RecordLimits::new(3, 3);
    let record1 = sample_record(0);
    let oversized_key_record =
        Record::new(1, 1_700_000_000, Some(vec![10, 20, 30, 40]), vec![1, 2, 3]);
    let oversized_payload_record =
        Record::new(1, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3, 4]);
    let record2 = sample_record(2);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("limits.log");
    let (mut log, _) = Log::open_active(&path, 0, limits).expect("opening the log should succeed");

    log.append(&record1)
        .expect("appending the first valid record should succeed");
    let error1 = log
        .append(&oversized_key_record)
        .expect_err("appending a record with oversized key should fail");
    let error2 = log
        .append(&oversized_payload_record)
        .expect_err("appending a record with oversized payload should fail");
    log.append(&record2)
        .expect("appending the second valid record should succeed");

    assert!(
        matches!(error1, StorageError::Codec(CodecError::KeyTooLarge)),
        "expected a key too large error"
    );
    assert!(
        matches!(error2, StorageError::Codec(CodecError::PayloadTooLarge)),
        "expected a payload too large error"
    );

    let scanner = log
        .scan()
        .expect("creating a scanner for the log should succeed");
    let records: Vec<_> = scanner.filter_map(Result::ok).collect();

    assert_eq!(
        records,
        vec![record1, record2],
        "expected exactly the two valid records in order"
    );
}

#[test]
fn scanning_relative_path_survives_working_directory_change() {
    const CHILD_MARKER: &str = "RIVET_CWD_TEST_CHILD";
    const DESTINATION: &str = "RIVET_CWD_TEST_DEST";

    if std::env::var_os(CHILD_MARKER).is_none() {
        let original = tempfile::tempdir().expect("original directory should be created");
        let destination = tempfile::tempdir().expect("destination directory should be created");
        let destination_path = destination
            .path()
            .canonicalize()
            .expect("destination should have an absolute path");

        let output = std::process::Command::new(
            std::env::current_exe().expect("test executable should be located"),
        )
        .args([
            "--exact",
            "scanning_relative_path_survives_working_directory_change",
            "--nocapture",
        ])
        .env(CHILD_MARKER, "1")
        .env(DESTINATION, destination_path)
        .current_dir(original.path())
        .output()
        .expect("child test process should run");

        assert!(
            output.status.success(),
            "child test failed ({})\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }

    let record = sample_record(0);
    let (mut log, _) = Log::open_active(
        std::path::Path::new("events.log"),
        0,
        RecordLimits::default(),
    )
    .expect("opening a relative log path should succeed");
    log.append(&record)
        .expect("appending a record should succeed");

    let destination = std::env::var_os(DESTINATION).expect("parent should supply the destination");
    std::env::set_current_dir(destination).expect("changing directory should succeed");

    let records = log
        .scan()
        .expect("creating a scanner after changing directory should succeed")
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the opened log should succeed");
    assert_eq!(
        records,
        vec![record],
        "scanning should read the original log after changing directory"
    );
}

#[test]
fn scanning_renamed_log_reads_original_file() {
    let record = sample_record(0);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("original.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    std::fs::rename(&path, dir.path().join("renamed.log"))
        .expect("renaming the log file should succeed");

    let renamed_path = dir.path().join("renamed.log");
    let (renamed_log, _) = Log::open_active(&renamed_path, 0, RecordLimits::default())
        .expect("opening the renamed log should succeed");
    let scanner = renamed_log
        .scan()
        .expect("creating a scanner for the renamed log should succeed");
    let records = scanner
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the renamed log should succeed");
    assert_eq!(
        records,
        vec![record],
        "scanning the renamed log should read the original record"
    );
}

#[test]
fn scanning_replaced_path_reads_original_file() {
    let original_only_record = sample_record(0);
    let shared_record = sample_record(1);
    let replacement_only_record = sample_record(2);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("original.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");

    log.append(&original_only_record)
        .expect("appending a record should succeed");

    std::fs::rename(&path, dir.path().join("renamed.log"))
        .expect("renaming the log file should succeed");

    let (mut replacement_log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the replacement log should succeed");

    replacement_log
        .append(&shared_record)
        .expect("appending a record to the replacement log should succeed");

    log.append(&shared_record)
        .expect("appending a record through the original log should succeed");
    replacement_log
        .append(&replacement_only_record)
        .expect("appending a new record through the replacement log should succeed");

    let scanner = log
        .scan()
        .expect("creating a scanner for the original log should succeed");
    let records = scanner
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the original log should succeed");

    assert_eq!(
        records,
        vec![original_only_record, shared_record],
        "scanning the original log should read the original and shared records"
    );
}

#[test]
fn simultaneous_scanners_have_independent_positions() {
    let record_count: usize = 99;
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("original.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    let mut expected_records = Vec::with_capacity(record_count);

    for n in 0..record_count {
        let payload_len = 100 + (n * 137) % 2000;
        let byte = (n % 256) as u8;
        let payload = vec![byte; payload_len];
        let offset = u64::try_from(n).expect("n should fit into u64");
        let timestamp = 1_700_000_000;
        let record = Record::new(offset, timestamp, None, payload);
        log.append(&record)
            .expect("appending a record should succeed");
        expected_records.push(record);
    }

    let mut scanner1 = log
        .scan()
        .expect("creating the first scanner should succeed");
    let mut scanner2 = log
        .scan()
        .expect("creating the second scanner should succeed");

    let mut scanner1_index = 0;
    let mut scanner2_index = 0;

    while scanner1_index < record_count {
        let mut z = 0;
        while z < 3 && scanner1_index < record_count {
            let result = scanner1.next();
            match result {
                Some(Ok(record)) => {
                    assert_eq!(
                        record, expected_records[scanner1_index],
                        "scanner1 should yield the expected record"
                    );
                }
                Some(Err(e)) => panic!("scanner1 encountered an error: {:?}", e),
                None => panic!("scanner1 reached EOF unexpectedly"),
            }
            z += 1;
            scanner1_index += 1;
        }
        let result2 = scanner2.next();
        match result2 {
            Some(Ok(record)) => {
                assert_eq!(
                    record, expected_records[scanner2_index],
                    "scanner2 should yield the expected record"
                );
            }
            Some(Err(e)) => panic!("scanner2 encountered an error: {:?}", e),
            None => panic!("scanner2 reached EOF unexpectedly"),
        }
        scanner2_index += 1;
    }

    while scanner2_index < record_count {
        let result = scanner2.next();
        match result {
            Some(Ok(record)) => {
                assert_eq!(
                    record, expected_records[scanner2_index],
                    "scanner2 should yield the expected record"
                );
            }
            Some(Err(e)) => panic!("scanner2 encountered an error: {:?}", e),
            None => panic!("scanner2 reached EOF unexpectedly"),
        }
        scanner2_index += 1;
    }

    assert!(
        scanner1.next().is_none(),
        "scanner1 should have reached EOF"
    );
    assert!(
        scanner2.next().is_none(),
        "scanner2 should have reached EOF"
    );
}

#[test]
fn append_after_partial_scan_preserves_unread_records() {
    let record_count: usize = 99;
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("original.log");
    let (mut log, _) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    let mut expected_records = Vec::with_capacity(record_count);

    for n in 0..record_count {
        let payload_len = 100 + (n * 137) % 2000;
        let byte = (n % 256) as u8;
        let payload = vec![byte; payload_len];
        let offset = u64::try_from(n).expect("n should fit into u64");
        let timestamp = 1_700_000_000;
        let record = Record::new(offset, timestamp, None, payload);
        log.append(&record)
            .expect("appending a record should succeed");
        expected_records.push(record);
    }

    let mut scanner = log
        .scan()
        .expect("creating the first scanner should succeed");

    let result = scanner.next();
    match result {
        Some(Ok(record)) => {
            assert_eq!(
                record, expected_records[0],
                "scanner should yield the expected record"
            );
        }
        Some(Err(e)) => panic!("scanner1 encountered an error: {:?}", e),
        None => panic!("scanner1 reached EOF unexpectedly"),
    }

    drop(scanner);

    let record = sample_record(u64::try_from(record_count).expect("should fit into u64"));

    log.append(&record)
        .expect("appending a record should succeed");

    let mut all_records = expected_records;
    all_records.push(record);

    scanner = log
        .scan()
        .expect("creating the scanner after appending should succeed");

    assert_eq!(
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log after appending should yield valid records"),
        all_records,
        "scanning a log after appending should yield all records including the newly appended one"
    );
}

#[test]
fn recovery_restores_next_offset_after_zero_records() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("zero.log");
    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    assert_eq!(next_offset, 0, "next offset should be 0 for an empty log");
    let record = sample_record(next_offset);
    log.append(&record)
        .expect("appending a record should succeed");
    drop(log);
    let (_, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    assert_eq!(
        next_offset, 1,
        "next offset should be 1 after appending one record"
    );
}

#[test]
fn recovery_restores_next_offset_after_one_record() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("one.log");
    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    assert_eq!(next_offset, 0, "next offset should be 0 for an empty log");

    let record1 = sample_record(next_offset);
    log.append(&record1)
        .expect("appending a record should succeed");
    drop(log);

    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    assert_eq!(
        next_offset, 1,
        "next offset should be 1 after appending one record"
    );

    let record2 = sample_record(next_offset);
    log.append(&record2)
        .expect("appending a second record should succeed");
    drop(log);

    let (log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    assert_eq!(
        next_offset, 2,
        "next offset should be 2 after appending two records"
    );

    let scanner = log.scan().expect("scanning the log should succeed");
    let records = scanner
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the original log should succeed");
    assert_eq!(
        records,
        vec![record1, record2],
        "scanned records should match appended records"
    );
}

#[test]
fn recovery_restores_next_offset_after_one_hundred_records() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("one_hundred.log");
    let mut records = Vec::new();
    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    assert_eq!(next_offset, 0, "next offset should be 0 for an empty log");

    for i in 0..100 {
        let record = sample_record(next_offset + i);
        log.append(&record)
            .expect("appending a record should succeed");
        records.push(record);
    }
    drop(log);

    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    assert_eq!(
        next_offset, 100,
        "next offset should be 100 after appending 100 records"
    );
    let scanner = log.scan().expect("scanning the log should succeed");
    let scanned_records = scanner
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the reopened log should succeed");
    assert_eq!(
        scanned_records, records,
        "scanned records should match appended records after recovery"
    );

    let record101 = sample_record(next_offset);
    log.append(&record101)
        .expect("appending the 101st record should succeed");
    drop(log);

    let (log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    assert_eq!(
        next_offset, 101,
        "next offset should be 101 after appending 101 records"
    );
    let scanner = log.scan().expect("scanning the log should succeed");
    let scanned_records = scanner
        .collect::<Result<Vec<_>, _>>()
        .expect("scanning the reopened log should succeed");
    let mut all_records = records;
    all_records.push(record101);
    assert_eq!(
        scanned_records, all_records,
        "scanned records should match appended records after recovery"
    );
}

#[test]
fn recovery_truncates_every_incomplete_record_prefix() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let encoded_bytes = sample_record(1)
        .encode(&RecordLimits::default())
        .expect("encoding the second record should succeed");

    for n in 0..encoded_bytes.len() {
        let record = sample_record(0);
        let path = dir.path().join(format!("trunc-{}.log", n));
        let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
            .expect("opening the log should succeed");
        assert_eq!(next_offset, 0, "next offset should be 0 for an empty log");

        log.append(&record)
            .expect("appending the first record should succeed");

        drop(log);

        let bytes = &encoded_bytes[..n];

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("opening the log file for appending should succeed");

        file.write_all(bytes)
            .expect("writing the partial record bytes should succeed");
        drop(file);

        let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
            .expect("reopening the log should succeed");
        assert_eq!(
            next_offset, 1,
            "next offset should remain 1 after truncating the incomplete record prefix"
        );

        let mut scanner = log.scan().expect("scanning the log should succeed");
        let scanned_record = scanner
            .next()
            .expect("scanning the log should yield the first record")
            .expect("scanning the log should succeed");
        assert_eq!(
            scanned_record, record,
            "the first scanned record should match the appended record"
        );
        assert_eq!(
            std::fs::read(&path).expect("reading the log file should succeed"),
            record
                .encode(&RecordLimits::default())
                .expect("encoding the first record should succeed")
        );

        let second_record = sample_record(next_offset);
        log.append(&second_record)
            .expect("appending the record after recovery should succeed");

        let scanner = log
            .scan()
            .expect("scanning the log after appending should succeed");
        let scanned_records = scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning the log after appending should succeed");
        assert_eq!(
            scanned_records,
            vec![record, second_record],
            "scanned records should include the newly appended record"
        );
    }
}

#[test]
fn repeated_recovery_preserves_file_bytes_and_next_offset() {
    let record1 = sample_record(0);
    let record2 = sample_record(1);
    let record3 = sample_record(2);
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding the third record should succeed");
    let partial_record3_bytes = record3_bytes[..record3_bytes.len() / 2].to_vec();
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("preserve_bytes.log");

    let (mut log, next_offset) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("opening the log should succeed");
    assert_eq!(
        next_offset, 0,
        "the next offset should be 0 after opening an empty log"
    );
    log.append(&record1)
        .expect("appending the first record should succeed");
    log.append(&record2)
        .expect("appending the second record should succeed");

    drop(log);

    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("opening the log file for appending should succeed");

    file.write_all(&partial_record3_bytes)
        .expect("writing the partial record bytes should succeed");
    drop(file);

    let (log, next_offset1) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    let log_bytes1 = std::fs::read(&path).expect("reading the log file bytes should succeed");

    drop(log);

    let (_, next_offset2) = Log::open_active(&path, 0, RecordLimits::default())
        .expect("reopening the log should succeed");
    let log_bytes2 = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert_eq!(
        log_bytes2, log_bytes1,
        "log bytes should be identical after reopening"
    );
    assert_eq!(
        next_offset2, next_offset1,
        "next offsets should be identical after reopening"
    );
    assert_eq!(
        next_offset1, 2,
        "the next offset should be 2 after reopening the log the first time"
    );
    assert_eq!(
        next_offset2, 2,
        "the next offset should be 2 after reopening the log the second time"
    );
}

#[test]
fn recovery_rejects_offset_gaps_without_modifying_file() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2 = sample_record(2);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("gaps.log");

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .expect("opening the log file for appending should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&record2_bytes)
        .expect("writing the second record bytes should succeed");

    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with offset gaps should fail");
    let log_bytes_after_error =
        std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 1,
                actual: 2
            }
        ),
        "error should be UnexpectedOffset with expected 1 and actual 2"
    );
    assert_eq!(
        log_bytes_after_error,
        [&record1_bytes[..], &record2_bytes[..]].concat(),
        "log bytes should be unchanged after failing to open due to offset gaps"
    );
}

#[test]
fn recovery_rejects_offset_gaps_at_beginning_without_modifying_file() {
    let record = sample_record(1);
    let record_bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding record should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("initial_gaps.log");

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .expect("opening the log file for appending should succeed");
    file.write_all(&record_bytes)
        .expect("writing the record bytes should succeed");

    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with offset gaps should fail");
    let log_bytes_after_error =
        std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 0,
                actual: 1
            }
        ),
        "error should be UnexpectedOffset with expected 0 and actual 1"
    );
    assert_eq!(
        log_bytes_after_error, record_bytes,
        "log bytes should be unchanged after failing to open due to offset gaps"
    );
}

#[test]
fn recovery_rejects_duplicate_offsets_without_modifying_file() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2 = sample_record(0);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("duplicate_offsets.log");

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .expect("opening the log file for appending should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&record2_bytes)
        .expect("writing the second record bytes should succeed");

    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with offset gaps should fail");
    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 1,
                actual: 0
            }
        ),
        "error should be UnexpectedOffset with expected 1 and actual 0"
    );
    assert_eq!(
        file_bytes,
        [&record1_bytes[..], &record2_bytes[..]].concat(),
        "file bytes should be unchanged after failing to open due to duplicate offsets"
    );
}

#[test]
fn recovery_rejects_regressing_offsets_without_modifying_file() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2 = sample_record(1);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");
    let record3 = sample_record(0);
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding record3 should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("regressing_offsets.log");

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .expect("opening the log file for appending should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&record2_bytes)
        .expect("writing the second record bytes should succeed");
    file.write_all(&record3_bytes)
        .expect("writing the third record bytes should succeed");

    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with regressing offsets should fail");
    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 2,
                actual: 0
            }
        ),
        "error should be UnexpectedOffset with expected 2 and actual 0"
    );
    assert_eq!(
        file_bytes,
        [&record1_bytes[..], &record2_bytes[..], &record3_bytes[..]].concat(),
        "file bytes should be unchanged after failing to open due to regressing offsets"
    );
}

#[test]
fn recovery_rejects_mid_log_invalid_magic_without_modifying_file() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2 = sample_record(1);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");
    let record3 = sample_record(0);
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding record3 should succeed");
    let mut modified_record2_bytes = record2_bytes.clone();
    modified_record2_bytes[0] ^= 0xFF;

    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("invalid_magic.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&modified_record2_bytes)
        .expect("writing the modified second record bytes should succeed");
    file.write_all(&record3_bytes)
        .expect("writing the third record bytes should succeed");
    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with invalid magic should fail");
    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidMagic,
                ..
            }
        ),
        "opening should report CodecError::InvalidMagic"
    );
    assert_eq!(
        file_bytes,
        [
            &record1_bytes[..],
            &modified_record2_bytes[..],
            &record3_bytes[..]
        ]
        .concat(),
        "file bytes should be unchanged after failing to open due to invalid magic"
    );
}

#[test]
fn mid_log_invalid_magic_reports_context_and_source() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record should succeed");
    let record2 = sample_record(1);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");
    let modified_record2_bytes = {
        let mut bytes = record2_bytes.clone();
        bytes[0] = 0; // Invalidate the magic byte
        bytes
    };
    let record3 = sample_record(2);
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding record3 should succeed");
    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("mid_log_invalid_magic.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&modified_record2_bytes)
        .expect("writing the modified second record bytes should succeed");
    file.write_all(&record3_bytes)
        .expect("writing the third record bytes should succeed");
    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with invalid magic should fail");
    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidMagic,
                ..
            }
        ),
        "error should be Codec(InvalidMagic)"
    );
    assert_eq!(
        file_bytes,
        [
            &record1_bytes[..],
            &modified_record2_bytes[..],
            &record3_bytes[..]
        ]
        .concat(),
        "file bytes should be unchanged after failing to open due to invalid magic"
    );

    let mut asserted = false;

    if let StorageError::CorruptRecord {
        path: error_path,
        byte_position,
        source,
    } = &error
    {
        assert_eq!(
            error_path, &path,
            "error path should match the expected path"
        );
        assert_eq!(
            byte_position,
            &(record1_bytes.len() as u64),
            "byte position should point to the start of the corrupted record"
        );
        assert!(
            matches!(source, CodecError::InvalidMagic),
            "error source should be Codec(InvalidMagic)"
        );
        asserted = true;
    }
    assert!(asserted, "the error should have been asserted");
}

#[test]
fn recovery_rejects_mid_log_invalid_checksum_without_modifying_file() {
    let record1 = sample_record(0);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2 = sample_record(1);
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");
    let record3 = sample_record(0);
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding record3 should succeed");
    let mut modified_record2_bytes = record2_bytes.clone();
    let payload_start = record2_bytes.len() - record2.payload().len() - 4;
    modified_record2_bytes[payload_start] ^= 0xFF; // Mutate the payload without changing the checksum

    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("invalid_checksum.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");
    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&modified_record2_bytes)
        .expect("writing the modified second record bytes should succeed");
    file.write_all(&record3_bytes)
        .expect("writing the third record bytes should succeed");
    drop(file);

    let error = Log::open_active(&path, 0, RecordLimits::default())
        .expect_err("opening the log with invalid checksum should fail");
    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");

    assert!(
        matches!(
            error,
            StorageError::CorruptRecord {
                source: CodecError::InvalidChecksum,
                ..
            }
        ),
        "error should be Codec(InvalidChecksum)"
    );
    assert_eq!(
        file_bytes,
        [
            &record1_bytes[..],
            &modified_record2_bytes[..],
            &record3_bytes[..]
        ]
        .concat(),
        "file bytes should be unchanged after failing to open due to invalid checksum"
    );
}

#[test]
fn recovery_of_empty_log_returns_nonzero_base_offset() {
    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("empty.log");
    let base_offset = 42;

    let (_, next_offset) = Log::open_active(&path, base_offset, RecordLimits::default())
        .expect("opening the missing log should succeed");

    assert_eq!(
        next_offset, base_offset,
        "next offset should equal the supplied base offset"
    );

    let file_bytes = std::fs::read(&path).expect("reading the log file bytes should succeed");
    assert!(file_bytes.is_empty(), "newly created file should be empty");
}

#[test]
fn recovery_restores_next_offset_from_nonzero_base() {
    let record1 = sample_record(42);
    let record2 = sample_record(43);
    let record3 = sample_record(44);
    let record1_bytes = record1
        .encode(&RecordLimits::default())
        .expect("encoding record1 should succeed");
    let record2_bytes = record2
        .encode(&RecordLimits::default())
        .expect("encoding record2 should succeed");
    let record3_bytes = record3
        .encode(&RecordLimits::default())
        .expect("encoding record3 should succeed");
    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("invalid_checksum.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");

    file.write_all(&record1_bytes)
        .expect("writing the first record bytes should succeed");
    file.write_all(&record2_bytes)
        .expect("writing the modified second record bytes should succeed");
    file.write_all(&record3_bytes)
        .expect("writing the third record bytes should succeed");
    drop(file);

    let (log, next_offset) = Log::open_active(&path, 42, RecordLimits::default())
        .expect("opening the log should succeed");
    assert_eq!(next_offset, 45, "recovery should return the next offset 45");

    let scanner = log.scan().expect("scanning the log should succeed");
    let recovered_offsets: Vec<u64> = scanner
        .map(|res| res.expect("reading a record should succeed").offset())
        .collect();
    assert_eq!(
        recovered_offsets,
        vec![42, 43, 44],
        "recovered record offsets should match the original offsets"
    );

    let file_bytes_after_recovery =
        std::fs::read(&path).expect("reading the log file bytes should succeed");
    let mut expected_bytes = Vec::new();
    expected_bytes.extend_from_slice(&record1_bytes);
    expected_bytes.extend_from_slice(&record2_bytes);
    expected_bytes.extend_from_slice(&record3_bytes);
    assert_eq!(
        file_bytes_after_recovery, expected_bytes,
        "file bytes should remain unchanged after successful recovery"
    );
}

#[test]
fn recovery_rejects_first_offset_below_nonzero_base_without_modifying_file() {
    let base_offset = 42;
    let record = sample_record(base_offset - 1);
    let record_bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");

    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("invalid_offset.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");
    file.write_all(&record_bytes)
        .expect("writing the record bytes should succeed");
    drop(file);

    let original_file_bytes =
        std::fs::read(&path).expect("reading the original log file bytes should succeed");

    let result = Log::open_active(&path, base_offset, RecordLimits::default());
    assert!(
        matches!(result, Err(StorageError::UnexpectedOffset { expected, actual }) if expected == base_offset && actual == base_offset - 1)
    );

    let file_bytes_after_failed_recovery = std::fs::read(&path)
        .expect("reading the log file bytes after failed recovery should succeed");
    assert_eq!(
        file_bytes_after_failed_recovery, original_file_bytes,
        "file bytes should remain unchanged after failed recovery"
    );
}

#[test]
fn recovery_rejects_first_offset_above_nonzero_base_without_modifying_file() {
    let base_offset = 42;
    let record = sample_record(base_offset + 1);
    let record_bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the record should succeed");

    let dir = tempfile::tempdir().expect("creating temp dir should succeed");
    let path = dir.path().join("invalid_offset.log");
    let mut file = std::fs::File::create(&path).expect("creating the log file should succeed");
    file.write_all(&record_bytes)
        .expect("writing the record bytes should succeed");
    drop(file);

    let original_file_bytes =
        std::fs::read(&path).expect("reading the original log file bytes should succeed");

    let result = Log::open_active(&path, base_offset, RecordLimits::default());
    assert!(
        matches!(result, Err(StorageError::UnexpectedOffset { expected, actual }) if expected == base_offset && actual == base_offset + 1)
    );

    let file_bytes_after_failed_recovery = std::fs::read(&path)
        .expect("reading the log file bytes after failed recovery should succeed");
    assert_eq!(
        file_bytes_after_failed_recovery, original_file_bytes,
        "file bytes should remain unchanged after failed recovery"
    );
}

#[test]
fn opening_missing_closed_log_fails_without_creating_file() {
    todo!(
        "Implement this test in this order:\n\
         1. Create a temporary directory and choose a path that does not exist.\n\
         2. Call the closed/read-only Log opening API with base offset 0 and default record limits.\n\
         3. Extract the returned StorageError without panicking.\n\
         4. Assert that the error retains an std::io::Error whose kind is NotFound.\n\
         5. Assert that the path still does not exist, proving closed opening never creates a file."
    )
}

#[test]
fn closed_log_validates_and_scans_records_from_nonzero_base() {
    todo!(
        "Implement this test in this order:\n\
         1. Choose a nonzero base offset and construct several consecutive records beginning there.\n\
         2. Encode the records with one RecordLimits value and write them consecutively to a temporary file.\n\
         3. Save the complete file bytes before opening it.\n\
         4. Open the file through the closed/read-only Log API with the chosen base offset and equivalent limits.\n\
         5. Assert that validation reports the offset immediately after the final record.\n\
         6. Scan the returned log and assert that every offset and record is preserved in order.\n\
         7. Read the file again and assert that closed validation did not change its bytes."
    )
}

#[test]
fn closed_log_rejects_every_incomplete_tail_without_modifying_file() {
    todo!(
        "Implement this test in this order:\n\
         1. Encode one complete record at a chosen nonzero base offset.\n\
         2. Encode the following record and iterate over every incomplete nonempty prefix of its bytes.\n\
         3. For each prefix, create a separate temporary file containing the complete record followed by that prefix.\n\
         4. Save the exact bytes before opening the file.\n\
         5. Attempt to open it through the closed/read-only Log API using the chosen base offset.\n\
         6. Assert that opening returns the appropriate incomplete-header or incomplete-body storage/codec error.\n\
         7. Read the file again and assert byte-for-byte equality with the saved contents.\n\
         8. Include the prefix length in assertion messages so a failure identifies the boundary."
    )
}

#[test]
fn active_and_closed_opening_treat_same_incomplete_tail_differently() {
    todo!(
        "Implement this test in this order:\n\
         1. Build bytes containing one complete record followed by an incomplete prefix of the next record.\n\
         2. Write identical bytes to two different temporary files.\n\
         3. Open the first file through the closed/read-only API and assert that it fails without changing the bytes.\n\
         4. Open the second file through the active API and assert that it succeeds.\n\
         5. Assert that active recovery returns the offset after the complete record.\n\
         6. Assert that the active file was truncated exactly to the complete record's encoded length.\n\
         7. Scan the recovered active log and assert that it contains only the complete record."
    )
}
