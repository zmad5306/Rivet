mod common;

use common::sample_record;
use rivet::error::CodecError;
use std::error::Error;
use std::fs::read;
use std::fs::write;

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

    let original_file_bytes = read(&path).expect("reading the log file should succeed");

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
        .expect("encoding the record should succeed");

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("single-record.log");

    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    let file_bytes = read(&path).expect("reading the log file should succeed");

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
        .expect("appending the first record should succeed");

    let file_bytes1 = read(&path).expect("reading the log file should succeed");

    drop(log);

    log = Log::open(&path, RecordLimits::default()).expect("reopening the log should succeed");

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
    let record2 = record_with_fields(0, 1_600_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3, 4]);
    let limits = RecordLimits::new(3, 3);

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("codec-rejection.log");

    let mut log = Log::open(&path, limits).expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending a record should succeed");

    let file_bytes1 = read(&path).expect("reading the log file should succeed");

    log.append(&record2)
        .expect_err("appending an oversized record should fail");

    let file_bytes2 = read(&path).expect("reading the log file should succeed");

    assert_eq!(
        file_bytes1, file_bytes2,
        "appending an oversized record should leave the file unchanged"
    );
}

#[test]
fn opening_invalid_path_preserves_io_error_source() {
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let missing_subdir = dir.path().join("missing-subdir");
    let path = missing_subdir.as_path().join("invalid-path.log");

    let error = Log::open(&path, RecordLimits::default())
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
    let log = Log::open(&path, RecordLimits::default())
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
    let path = dir.path().join("scan-many-records.log");
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
    let path = dir.path().join("scan-reopened.log");
    let mut log = Log::open(&path, RecordLimits::default())
        .expect("opening a missing log path should succeed");

    log.append(&record1)
        .expect("appending the first record should succeed");
    log.append(&record2)
        .expect("appending the second record should succeed");
    log.append(&record3)
        .expect("appending the third record should succeed");

    drop(log);

    log = Log::open(&path, RecordLimits::default()).expect("reopening the log should succeed");

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
    let path = dir.path().join("append-after-scan.log");
    let mut log = Log::open(&path, RecordLimits::default())
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

    assert_eq!(record1, scanned_record1);

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
        vec![record1, record2],
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log with two records should yield valid records"),
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
fn scanning_partial_header_returns_incomplete_header() {
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

    let log = Log::open(&path, RecordLimits::default())
        .expect("opening the log with a partial header should succeed");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the log with a partial header should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    let error = match scanner.next() {
        Some(Err(error)) => error,
        Some(Ok(record)) => panic!(
            "expected an incomplete header error, got a valid record: {:?}",
            record
        ),
        None => panic!("expected an incomplete header error, got None"),
    };

    assert!(matches!(
        error,
        StorageError::Codec(CodecError::IncompleteHeader)
    ));

    assert!(
        scanner.next().is_none(),
        "expected no more records after the incomplete header"
    );
}

#[test]
fn scanning_partial_body_returns_incomplete_body() {
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

    let log = Log::open(&path, RecordLimits::default())
        .expect("opening the log with a partial body should succeed");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the log with a partial body should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    let error = match scanner.next() {
        Some(Err(error)) => error,
        Some(Ok(record)) => panic!(
            "expected an incomplete body error, got a valid record: {:?}",
            record
        ),
        None => panic!("expected an incomplete body error, got None"),
    };

    assert!(matches!(
        error,
        StorageError::Codec(CodecError::IncompleteBody)
    ));

    assert!(
        scanner.next().is_none(),
        "expected no more records after the incomplete body"
    );
}

#[test]
fn scanning_partial_checksum_returns_incomplete_body() {
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

    let log = Log::open(&path, RecordLimits::default())
        .expect("opening the log with a partial checksum should succeed");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the log with a partial checksum should succeed");

    let record1_from_log = match scanner.next() {
        Some(Ok(record)) => record,
        Some(Err(error)) => panic!("expected the first record, got error: {:?}", error),
        None => panic!("expected the first record, got None"),
    };

    assert_eq!(
        record1_from_log, record1,
        "the first record read from the log should match the first record written"
    );

    let error = match scanner.next() {
        Some(Err(error)) => error,
        Some(Ok(record)) => panic!(
            "expected an incomplete checksum error, got a valid record: {:?}",
            record
        ),
        None => panic!("expected an incomplete checksum error, got None"),
    };

    assert!(matches!(
        error,
        StorageError::Codec(CodecError::IncompleteBody)
    ));

    assert!(
        scanner.next().is_none(),
        "expected no more records after the incomplete checksum"
    );
}

#[test]
fn scanning_corrupt_record_preserves_codec_error() {
    let record = sample_record(0);
    let mut bytes = record
        .encode(&RecordLimits::default())
        .expect("encoding the first record should succeed");

    bytes[33] ^= 0x01;

    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("corrupt.log");

    write(&path, &bytes).expect("writing the corrupt record should succeed");

    let log = Log::open(&path, RecordLimits::default())
        .expect("opening the log with a corrupt record should succeed");

    let mut scanner = log
        .scan()
        .expect("creating a scanner for the log with a corrupt record should succeed");

    let error = match scanner.next() {
        Some(Err(error)) => error,
        Some(Ok(record)) => panic!(
            "expected a corrupt record error, got a valid record: {:?}",
            record
        ),
        None => panic!("expected a corrupt record error, got None"),
    };

    assert!(
        matches!(error, StorageError::Codec(CodecError::InvalidChecksum)),
        "expected a corrupt record error with an invalid checksum"
    );

    assert!(
        matches!(
            error.source().unwrap().downcast_ref::<CodecError>(),
            Some(CodecError::InvalidChecksum)
        ),
        "expected the source of the error to be a CodecError::InvalidChecksum"
    );

    assert!(
        scanner.next().is_none(),
        "expected no more records after the corrupt record"
    );
}

#[test]
fn codec_rejection_allows_later_valid_appends() {
    let limits = RecordLimits::new(3, 3);
    let record1 = sample_record(0);
    let oversized_key_record =
        record_with_fields(1, 1_700_000_000, Some(vec![10, 20, 30, 40]), vec![1, 2, 3]);
    let oversized_payload_record =
        record_with_fields(1, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3, 4]);
    let record2 = sample_record(2);
    let dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = dir.path().join("limits.log");
    let mut log = Log::open(&path, limits).expect("opening the log should succeed");

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
        // The parent owns these directories until the child has exited.
        let original = tempfile::tempdir().expect("original directory should be created");
        let destination = tempfile::tempdir().expect("destination directory should be created");
        let destination_path = destination
            .path()
            .canonicalize()
            .expect("destination should have an absolute path");

        // Re-run only this test in a separate process. The marker makes that
        // invocation execute the assertions instead of spawning another child.
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
    let mut log = Log::open(std::path::Path::new("events.log"), RecordLimits::default())
        .expect("opening a relative log path should succeed");
    log.append(&record)
        .expect("appending a record should succeed");

    // Only the child changes directory, so parallel tests remain unaffected.
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
    let mut log =
        Log::open(&path, RecordLimits::default()).expect("opening the log should succeed");

    log.append(&record)
        .expect("appending a record should succeed");

    std::fs::rename(&path, dir.path().join("renamed.log"))
        .expect("renaming the log file should succeed");

    let renamed_path = dir.path().join("renamed.log");
    let renamed_log = Log::open(&renamed_path, RecordLimits::default())
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
    let mut log =
        Log::open(&path, RecordLimits::default()).expect("opening the log should succeed");

    log.append(&original_only_record)
        .expect("appending a record should succeed");

    std::fs::rename(&path, dir.path().join("renamed.log"))
        .expect("renaming the log file should succeed");

    let mut replacement_log = Log::open(&path, RecordLimits::default())
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
    let mut log =
        Log::open(&path, RecordLimits::default()).expect("opening the log should succeed");
    let mut expected_records = Vec::with_capacity(record_count);

    for n in 0..record_count {
        let payload_len = 100 + (n * 137) % 2000;
        let byte = (n % 256) as u8;
        let payload = vec![byte; payload_len];
        let offset = u64::try_from(n).expect("n should fit into u64");
        let timestamp = 1_700_000_000;
        let record = record_with_fields(offset, timestamp, None, payload);
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
    let mut log =
        Log::open(&path, RecordLimits::default()).expect("opening the log should succeed");
    let mut expected_records = Vec::with_capacity(record_count);

    for n in 0..record_count {
        let payload_len = 100 + (n * 137) % 2000;
        let byte = (n % 256) as u8;
        let payload = vec![byte; payload_len];
        let offset = u64::try_from(n).expect("n should fit into u64");
        let timestamp = 1_700_000_000;
        let record = record_with_fields(offset, timestamp, None, payload);
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
        all_records,
        scanner
            .collect::<Result<Vec<_>, _>>()
            .expect("scanning a log after appending should yield valid records"),
        "scanning a log after appending should yield all records including the newly appended one"
    );
}
