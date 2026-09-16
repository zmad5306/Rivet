use std::{
    fs::{create_dir_all, read_dir},
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use crate::{
    error::{CodecError, ConfigurationError, StorageError},
    storage::{
        log::{Log, LogScanner},
        record::{Record, RecordLimits},
    },
};

const SEGMENT_SIZE: u64 = 1024 * 1024 * 64; // 64 MB segment size

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct SegmentConfig {
    max_segment_bytes: NonZeroU64,
}

impl SegmentConfig {
    pub fn new(max_segment_bytes: u64) -> Result<Self, ConfigurationError> {
        let max_segment_bytes = match NonZeroU64::new(max_segment_bytes) {
            Some(non_zero) => non_zero,
            None => {
                return Err(ConfigurationError::InvalidSegmentBytes {
                    value: max_segment_bytes,
                });
            }
        };

        Ok(Self { max_segment_bytes })
    }

    pub fn max_segment_bytes(&self) -> u64 {
        self.max_segment_bytes.get()
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: NonZeroU64::new(SEGMENT_SIZE).expect("SEGMENT_SIZE is non-zero"),
        }
    }
}

pub struct SegmentedLogScanner<'a> {
    log: &'a SegmentedLog,
    segment_index: usize,
    current_scanner: Option<LogScanner<'a>>,
    finished: bool,
}

impl<'a> Iterator for SegmentedLogScanner<'a> {
    type Item = Result<Record, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(scanner) = &mut self.current_scanner
                && let Some(result) = scanner.next()
            {
                match result {
                    Ok(record) => return Some(Ok(record)),
                    Err(err) => {
                        self.finished = true;
                        self.current_scanner = None;
                        return Some(Err(err));
                    }
                }
            }

            self.segment_index += 1;
            self.current_scanner = match self.log.get_segment_scanner(self.segment_index) {
                Ok(Some(scanner)) => Some(scanner),
                Ok(None) => {
                    self.finished = true;
                    return None;
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SegmentMetadata {
    base_offset: u64,
    path: PathBuf,
    file_len: u64,
}

impl SegmentMetadata {
    fn new(base_offset: u64, path: &Path, file_len: u64) -> Self {
        Self {
            base_offset,
            path: path.to_path_buf(),
            file_len,
        }
    }

    fn base_offset(&self) -> u64 {
        self.base_offset
    }

    fn file_len(&self) -> u64 {
        self.file_len
    }

    fn filename(base_offset: u64) -> String {
        format!("{base_offset:020}.log")
    }

    fn parse_base_offset(path: &Path) -> Result<u64, StorageError> {
        let filename = match path.file_name() {
            Some(filename) => match filename.to_str() {
                Some(s) => s,
                None => {
                    return Err(StorageError::InvalidSegmentFilename {
                        path: path.to_path_buf(),
                    });
                }
            },
            None => {
                return Err(StorageError::InvalidSegmentFilename {
                    path: path.to_path_buf(),
                });
            }
        };

        let padded_segment_number = match filename.strip_suffix(".log") {
            Some(psn) => {
                if psn.len() != 20 {
                    return Err(StorageError::InvalidSegmentFilename {
                        path: path.to_path_buf(),
                    });
                }
                for byte in psn.bytes() {
                    if !byte.is_ascii_digit() {
                        return Err(StorageError::InvalidSegmentFilename {
                            path: path.to_path_buf(),
                        });
                    }
                }
                psn
            }
            None => {
                return Err(StorageError::InvalidSegmentFilename {
                    path: path.to_path_buf(),
                });
            }
        };

        match padded_segment_number.parse::<u64>() {
            Ok(base_offset) => Ok(base_offset),
            Err(_) => Err(StorageError::InvalidSegmentFilename {
                path: path.to_path_buf(),
            }),
        }
    }
}

#[derive(Debug)]
struct ClosedSegment {
    metadata: SegmentMetadata,
    log: Log,
}

impl ClosedSegment {
    fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        self.log.scan()
    }
}

#[derive(Debug)]
struct ActiveSegment {
    metadata: SegmentMetadata,
    log: Log,
}

impl ActiveSegment {
    fn append(&mut self, record: &Record) -> Result<(), StorageError> {
        let encoded_len = record.encoded_len()?;
        let encoded_length = u64::try_from(encoded_len)
            .map_err(|_| StorageError::Codec(CodecError::LengthOverflow))?;
        let future_file_len = self
            .metadata
            .file_len()
            .checked_add(encoded_length)
            .ok_or(StorageError::Codec(CodecError::LengthOverflow))?;

        self.log.append(record)?;
        self.metadata.file_len = future_file_len;

        Ok(())
    }

    fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        self.log.scan()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SegmentCandidate {
    base_offset: u64,
    path: PathBuf,
    file_len: u64,
}

#[derive(Debug)]
pub struct SegmentedLog {
    directory: PathBuf,
    closed_segments: Vec<ClosedSegment>,
    active_segment: ActiveSegment,
    next_offset: u64,
    limits: RecordLimits,
    config: SegmentConfig,
    append_disabled: bool,
}

impl SegmentedLog {
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn limits(&self) -> RecordLimits {
        self.limits
    }

    pub fn config(&self) -> &SegmentConfig {
        &self.config
    }

    fn discover_segments(directory: &Path) -> Result<Vec<SegmentCandidate>, StorageError> {
        create_dir_all(directory)?;
        let mut candidates = Vec::new();
        for entry in read_dir(directory)? {
            let dir_entry = entry?;
            let path = dir_entry.path();
            let file_type = dir_entry.file_type()?;

            if !file_type.is_file() {
                return Err(StorageError::UnexpectedSegmentEntry { path });
            }

            let base_offset = SegmentMetadata::parse_base_offset(&path)?;
            let file_len = dir_entry.metadata()?.len();

            candidates.push(SegmentCandidate {
                base_offset,
                path,
                file_len,
            });
        }

        candidates.sort_by_key(|candidate| candidate.base_offset);

        for pair in candidates.windows(2) {
            let current = &pair[0];
            let next = &pair[1];
            if current.base_offset == next.base_offset {
                return Err(StorageError::DuplicateSegmentBaseOffset {
                    base_offset: current.base_offset,
                    first_path: current.path.clone(),
                    second_path: next.path.clone(),
                });
            }
        }

        Ok(candidates)
    }

    pub fn open(
        directory: &Path,
        limits: RecordLimits,
        config: SegmentConfig,
    ) -> Result<Self, StorageError> {
        let mut candidates = Self::discover_segments(directory)?;
        if candidates.is_empty() {
            let file_name = SegmentMetadata::filename(0);
            let path = directory.join(&file_name);
            let (log, next_offset) = Log::open_active(&path, 0, limits)?;
            let metadata = SegmentMetadata::new(0, &path, 0);

            return Ok(Self {
                directory: directory.to_path_buf(),
                limits,
                config,
                active_segment: ActiveSegment { log, metadata },
                closed_segments: Vec::new(),
                next_offset,
                append_disabled: false,
            });
        }

        let active_candidate = candidates
            .pop()
            .expect("There should be at least one candidate");
        let mut expected_next_offset = 0;
        let mut closed_segments = Vec::new();

        for closed_candidate in candidates {
            if closed_candidate.base_offset != expected_next_offset {
                return Err(StorageError::UnexpectedSegmentBaseOffset {
                    path: closed_candidate.path.clone(),
                    expected: expected_next_offset,
                    actual: closed_candidate.base_offset,
                });
            }

            let (log, next_offset) =
                Log::open_closed(&closed_candidate.path, closed_candidate.base_offset, limits)?;

            if log.is_empty()? {
                return Err(StorageError::EmptyClosedSegment {
                    path: closed_candidate.path.clone(),
                    base_offset: closed_candidate.base_offset,
                });
            }

            closed_segments.push(ClosedSegment {
                metadata: SegmentMetadata::new(
                    closed_candidate.base_offset,
                    &closed_candidate.path,
                    log.len()?,
                ),
                log,
            });

            expected_next_offset = next_offset;
        }

        if active_candidate.base_offset != expected_next_offset {
            return Err(StorageError::UnexpectedSegmentBaseOffset {
                path: active_candidate.path.clone(),
                expected: expected_next_offset,
                actual: active_candidate.base_offset,
            });
        }

        let (log, next_offset) =
            Log::open_active(&active_candidate.path, active_candidate.base_offset, limits)?;
        let metadata = SegmentMetadata::new(
            active_candidate.base_offset,
            &active_candidate.path,
            log.len()?,
        );

        Ok(Self {
            directory: directory.to_path_buf(),
            limits,
            config,
            active_segment: ActiveSegment { log, metadata },
            closed_segments,
            next_offset,
            append_disabled: false,
        })
    }

    fn get_segment_scanner(&self, index: usize) -> Result<Option<LogScanner<'_>>, StorageError> {
        if index < self.closed_segments.len() {
            let segment = &self.closed_segments[index];
            let scanner = segment.scan()?;
            return Ok(Some(scanner));
        }
        if index == self.closed_segments.len() {
            let segment = &self.active_segment;
            let scanner = segment.scan()?;
            return Ok(Some(scanner));
        }
        Ok(None)
    }

    fn rotate(&mut self) -> Result<(), StorageError> {
        let path = &self
            .directory
            .join(SegmentMetadata::filename(self.next_offset));

        let new_active_log = Log::create_active(path, self.limits)?;
        let new_active_metadata = SegmentMetadata::new(self.next_offset, path, 0);
        let new_active_segment = ActiveSegment {
            log: new_active_log,
            metadata: new_active_metadata,
        };

        let old_active_segment = std::mem::replace(&mut self.active_segment, new_active_segment);
        let closed_segment = ClosedSegment {
            metadata: old_active_segment.metadata,
            log: old_active_segment.log,
        };

        self.closed_segments.push(closed_segment);

        Ok(())
    }

    pub fn append(&mut self, record: &Record) -> Result<(), StorageError> {
        if self.append_disabled {
            return Err(StorageError::AppendDisabled);
        }

        if self.next_offset != record.offset() {
            return Err(StorageError::UnexpectedOffset {
                expected: self.next_offset,
                actual: record.offset(),
            });
        }

        let following_offset = match self.next_offset.checked_add(1) {
            Some(offset) => offset,
            None => return Err(StorageError::OffsetOverflow),
        };

        self.active_segment.append(record)?;
        self.next_offset = following_offset;

        if self.active_segment.metadata.file_len() >= self.config.max_segment_bytes() {
            match self.rotate() {
                Ok(_) => {}
                Err(e) => {
                    self.append_disabled = true;
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub fn scan(&self) -> Result<SegmentedLogScanner<'_>, StorageError> {
        let scanner = SegmentedLogScanner {
            segment_index: 0,
            current_scanner: self.get_segment_scanner(0)?.take(),
            finished: false,
            log: self,
        };
        Ok(scanner)
    }

    pub fn read(&self, offset: u64) -> Result<Option<Record>, StorageError> {
        let index = if offset >= self.active_segment.metadata.base_offset() {
            self.closed_segments.len()
        } else {
            match self
                .closed_segments
                .iter()
                .rposition(|segment| segment.metadata.base_offset() <= offset)
            {
                Some(idx) => idx,
                None => return Ok(None),
            }
        };

        let scanner = self
            .get_segment_scanner(index)?
            .expect("selected segment index must exist");

        for result in scanner {
            let rec = result?;
            if rec.offset() == offset {
                return Ok(Some(rec));
            }
            if rec.offset() > offset {
                return Ok(None);
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions, read},
        io::Write,
        num::NonZeroU64,
        path::Path,
        vec,
    };

    use crate::{
        error::{CodecError, ConfigurationError, StorageError},
        storage::{
            log::Log,
            record::{Record, RecordLimits},
            segment::{ActiveSegment, ClosedSegment, SegmentConfig, SegmentMetadata, SegmentedLog},
        },
    };

    #[test]
    fn default_segment_config_uses_sixty_four_mibibytes() {
        let config = SegmentConfig::default();
        let expected_byte: u64 = 64 * 1024 * 1024;
        let actual_byte = config.max_segment_bytes();
        assert_eq!(
            actual_byte, expected_byte,
            "Default segment config should use 64 MiB"
        );
    }

    #[test]
    fn segment_config_preserves_custom_max_segment_bytes() {
        let max_segment_bytes = 1024; // Small nonzero value suitable for rotation tests
        let config = SegmentConfig::new(max_segment_bytes).expect("Failed to create SegmentConfig");
        let actual_byte = config.max_segment_bytes();
        assert_eq!(
            actual_byte, max_segment_bytes,
            "Segment config should preserve custom max segment bytes"
        );
    }

    #[test]
    fn segment_config_rejects_zero_max_segment_bytes() {
        let error = SegmentConfig::new(0)
            .expect_err("constructing SegmentConfig with zero max_segment_bytes should fail");
        assert_eq!(
            error,
            ConfigurationError::InvalidSegmentBytes { value: 0 },
            "Segment config should reject zero max segment bytes"
        );
    }

    #[test]
    fn segment_metadata_constructor_preserves_all_fields() {
        let base_offset = 42;
        let file_len = 1024;
        let path = Path::new("segment.log");
        let metadata = SegmentMetadata::new(base_offset, path, file_len);
        assert_eq!(
            metadata.base_offset(),
            base_offset,
            "SegmentMetadata should preserve the base offset"
        );
        assert_eq!(
            metadata.path.as_path(),
            path,
            "SegmentMetadata should preserve the path"
        );
        assert_eq!(
            metadata.file_len(),
            file_len,
            "SegmentMetadata should preserve the byte length"
        );
    }

    #[test]
    fn closed_segment_exposes_supplied_metadata() {
        let base_offset = 42;
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let path = dir_path.join("00000000000000000042.log");
        let (mut log, _) = Log::open_active(&path, base_offset, RecordLimits::default())
            .expect("opening a missing log path should succeed");
        let record = Record::new(
            base_offset,
            1_700_000_000,
            Some(vec![10, 20, 30]),
            vec![1, 2, 3],
        );
        log.append(&record)
            .expect("appending a record should succeed");

        drop(log);

        let file_len = path.metadata().expect("should get metadata").len();

        let (log, _) = Log::open_closed(&path, base_offset, RecordLimits::default())
            .expect("reopening the log should succeed");
        let metadata = SegmentMetadata::new(base_offset, &path, file_len);
        let closed_segment = ClosedSegment { metadata, log };

        assert_eq!(
            closed_segment.metadata.base_offset(),
            base_offset,
            "ClosedSegment should expose the correct base offset"
        );
        assert_eq!(
            closed_segment.metadata.path.as_path(),
            path,
            "ClosedSegment should expose the correct path"
        );
        assert_eq!(
            closed_segment.metadata.file_len(),
            file_len,
            "ClosedSegment should expose the correct file length"
        );
    }

    #[test]
    fn active_segment_exposes_supplied_metadata() {
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let path = dir_path.join("00000000000000000000.log");
        let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
            .expect("opening a missing log path should succeed");
        let file_len = read(&path).map(|b| b.len()).unwrap_or(0);
        let file_length = u64::try_from(file_len).expect("byte length should fit in u64");
        let base_offset = 0;
        let metadata = SegmentMetadata::new(base_offset, &path, file_length);
        let active_segment = ActiveSegment { metadata, log };
        let exposed_metadata = active_segment.metadata;
        assert_eq!(
            exposed_metadata.base_offset(),
            base_offset,
            "ActiveSegment should expose the correct base offset"
        );
        assert_eq!(
            exposed_metadata.path.as_path(),
            path,
            "ActiveSegment should expose the correct path"
        );
        assert_eq!(
            exposed_metadata.file_len(),
            file_length,
            "ActiveSegment should expose the correct byte length"
        );
    }

    #[test]
    fn active_segment_append_updates_length_and_preserves_records() {
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        fs::File::create(&path).expect("creating the log file should succeed");
        let metadata = SegmentMetadata::new(0, &path, 0);
        let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
            .expect("opening a missing log path should succeed");
        let mut active_segment = ActiveSegment { metadata, log };
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record2 = Record::new(1, 1_700_000_001, None, vec![4, 5, 6]);
        let record1_encoded_len = u64::try_from(
            record1
                .encoded_len()
                .expect("record1 encoded length should be retrievable"),
        )
        .expect("record1 encoded length should fit in u64");
        let record2_encoded_len = u64::try_from(
            record2
                .encoded_len()
                .expect("record2 encoded length should be retrievable"),
        )
        .expect("record2 encoded length should fit in u64");

        active_segment
            .append(&record1)
            .expect("appending record1 should succeed");

        assert_eq!(
            active_segment.metadata.file_len(),
            record1_encoded_len,
            "ActiveSegment byte length should be updated after appending record1"
        );

        active_segment
            .append(&record2)
            .expect("appending record2 should succeed");

        assert_eq!(
            active_segment.metadata.file_len(),
            record1_encoded_len + record2_encoded_len,
            "ActiveSegment byte length should be updated after appending record2"
        );
        assert_eq!(
            fs::metadata(&path)
                .expect("getting file metadata should succeed")
                .len(),
            record1_encoded_len + record2_encoded_len,
            "Physical file length should match the accumulated encoded lengths after appending record2"
        );

        let mut scanner = active_segment
            .scan()
            .expect("creating scanner should succeed");
        let scanned_record1 = scanner
            .read_next_record()
            .expect("expected record1 to be present")
            .expect("expected record1 to be valid");
        let scanned_record2 = scanner
            .read_next_record()
            .expect("expected record2 to be present")
            .expect("expected record2 to be valid");
        let next_scan_result = scanner
            .read_next_record()
            .expect("expected no more records to be present");

        assert_eq!(
            scanned_record1, record1,
            "Scanned record1 should match the appended record1"
        );
        assert_eq!(
            scanned_record2, record2,
            "Scanned record2 should match the appended record2"
        );
        assert!(
            next_scan_result.is_none(),
            "No more records should be present after scanning all appended records"
        );
    }

    #[test]
    fn active_segment_append_rejection_preserves_state_and_allows_retry() {
        let limits = RecordLimits::new(3, 3);
        let dir = tempfile::tempdir().expect("creating temp dir should succeed");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        fs::File::create(&path).expect("creating the log file should succeed");
        let (log, _) =
            Log::open_active(&path, 0, limits).expect("opening the active log should succeed");
        let metadata = SegmentMetadata::new(0, &path, 0);
        let mut active_segment = ActiveSegment { metadata, log };
        let initial_file_bytes =
            fs::read(&path).expect("reading initial file bytes should succeed");
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3, 4]);

        let error = active_segment
            .append(&record1)
            .expect_err("appending a record exceeding the payload limit should fail");
        assert!(
            matches!(error, StorageError::Codec(CodecError::PayloadTooLarge)),
            "Expected a PayloadTooLarge error"
        );

        let final_file_bytes = fs::read(&path).expect("reading final file bytes should succeed");
        assert_eq!(
            initial_file_bytes, final_file_bytes,
            "File bytes should remain unchanged after a rejected append"
        );
        assert_eq!(
            active_segment.metadata.file_len(),
            0,
            "Metadata byte length should remain unchanged after a rejected append"
        );

        let record2 = Record::new(0, 1_700_000_000, None, vec![5, 6, 7]);
        active_segment
            .append(&record2)
            .expect("appending a valid record should succeed");

        let record2_encoded = record2
            .encode(&limits)
            .expect("encoding record2 should succeed");
        let record2_encoded_len = u64::try_from(record2_encoded.len())
            .expect("converting encoded length to u64 should succeed");

        let final_file_bytes = fs::read(&path).expect("reading final file bytes should succeed");
        assert_eq!(
            final_file_bytes, record2_encoded,
            "File bytes should reflect the successfully appended record"
        );
        assert_eq!(
            active_segment.metadata.file_len(),
            record2_encoded_len,
            "Metadata byte length should reflect the successfully appended record"
        );
    }

    #[test]
    fn active_segment_append_length_overflow_does_not_write() {
        let dir = tempfile::tempdir().expect("creating temporary directory should succeed");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        let (log, _) = Log::open_active(&path, 0, RecordLimits::default())
            .expect("opening active log should succeed");
        let metadata = SegmentMetadata::new(0, &path, u64::MAX);
        let mut active_segment = ActiveSegment { metadata, log };
        let record = Record::new(0, 1_700_000_000, None, vec![5, 6, 7]);
        let error = active_segment
            .append(&record)
            .expect_err("appending a record that would overflow should fail");
        assert!(
            matches!(error, StorageError::Codec(CodecError::LengthOverflow)),
            "Expected a LengthOverflow error"
        );
        assert_eq!(
            active_segment.metadata.file_len(),
            u64::MAX,
            "Metadata byte length should remain u64::MAX after a failed append"
        );
        assert_eq!(
            fs::read(&path).expect("reading final file bytes should succeed"),
            vec![],
            "File should remain empty after a failed append due to length overflow"
        );
    }

    #[test]
    fn segment_filename_formats_canonical_boundary_offsets() {
        let filename = SegmentMetadata::filename(0);
        assert_eq!(
            filename, "00000000000000000000.log",
            "Filename for offset 0 should be canonical"
        );

        let filename = SegmentMetadata::filename(42);
        assert_eq!(
            filename, "00000000000000000042.log",
            "Filename for offset 42 should be canonical"
        );

        let filename = SegmentMetadata::filename(u64::MAX);
        assert_eq!(
            filename, "18446744073709551615.log",
            "Filename for offset u64::MAX should be canonical"
        );
    }

    #[test]
    fn segment_filename_parser_accepts_canonical_boundary_offsets() {
        let path = Path::new("00000000000000000000.log");
        let offset =
            SegmentMetadata::parse_base_offset(path).expect("Parsing base offset should succeed");
        assert_eq!(
            offset, 0,
            "Parsed offset should match the value encoded in the filename"
        );

        let path = Path::new("00000000000000000042.log");
        let offset =
            SegmentMetadata::parse_base_offset(path).expect("Parsing base offset should succeed");
        assert_eq!(
            offset, 42,
            "Parsed offset should match the value encoded in the filename"
        );

        let path = Path::new("18446744073709551615.log");
        let offset =
            SegmentMetadata::parse_base_offset(path).expect("Parsing base offset should succeed");
        assert_eq!(
            offset,
            u64::MAX,
            "Parsed offset should match the value encoded in the filename"
        );

        let path = Path::new("parent_dir/00000000000000000042.log");
        let offset =
            SegmentMetadata::parse_base_offset(path).expect("Parsing base offset should succeed");
        assert_eq!(
            offset, 42,
            "Parsed offset should match the value encoded in the filename"
        );
    }

    #[test]
    fn segment_filename_format_and_parse_round_trip() {
        let offsets = [0, 1, 42, 123456789, u64::MAX];
        for &offset in &offsets {
            let filename = SegmentMetadata::filename(offset);
            let path = Path::new(&filename);
            let parsed_offset = SegmentMetadata::parse_base_offset(path)
                .expect("Parsing base offset should succeed");
            assert_eq!(
                parsed_offset, offset,
                "Parsed offset should match the original offset {}",
                offset
            );
        }
    }

    #[test]
    fn segment_filename_parser_rejects_noncanonical_names_and_preserves_path() {
        let invalid_filenames = [
            "42.log",
            "00000000000000000042",
            "00000000000000000042.txt",
            "00000000000000000042.log.log",
            "+0000000000000000042.log",
            "0000000000000000004x.log",
        ];

        for &filename in &invalid_filenames {
            let path = Path::new("parent_dir").join(filename);
            match SegmentMetadata::parse_base_offset(&path) {
                Ok(_) => panic!(
                    "Parsing should have failed for invalid filename: {}",
                    filename
                ),
                Err(StorageError::InvalidSegmentFilename { path: err_path }) => {
                    assert_eq!(
                        err_path, path,
                        "Error should retain the complete supplied path for filename: {}",
                        filename
                    );
                }
                Err(err) => panic!("Unexpected error type for filename {}: {:?}", filename, err),
            }
        }
    }

    #[test]
    fn segment_filename_parser_rejects_path_without_filename() {
        let path = Path::new("/");
        match SegmentMetadata::parse_base_offset(path) {
            Ok(_) => panic!(
                "Parsing should have failed for path without filename: {:?}",
                path
            ),
            Err(StorageError::InvalidSegmentFilename { path: err_path }) => {
                assert_eq!(
                    err_path, path,
                    "Error should retain the complete supplied path for path without filename: {:?}",
                    path
                );
            }
            Err(err) => panic!(
                "Unexpected error type for path without filename {:?}: {:?}",
                path, err
            ),
        }
    }

    #[cfg(unix)]
    #[test]
    fn segment_filename_parser_rejects_non_utf8_filename() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let invalid_bytes = b"00000000000000000042\xFF.log";
        let os_string = OsString::from_vec(invalid_bytes.to_vec());
        let path = Path::new("parent_dir").join(&os_string);

        match SegmentMetadata::parse_base_offset(&path) {
            Ok(_) => panic!(
                "Parsing should have failed for non-UTF-8 filename: {:?}",
                path
            ),
            Err(StorageError::InvalidSegmentFilename { path: err_path }) => {
                assert_eq!(
                    err_path, path,
                    "Error should retain the complete supplied path for non-UTF-8 filename: {:?}",
                    path
                );
            }
            Err(err) => panic!(
                "Unexpected error type for non-UTF-8 filename {:?}: {:?}",
                path, err
            ),
        }
    }

    #[test]
    fn segmented_log_append_advances_offset_and_updates_active_length() {
        let dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        let mut segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("Failed to open segmented log");
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record2 = Record::new(1, 1_700_000_001, None, vec![4, 5, 6]);
        let record1_len = u64::try_from(
            record1
                .encoded_len()
                .expect("Failed to get encoded length of record1"),
        )
        .expect("Failed to convert record1 length to u64");
        let record2_len = u64::try_from(
            record2
                .encoded_len()
                .expect("Failed to get encoded length of record2"),
        )
        .expect("Failed to convert record2 length to u64");

        segmented_log
            .append(&record1)
            .expect("Failed to append record1");

        assert_eq!(
            segmented_log.next_offset(),
            1,
            "Next offset should be 1 after appending record1"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            record1_len,
            "Active segment metadata file length should match the length of the appended record1"
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("Failed to get segment file metadata")
                .len(),
            record1_len,
            "Active length should match the length of the appended record1"
        );

        segmented_log
            .append(&record2)
            .expect("Failed to append record2");

        assert_eq!(
            segmented_log.next_offset(),
            2,
            "Next offset should be 2 after appending record2"
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("Failed to get segment file metadata")
                .len(),
            record1_len + record2_len,
            "Active length should match the accumulated length of appended records"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            record1_len + record2_len,
            "Active segment metadata file length should match the accumulated length of appended records"
        );

        let mut scanner = segmented_log.scan().expect("Failed to scan segmented log");
        let scanned_record1 = scanner
            .next()
            .expect("Failed to get first scanned record")
            .expect("Expected a record");
        let scanned_record2 = scanner
            .next()
            .expect("Failed to get second scanned record")
            .expect("Expected a record");
        let next_scan_result = scanner.next();

        assert_eq!(
            scanned_record1, record1,
            "First scanned record should match record1"
        );
        assert_eq!(
            scanned_record2, record2,
            "Second scanned record should match record2"
        );
        assert!(
            next_scan_result.is_none(),
            "Next scan result should be None, indicating EOF"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            record1_len + record2_len,
            "Active segment metadata file length should match the accumulated length of appended records"
        );
    }

    #[test]
    fn segmented_log_append_rejects_unexpected_offsets_without_mutation() {
        let dir = tempfile::tempdir()
            .expect("Failed to create temporary directory for segmented log test");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        let mut segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("Failed to open segmented log");
        let record = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let expected_file_content = record
            .encode(&RecordLimits::default())
            .expect("Failed to encode record");
        let expected_file_len = u64::try_from(expected_file_content.len())
            .expect("Failed to convert expected file content length to u64");

        segmented_log
            .append(&record)
            .expect("Failed to append record");

        assert_eq!(
            std::fs::read(&path).expect("reading the closed segment should succeed"),
            expected_file_content,
            "Segment file content should match the expected encoded record"
        );

        let file_len_snapshot = std::fs::metadata(&path)
            .expect("Failed to get segment file metadata")
            .len();
        let next_offset_snapshot = segmented_log.next_offset();

        assert_eq!(
            file_len_snapshot, expected_file_len,
            "Segment file length should match the expected encoded record length"
        );
        assert_eq!(
            next_offset_snapshot, 1,
            "Next offset should be 1 after appending the first record"
        );

        let offset_zero_record = Record::new(0, 1_700_000_000, None, vec![4, 5, 6]);

        let error = segmented_log
            .append(&offset_zero_record)
            .expect_err("Appending an unexpected offset should fail");

        assert!(
            matches!(
                error,
                StorageError::UnexpectedOffset {
                    expected: 1,
                    actual: 0
                }
            ),
            "Error should be UnexpectedOffset with expected 1 and actual 0"
        );
        assert_eq!(
            std::fs::read(&path).expect("reading the closed segment should succeed"),
            expected_file_content,
            "Segment file content should match the expected encoded record"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len, expected_file_len,
            "Active segment metadata byte length should match the expected encoded record length"
        );
        assert_eq!(
            segmented_log.next_offset(),
            1,
            "Next offset should remain 1 after failed append"
        );

        let offset_two_record = Record::new(2, 1_700_000_000, None, vec![7, 8, 9]);

        let error = segmented_log
            .append(&offset_two_record)
            .expect_err("Appending an unexpected offset should fail");

        assert!(
            matches!(
                error,
                StorageError::UnexpectedOffset {
                    expected: 1,
                    actual: 2
                }
            ),
            "Error should be UnexpectedOffset with expected 1 and actual 2"
        );
        assert_eq!(
            std::fs::read(&path).expect("reading the closed segment should succeed"),
            expected_file_content,
            "Segment file content should match the expected encoded record"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len, expected_file_len,
            "Active segment metadata byte length should match the expected encoded record length"
        );
        assert_eq!(
            segmented_log.next_offset(),
            1,
            "Next offset should remain 1 after failed append"
        );

        let offset_one_record = Record::new(1, 1_700_000_000, None, vec![10, 11, 12]);
        segmented_log
            .append(&offset_one_record)
            .expect("Appending offset 1 should succeed");

        let offset_one_record_bytes = offset_one_record
            .encode(&RecordLimits::default())
            .expect("encoding offset one record should succeed");
        let offset_one_record_len = u64::try_from(offset_one_record_bytes.len())
            .expect("converting offset one record length to u64 should succeed");

        assert_eq!(
            std::fs::read(&path).expect("reading the closed segment should succeed"),
            [
                expected_file_content.as_slice(),
                offset_one_record_bytes.as_slice()
            ]
            .concat(),
            "Segment file content should match the expected encoded record"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len,
            expected_file_len + offset_one_record_len,
            "Active segment metadata byte length should match the expected encoded record length"
        );
        assert_eq!(
            segmented_log.next_offset(),
            2,
            "Next offset should be 2 after successful append of offset 1"
        );
    }

    #[test]
    fn segmented_log_append_payload_rejection_preserves_next_offset_and_allows_retry() {
        let limits = RecordLimits::new(2, 3);
        let dir = tempfile::tempdir().expect("creating temp dir should succeed");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(0));
        let mut segmented_log = SegmentedLog::open(dir_path, limits, SegmentConfig::default())
            .expect("opening segmented log should succeed");
        let record0 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record1 = Record::new(1, 1_700_000_001, None, vec![4, 5, 6, 7]);
        let record2 = Record::new(1, 1_700_000_002, None, vec![8, 9, 10]);
        let expected_file_data = record0
            .encode(&limits)
            .expect("encoding expected file data should succeed");
        let expected_file_data_len = u64::try_from(expected_file_data.len())
            .expect("converting expected file data length to u64 should succeed");
        let record2_file_data = record2
            .encode(&limits)
            .expect("encoding record 2 should succeed");
        let record2_file_data_len = u64::try_from(record2_file_data.len())
            .expect("converting record 2 file data length to u64 should succeed");

        segmented_log
            .append(&record0)
            .expect("Appending offset 0 should succeed");

        let file_data = std::fs::read(&path).expect("reading the segment file should succeed");
        let error = segmented_log
            .append(&record1)
            .expect_err("Appending oversized payload should fail");

        assert_eq!(
            std::fs::read(&path).expect("reading the closed segment should succeed"),
            expected_file_data,
            "Segment file content should match the expected encoded record"
        );
        assert!(matches!(
            error,
            StorageError::Codec(CodecError::PayloadTooLarge)
        ));
        assert_eq!(
            segmented_log.next_offset(),
            1,
            "Next offset should remain 1 after failed append of offset 1"
        );
        assert_eq!(
            file_data, expected_file_data,
            "Segment file should remain unchanged after failed append"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            expected_file_data_len,
            "Active metadata length should remain unchanged after failed append"
        );
        assert_eq!(
            fs::metadata(&path)
                .expect("getting file metadata should succeed")
                .len(),
            expected_file_data_len,
            "Physical file length should remain unchanged after failed append"
        );

        segmented_log
            .append(&record2)
            .expect("Appending offset 1 should succeed");

        let file_data_after_record2 =
            std::fs::read(&path).expect("reading the segment file should succeed");
        assert_eq!(
            segmented_log.next_offset(),
            2,
            "Next offset should be 2 after appending record 2"
        );
        assert_eq!(
            file_data_after_record2,
            [expected_file_data, record2_file_data].concat(),
            "Segment file should contain both the initial and the second record after successful append"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            expected_file_data_len + record2_file_data_len,
            "Active metadata length should reflect both the initial and the second record after successful append"
        );
        assert_eq!(
            fs::metadata(&path)
                .expect("getting file metadata should succeed")
                .len(),
            expected_file_data_len + record2_file_data_len,
            "Physical file length should reflect both the initial and the second record after successful append"
        );

        let mut scanner = segmented_log
            .scan()
            .expect("scanning the segmented log should succeed");
        let scanned_record0 = scanner
            .next()
            .expect("scanning should return the first record")
            .expect("scanned record 0 should be present");
        let scanned_record2 = scanner
            .next()
            .expect("scanning should return the second record")
            .expect("scanned record 2 should be present");

        assert_eq!(scanned_record0, record0);
        assert_eq!(scanned_record2, record2);
    }

    #[test]
    fn segmented_log_append_offset_overflow_does_not_write() {
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let record = Record::new(
            u64::MAX,
            1_600_000_000,
            Some(vec![10, 20, 30, 40, 50]),
            vec![1, 2, 3, 5, 6, 7],
        );
        let mut segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("opening a fresh segmented log should succeed");
        segmented_log.next_offset = u64::MAX;

        let error = segmented_log
            .append(&record)
            .expect_err("appending a record with offset u64::MAX should fail");

        assert!(
            matches!(error, StorageError::OffsetOverflow),
            "appending a record with offset u64::MAX should return an offset overflow error"
        );

        assert_eq!(
            segmented_log.next_offset,
            u64::MAX,
            "next_offset should remain u64::MAX after failed append"
        );

        let active_metadata_length = segmented_log.active_segment.metadata.file_len();
        assert_eq!(
            active_metadata_length, 0,
            "active metadata length should remain zero after failed append"
        );

        let segment_files = std::fs::read_dir(dir_path)
            .expect("reading the directory should succeed")
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(
            segment_files.len(),
            1,
            "the directory should contain only the initial zero-based segment"
        );

        let path = dir_path.join(SegmentMetadata::filename(0));
        let file_len = fs::metadata(&path)
            .expect("reading file metadata should succeed")
            .len();
        assert_eq!(
            file_len, 0,
            "file length should remain zero after failed append"
        );
    }

    #[test]
    fn segmented_log_append_restart_restores_records_and_continues_offsets() {
        let record1 = Record::new(
            0,
            1_600_000_000,
            Some(vec![10, 20, 30, 40, 50]),
            vec![1, 2, 3, 5, 6, 7],
        );
        let record2 = Record::new(
            1,
            1_600_000_001,
            Some(vec![11, 21, 31, 41, 51]),
            vec![2, 3, 4, 6, 7, 8],
        );
        let record3 = Record::new(
            2,
            1_600_000_002,
            Some(vec![12, 22, 32, 42, 52]),
            vec![3, 4, 5, 7, 8, 9],
        );
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let mut segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("opening segmented log should succeed");

        segmented_log
            .append(&record1)
            .expect("appending record1 should succeed");
        segmented_log
            .append(&record2)
            .expect("appending record2 should succeed");

        drop(segmented_log);

        let mut segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("reopening segmented log should succeed");
        let scanned_records: Vec<_> = segmented_log
            .scan()
            .expect("scanning segmented log should succeed")
            .map(|result| result.expect("scanned record should be Ok"))
            .collect();

        assert_eq!(
            segmented_log.next_offset(),
            2,
            "next_offset should be 2 after reopening"
        );
        assert_eq!(
            scanned_records.len(),
            2,
            "there should be two records after reopening"
        );
        assert_eq!(scanned_records[0], record1, "first record should match");
        assert_eq!(scanned_records[1], record2, "second record should match");

        segmented_log
            .append(&record3)
            .expect("appending record3 should succeed");
        let scanned_records: Vec<_> = segmented_log
            .scan()
            .expect("scanning segmented log should succeed")
            .map(|result| result.expect("scanned record should be Ok"))
            .collect();

        assert_eq!(
            segmented_log.next_offset(),
            3,
            "next_offset should be 3 after appending record3"
        );
        assert_eq!(
            scanned_records.len(),
            3,
            "there should be three records after appending record3"
        );
        assert_eq!(scanned_records[2], record3, "third record should match");
    }

    #[test]
    fn segmented_log_append_rotates_at_threshold_and_preserves_closed_bytes() {
        let config = SegmentConfig {
            max_segment_bytes: NonZeroU64::new(68).expect("max_segment_bytes should be nonzero"),
        };
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let record1 = Record::new(0, 1_700_000_000, None, vec![]);
        let record2 = Record::new(1, 1_700_000_001, None, vec![]);
        let record3 = Record::new(2, 1_700_000_002, None, vec![]);
        let mut segmented_log = SegmentedLog::open(dir_path, RecordLimits::default(), config)
            .expect("failed to open segmented log");

        segmented_log
            .append(&record1)
            .expect("appending record1 should succeed");

        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            34,
            "active segment file length should be 34 bytes after appending record1"
        );

        segmented_log
            .append(&record2)
            .expect("appending record2 should succeed");

        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            0,
            "active segment file length should be 0 bytes after appending record2"
        );

        let closed_segments = &segmented_log.closed_segments;

        assert_eq!(
            closed_segments.len(),
            1,
            "there should be one closed segment after appending record2"
        );
        assert_eq!(
            closed_segments[0].metadata.file_len(),
            68,
            "closed segment file length should be 68 bytes"
        );

        let closed_setment_path = dir_path.join(SegmentMetadata::filename(0));
        assert!(
            closed_setment_path.exists(),
            "closed segment file should exist"
        );

        let closed_segment_data =
            read(&closed_setment_path).expect("failed to read closed segment file");
        let closed_segment_data_snapshot = closed_segment_data.clone();
        assert_eq!(
            closed_segment_data.len(),
            68,
            "closed segment file should have 68 bytes"
        );

        segmented_log
            .append(&record3)
            .expect("appending record3 should succeed");

        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            34,
            "active segment file length should be 34 bytes after appending record3"
        );
        assert_eq!(
            segmented_log.next_offset, 3,
            "next_offset should be 3 after appending record3"
        );

        let closed_segment_data =
            read(&closed_setment_path).expect("failed to read closed segment file");
        assert_eq!(
            closed_segment_data.len(),
            68,
            "closed segment file should still have 68 bytes"
        );
        assert_eq!(
            closed_segment_data, closed_segment_data_snapshot,
            "closed segment data should not change after appending to the active segment"
        );

        let active_segment_path = dir_path.join(SegmentMetadata::filename(2));
        assert!(
            active_segment_path.exists(),
            "active segment file should exist"
        );

        let active_segment_data =
            read(&active_segment_path).expect("failed to read active segment file");
        assert_eq!(
            active_segment_data.len(),
            34,
            "active segment file should have 34 bytes"
        );
        assert_eq!(
            active_segment_data,
            record3
                .encode(&RecordLimits::default())
                .expect("failed to encode record3"),
            "active segment data should match the appended record3"
        );

        let mut scanner = segmented_log
            .scan()
            .expect("failed to create scanner for segmented log");
        let scanned_record1 = scanner
            .next()
            .expect("failed to scan record1")
            .expect("record1 should exist");
        let scanned_record2 = scanner
            .next()
            .expect("failed to scan record2")
            .expect("record2 should exist");
        let scanned_record3 = scanner
            .next()
            .expect("failed to scan record3")
            .expect("record3 should exist");

        assert_eq!(
            scanned_record1, record1,
            "first scanned record should match record1"
        );
        assert_eq!(
            scanned_record2, record2,
            "second scanned record should match record2"
        );
        assert_eq!(
            scanned_record3, record3,
            "third scanned record should match record3"
        );

        let read_record1 = segmented_log
            .read(0)
            .expect("failed to read record1")
            .expect("record1 should exist");
        let read_record2 = segmented_log
            .read(1)
            .expect("failed to read record2")
            .expect("record2 should exist");
        let read_record3 = segmented_log
            .read(2)
            .expect("failed to read record3")
            .expect("record3 should exist");

        assert_eq!(read_record1, record1, "read record1 should match record1");
        assert_eq!(read_record2, record2, "read record2 should match record2");
        assert_eq!(read_record3, record3, "read record3 should match record3");

        let segment_files = fs::read_dir(dir_path).expect("failed to read segment directory");
        assert_eq!(
            segment_files.count(),
            2,
            "there should be two segment files before reading files"
        );
    }

    #[test]
    fn segmented_log_append_oversized_segment_record_rotates_after_writing() {
        let config = SegmentConfig::new(30).expect("failed to create segment config");
        let record = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let encoded_record = record
            .encode(&RecordLimits::default())
            .expect("failed to encode record");
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let mut segmented_log = SegmentedLog::open(dir_path, RecordLimits::default(), config)
            .expect("failed to create segmented log");

        segmented_log
            .append(&record)
            .expect("failed to append oversized record");

        assert_eq!(
            segmented_log.next_offset(),
            1,
            "next_offset should be 1 after appending oversized record"
        );

        let segment_files = fs::read_dir(dir_path).expect("failed to read segment directory");

        assert_eq!(
            segment_files.count(),
            2,
            "there should be two segment files after rotation"
        );

        let closed_path = dir_path.join(SegmentMetadata::filename(0));
        let active_path = dir_path.join(SegmentMetadata::filename(1));

        assert!(closed_path.exists(), "closed segment file should exist");
        assert!(active_path.exists(), "active segment file should exist");

        assert_eq!(
            fs::read(&closed_path).expect("failed to read closed segment file"),
            encoded_record
        );
        assert_eq!(
            fs::metadata(&active_path)
                .expect("failed to read active segment metadata")
                .len(),
            0,
            "active segment file should be empty"
        );

        let mut scanner = segmented_log.scan().expect("failed to create scanner");
        let scanned_record1 = scanner
            .next()
            .expect("failed to scan first record")
            .expect("first scanned record should exist");
        let next_scan_result = scanner.next();

        assert_eq!(
            scanned_record1, record,
            "scanned record should match the appended record"
        );
        assert!(
            next_scan_result.is_none(),
            "there should be no more records to scan"
        );

        let read_record1 = segmented_log
            .read(0)
            .expect("failed to read record at offset 0")
            .expect("record at offset 0 should exist");
        let read_record_none = segmented_log
            .read(1)
            .expect("failed to read record at offset 1");
        assert_eq!(
            read_record1, record,
            "read record should match the appended record"
        );
        assert!(
            read_record_none.is_none(),
            "record at offset 1 should not exist"
        );
    }

    #[test]
    fn segmented_log_restart_after_rotation_resumes_empty_active_segment() {
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let config =
            SegmentConfig::new(68).expect("failed to create segment config with 68-byte threshold");
        let record1 = Record::new(0, 1_700_000_000, None, vec![]);
        let record2 = Record::new(1, 1_700_000_000, None, vec![]);
        let record3 = Record::new(2, 1_700_000_000, None, vec![]);
        let record4 = Record::new(3, 1_700_000_000, None, vec![]);
        let record5 = Record::new(4, 1_700_000_000, None, vec![]);
        let mut segmented_log = SegmentedLog::open(dir_path, RecordLimits::default(), config)
            .expect("failed to open segmented log");

        segmented_log
            .append(&record1)
            .expect("failed to append record1");
        assert_eq!(segmented_log.next_offset, 1);
        segmented_log
            .append(&record2)
            .expect("failed to append record2");
        assert_eq!(segmented_log.next_offset, 2);
        segmented_log
            .append(&record3)
            .expect("failed to append record3");
        assert_eq!(segmented_log.next_offset, 3);
        segmented_log
            .append(&record4)
            .expect("failed to append record4");
        assert_eq!(segmented_log.next_offset, 4);

        let closed_segments = &segmented_log.closed_segments;
        assert_eq!(closed_segments.len(), 2);
        assert_eq!(closed_segments[0].metadata.base_offset(), 0);
        assert_eq!(closed_segments[1].metadata.base_offset(), 2);
        assert_eq!(segmented_log.active_segment.metadata.base_offset(), 4);

        let closed_path1 = dir_path.join(SegmentMetadata::filename(0));
        let closed_path2 = dir_path.join(SegmentMetadata::filename(2));
        let active_path = dir_path.join(SegmentMetadata::filename(4));

        let closed_segment1_snapshot =
            fs::read(&closed_path1).expect("failed to read closed segment 1");
        let closed_segment2_snapshot =
            fs::read(&closed_path2).expect("failed to read closed segment 2");

        drop(segmented_log);

        let mut segmented_log = SegmentedLog::open(dir_path, RecordLimits::default(), config)
            .expect("failed to reopen segmented log");

        assert_eq!(segmented_log.next_offset, 4);
        assert_eq!(segmented_log.active_segment.metadata.file_len(), 0);
        assert_eq!(segmented_log.active_segment.metadata.base_offset(), 4);

        let segment_files = fs::read_dir(dir_path).expect("failed to read segment directory");

        assert_eq!(
            segment_files.count(),
            3,
            "there should be three segment files after rotation"
        );

        assert!(closed_path1.exists(), "closed segment file 1 should exist");
        assert!(closed_path2.exists(), "closed segment file 2 should exist");
        assert!(active_path.exists(), "active segment file should exist");

        let scanner = segmented_log.scan().expect("failed to create scanner");

        let records: Vec<_> = scanner
            .map(|result| result.expect("failed to read record"))
            .collect();

        assert_eq!(
            records.len(),
            4,
            "there should be four records after rotation"
        );
        assert_eq!(records[0], record1, "record 1 should match");
        assert_eq!(records[1], record2, "record 2 should match");
        assert_eq!(records[2], record3, "record 3 should match");
        assert_eq!(records[3], record4, "record 4 should match");

        let read_record1 = segmented_log
            .read(0)
            .expect("failed to read record 1")
            .expect("record 1 should exist");
        let read_record2 = segmented_log
            .read(1)
            .expect("failed to read record 2")
            .expect("record 2 should exist");
        let read_record3 = segmented_log
            .read(2)
            .expect("failed to read record 3")
            .expect("record 3 should exist");
        let read_record4 = segmented_log
            .read(3)
            .expect("failed to read record 4")
            .expect("record 4 should exist");
        let read_record_none = segmented_log.read(4).expect("failed to read record 4");

        assert_eq!(read_record1, record1, "read record 1 should match");
        assert_eq!(read_record2, record2, "read record 2 should match");
        assert_eq!(read_record3, record3, "read record 3 should match");
        assert_eq!(read_record4, record4, "read record 4 should match");
        assert!(read_record_none.is_none(), "read record 4 should not exist");

        segmented_log
            .append(&record5)
            .expect("failed to append record 5");

        assert_eq!(
            fs::metadata(active_path)
                .expect("should get metadata")
                .len(),
            34,
            "active segment file should have 34 bytes"
        );
        assert_eq!(segmented_log.next_offset(), 5);

        assert_eq!(
            fs::read(&closed_path1).expect("should read closed segment file"),
            closed_segment1_snapshot,
            "closed segment 1 should match the snapshot"
        );
        assert_eq!(
            fs::read(&closed_path2).expect("should read closed segment file"),
            closed_segment2_snapshot,
            "closed segment 2 should match the snapshot"
        );

        drop(segmented_log);

        let segmented_log = SegmentedLog::open(dir_path, RecordLimits::default(), config)
            .expect("failed to reopen segmented log");

        let scanned_records: Vec<_> = segmented_log
            .scan()
            .expect("scanning segmented log should succeed")
            .map(|result| result.expect("scanned record should be Ok"))
            .collect();

        assert_eq!(
            scanned_records,
            vec![record1, record2, record3, record4, record5],
            "scanned records should match the appended records"
        );
    }

    #[test]
    fn segmented_log_exposes_supplied_state_without_mutable_access() {
        let base_offset = 0;
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let dir_path = dir.path();
        let closed_path = dir_path.join(SegmentMetadata::filename(base_offset));
        let active_path = dir_path.join(SegmentMetadata::filename(base_offset + 1));
        let record = Record::new(
            base_offset,
            1_700_000_000,
            Some(vec![10, 20, 30]),
            vec![1, 2, 3],
        );
        let limits = RecordLimits::new(512 * 512, 512 * 512 * 64);
        let (mut log, _) = Log::open_active(&closed_path, base_offset, limits)
            .expect("active log should be created");
        let config = SegmentConfig::default();
        let expected_max_segment_bytes = config.max_segment_bytes();
        log.append(&record)
            .expect("record should be appended successfully");

        drop(log);

        let closed_file_len = closed_path.metadata().expect("should get metadata").len();
        let (closed_log, _) = Log::open_closed(&closed_path, base_offset, limits)
            .expect("closed log should be opened successfully");
        let (active_log, _) = Log::open_active(&active_path, base_offset + 1, limits)
            .expect("active log should be opened successfully");
        let closed_metadata = SegmentMetadata::new(base_offset, &closed_path, closed_file_len);
        let active_metadata = SegmentMetadata::new(base_offset + 1, &active_path, 0);
        let closed_segment = ClosedSegment {
            metadata: closed_metadata,
            log: closed_log,
        };
        let active_segment = ActiveSegment {
            metadata: active_metadata,
            log: active_log,
        };
        let segmented_log = SegmentedLog {
            directory: dir_path.to_path_buf(),
            closed_segments: vec![closed_segment],
            active_segment,
            config,
            next_offset: base_offset + 1,
            limits,
            append_disabled: false,
        };

        assert_eq!(
            segmented_log.directory.as_path(),
            dir_path,
            "segmented log should preserve its partition directory"
        );
        assert_eq!(
            segmented_log.closed_segments.len(),
            1,
            "segmented log should contain one closed segment"
        );

        let exposed_closed = &segmented_log.closed_segments[0];

        assert_eq!(exposed_closed.metadata.base_offset(), 0);
        assert_eq!(exposed_closed.metadata.path.as_path(), closed_path);
        assert_eq!(exposed_closed.metadata.file_len(), closed_file_len);

        let exposed_active = &segmented_log.active_segment;

        assert_eq!(exposed_active.metadata.base_offset(), 1);
        assert_eq!(exposed_active.metadata.path.as_path(), active_path);
        assert_eq!(exposed_active.metadata.file_len(), 0);

        assert_eq!(
            segmented_log.next_offset(),
            1,
            "next offset should equal the empty active segment's base"
        );

        assert_eq!(
            segmented_log.limits(),
            limits,
            "segmented log should preserve its record limits"
        );

        assert_eq!(
            segmented_log.config().max_segment_bytes(),
            expected_max_segment_bytes,
            "segmented log should preserve its segment configuration"
        );
    }

    #[test]
    fn segmented_log_open_creates_missing_directory_and_initial_active_segment() {
        let base_offset = 0;
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let path = dir_path.join(SegmentMetadata::filename(base_offset));

        assert!(
            !path.exists(),
            "the segment path should not exist before opening the segmented log"
        );

        let segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("failed to open segmented log");

        assert!(
            dir_path.exists(),
            "the segment path should exist after opening the segmented log"
        );
        assert!(
            segmented_log.closed_segments.is_empty(),
            "there should be no closed segments initially"
        );

        let active_segment = &segmented_log.active_segment;
        assert_eq!(active_segment.metadata.base_offset(), 0);
        assert_eq!(active_segment.metadata.path.as_path(), path);
        assert_eq!(active_segment.metadata.file_len(), 0);

        assert_eq!(segmented_log.next_offset(), 0);
        assert_eq!(segmented_log.limits(), RecordLimits::default());
        assert_eq!(segmented_log.config(), &SegmentConfig::default());
    }

    #[test]
    fn segment_discovery_sorts_candidates_by_numeric_base_offset() {
        let base_offset_100 = 100;
        let base_offset_2 = 2;
        let base_offset_42 = 42;
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let path_100 = dir_path.join(SegmentMetadata::filename(base_offset_100));
        let path_2 = dir_path.join(SegmentMetadata::filename(base_offset_2));
        let path_42 = dir_path.join(SegmentMetadata::filename(base_offset_42));

        std::fs::File::create(&path_100).expect("failed to create segment file for offset 100");
        std::fs::File::create(&path_2).expect("failed to create segment file for offset 2");
        std::fs::File::create(&path_42).expect("failed to create segment file for offset 42");

        let candidates =
            SegmentedLog::discover_segments(dir_path).expect("failed to discover segments");

        assert_eq!(candidates[0].base_offset, base_offset_2);
        assert_eq!(candidates[1].base_offset, base_offset_42);
        assert_eq!(candidates[2].base_offset, base_offset_100);

        for candidate in &candidates {
            assert!(candidate.path.exists(), "the candidate path should exist");
            assert_eq!(
                candidate.file_len, 0,
                "the candidate should have byte length 0"
            );
        }
    }

    #[test]
    fn segment_discovery_rejects_invalid_filename_and_preserves_path() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let invalid_file_path = dir_path.join("notes.txt");
        std::fs::File::create(&invalid_file_path).expect("failed to create invalid segment file");

        let error = SegmentedLog::discover_segments(dir_path)
            .expect_err("expected an error due to invalid segment filename");

        assert!(
            matches!(error, StorageError::InvalidSegmentFilename { path } if path == invalid_file_path),
            "expected StorageError::InvalidSegmentFilename with the correct path"
        );
    }

    #[test]
    fn segment_discovery_rejects_subdirectory_and_preserves_path() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let child_dir_path = dir_path.join("child");
        std::fs::create_dir(&child_dir_path).expect("failed to create child directory");

        let error = SegmentedLog::discover_segments(dir_path)
            .expect_err("expected an error due to unexpected segment entry");

        assert!(
            matches!(error, StorageError::UnexpectedSegmentEntry { path } if path == child_dir_path),
            "expected StorageError::UnexpectedSegmentEntry with the correct path"
        );
    }

    #[cfg(unix)]
    #[test]
    fn segment_discovery_rejects_symbolic_link_and_preserves_path() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let target_file_path = dir_path.join("target.txt");
        std::fs::File::create(&target_file_path).expect("failed to create target file");

        let symlink_path = dir_path.join("symlink");
        std::os::unix::fs::symlink(&target_file_path, &symlink_path)
            .expect("failed to create symlink");

        let error = SegmentedLog::discover_segments(dir_path)
            .expect_err("expected an error due to unexpected segment entry");

        assert!(
            matches!(error, StorageError::UnexpectedSegmentEntry { path } if path == symlink_path),
            "expected StorageError::UnexpectedSegmentEntry with the correct path"
        );
    }

    #[test]
    fn segmented_log_open_classifies_sorted_final_segment_as_active() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let base_offset_0 = 1;
        let base_offset_1 = 0;
        let base_offset_2 = 2;
        let file_0_path = dir_path.join(SegmentMetadata::filename(base_offset_0));
        let file_1_path = dir_path.join(SegmentMetadata::filename(base_offset_1));
        let file_2_path = dir_path.join(SegmentMetadata::filename(base_offset_2));
        let limits = RecordLimits::default();

        // Create base 1 before base 0 so discovery must sort by filename offset.
        let (mut log_1, _) = Log::open_active(&file_0_path, base_offset_0, limits)
            .expect("creating the base-1 log should succeed");
        log_1
            .append(&Record::new(
                base_offset_0,
                1_700_000_001,
                Some(vec![40, 50, 60]),
                vec![4, 5, 6],
            ))
            .expect("appending record 1 should succeed");
        drop(log_1);

        let (mut log_0, _) = Log::open_active(&file_1_path, base_offset_1, limits)
            .expect("creating the base-0 log should succeed");
        log_0
            .append(&Record::new(
                base_offset_1,
                1_700_000_000,
                Some(vec![10, 20, 30]),
                vec![1, 2, 3],
            ))
            .expect("appending record 0 should succeed");
        drop(log_0);

        let (log_2, _) = Log::open_active(&file_2_path, base_offset_2, limits)
            .expect("creating the empty base-2 active log should succeed");
        drop(log_2);

        let segmented_log = SegmentedLog::open(dir_path, limits, SegmentConfig::default())
            .expect("opening the segmented log should succeed");

        assert_eq!(
            segmented_log.closed_segments.len(),
            2,
            "Unexpected number of closed segments"
        );
        assert_eq!(
            segmented_log
                .closed_segments
                .iter()
                .map(|segment| segment.metadata.base_offset())
                .collect::<Vec<_>>(),
            vec![base_offset_1, base_offset_0],
            "Closed segments do not have the expected base offsets"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.base_offset(),
            base_offset_2,
            "Active segment does not have the expected base offset"
        );

        assert_eq!(
            segmented_log.next_offset(),
            base_offset_2,
            "an empty active segment should recover its base offset as next_offset"
        );
    }

    #[test]
    fn segmented_log_open_rejects_first_base_other_than_zero() {
        let base_offset = 1;
        let dir = tempfile::tempdir().expect("creating a temp dir should succeed");
        let dir_path = dir.path();
        let segment_path = dir_path.join(SegmentMetadata::filename(base_offset));

        std::fs::File::create(&segment_path)
            .expect("creating the non-zero base segment should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening a segmented log with a non-zero base segment should fail");

        assert!(
            matches!(error, StorageError::UnexpectedSegmentBaseOffset { expected: 0, actual: 1, path } if path == segment_path)
        );
    }

    #[test]
    fn segmented_log_open_rejects_gap_between_segments() {
        let dir = tempfile::tempdir().expect("creating a temp dir should succeed");
        let dir_path = dir.path();
        let closed_segment = dir_path.join(SegmentMetadata::filename(0));
        let active_segment = dir_path.join(SegmentMetadata::filename(2));
        let record = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let encoded_record = record
            .encode(&RecordLimits::default())
            .expect("encoding the record should succeed");

        std::fs::write(&closed_segment, &encoded_record)
            .expect("writing the closed segment should succeed");
        std::fs::File::create(&active_segment).expect("creating the active segment should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening a segmented log with a gap between segments should fail");

        assert!(
            matches!(error, StorageError::UnexpectedSegmentBaseOffset { expected: 1, actual: 2, path } if path == active_segment)
        );

        // make sure the closed segment still exists and is unchanged
        assert!(closed_segment.exists());
        assert!(active_segment.exists());
        assert_eq!(
            std::fs::read(&closed_segment).expect("reading the closed segment should succeed"),
            encoded_record
        );
        assert_eq!(
            std::fs::read(&active_segment).expect("reading the active segment should succeed"),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn segmented_log_open_rejects_overlap_between_segments() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let closed_segment = dir_path.join(SegmentMetadata::filename(0));
        let active_segment = dir_path.join(SegmentMetadata::filename(2));
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record2 = Record::new(1, 1_700_000_000, None, vec![4, 5, 6]);
        let record3 = Record::new(2, 1_700_000_000, None, vec![7, 8, 9]);
        let (mut log, _) = Log::open_active(&closed_segment, 0, RecordLimits::default())
            .expect("opening the active log should succeed");
        log.append(&record1)
            .expect("appending record1 should succeed");
        log.append(&record2)
            .expect("appending record2 should succeed");
        log.append(&record3)
            .expect("appending record3 should succeed");

        drop(log);

        std::fs::write(&active_segment, Vec::<u8>::new())
            .expect("writing the active segment should succeed");
        let closed_file_bytes =
            std::fs::read(&closed_segment).expect("reading the closed segment should succeed");
        let active_file_bytes =
            std::fs::read(&active_segment).expect("reading the active segment should succeed");
        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err(
                "opening the segmented log should fail due to unexpected segment base offset",
            );

        assert!(matches!(
            error,
            StorageError::UnexpectedSegmentBaseOffset {
                expected: 3,
                actual: 2,
                path,
            } if path == active_segment
        ));
        assert_eq!(
            std::fs::read(&closed_segment).expect("reading the closed segment should succeed"),
            closed_file_bytes
        );
        assert_eq!(
            std::fs::read(&active_segment).expect("reading the active segment should succeed"),
            active_file_bytes
        );
    }

    #[test]
    fn segmented_log_open_rejects_empty_closed_segment() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let empty_segment = dir_path.join(SegmentMetadata::filename(0));
        let final_segment = dir_path.join(SegmentMetadata::filename(1));

        std::fs::write(&empty_segment, Vec::<u8>::new())
            .expect("writing the empty segment should succeed");
        std::fs::write(&final_segment, Vec::<u8>::new())
            .expect("writing the final segment should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening the segmented log should fail due to empty closed segment");

        assert!(
            matches!(error, StorageError::EmptyClosedSegment { base_offset: 0, path } if path == empty_segment)
        );
    }

    #[test]
    fn invalid_closed_layout_does_not_recover_partial_active_tail() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let closed_segment = dir_path.join(SegmentMetadata::filename(0));
        let active_segment = dir_path.join(SegmentMetadata::filename(1));

        std::fs::write(&closed_segment, Vec::<u8>::new())
            .expect("writing the invalid closed segment should succeed");

        let active_file_bytes = vec![0x52];
        std::fs::write(&active_segment, &active_file_bytes)
            .expect("writing the active segment should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening the segmented log should fail due to invalid closed segment");

        assert!(
            matches!(error, StorageError::EmptyClosedSegment { base_offset: 0, path } if path == closed_segment)
        );
        assert_eq!(
            std::fs::read(&active_segment).expect("reading the active segment should succeed"),
            active_file_bytes
        );
    }

    #[test]
    fn populated_multi_segment_restart_restores_global_next_offset() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();

        let record_0 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record_1 = Record::new(1, 1_700_000_001, None, vec![4, 5, 6]);
        let record_2 = Record::new(2, 1_700_000_002, None, vec![7, 8, 9]);
        let record_3 = Record::new(3, 1_700_000_003, None, vec![10, 11, 12]);
        let record_4 = Record::new(4, 1_700_000_004, None, vec![13, 14, 15]);
        let record_5 = Record::new(5, 1_700_000_005, None, vec![16, 17, 18]);

        let closed_segment_0 = dir_path.join(SegmentMetadata::filename(0));
        let closed_segment_1 = dir_path.join(SegmentMetadata::filename(2));
        let active_segment = dir_path.join(SegmentMetadata::filename(4));

        let (mut closed_log_0, _) = Log::open_active(&closed_segment_0, 0, RecordLimits::default())
            .expect("opening the closed segment should succeed");
        let (mut closed_log_1, _) = Log::open_active(&closed_segment_1, 2, RecordLimits::default())
            .expect("opening the closed segment should succeed");
        let (mut active_log, _) = Log::open_active(&active_segment, 4, RecordLimits::default())
            .expect("opening the active segment should succeed");

        closed_log_0
            .append(&record_0)
            .expect("appending record 0 should succeed");
        closed_log_0
            .append(&record_1)
            .expect("appending record 1 should succeed");

        closed_log_1
            .append(&record_2)
            .expect("appending record 2 should succeed");
        closed_log_1
            .append(&record_3)
            .expect("appending record 3 should succeed");

        active_log
            .append(&record_4)
            .expect("appending record 4 should succeed");
        active_log
            .append(&record_5)
            .expect("appending record 5 should succeed");

        drop(closed_log_0);
        drop(closed_log_1);
        drop(active_log);

        let segmented_log =
            SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                .expect("opening the segmented log should succeed");

        let closed_segments = &segmented_log.closed_segments;
        assert_eq!(closed_segments[0].metadata.base_offset(), 0);
        assert_eq!(closed_segments[1].metadata.base_offset(), 2);
        assert_eq!(segmented_log.active_segment.metadata.base_offset(), 4);
        assert_eq!(segmented_log.next_offset(), 6);
    }

    #[test]
    fn segmented_log_open_rejects_filename_first_record_mismatch_without_mutation() {
        let dir = tempfile::tempdir().expect("creating temp dir should succeed");
        let dir_path = dir.path();
        let record1 = Record::new(1, 1_700_000_000, None, vec![1, 2, 3]);
        let closed_path = dir_path.join(SegmentMetadata::filename(0));
        let active_path = dir_path.join(SegmentMetadata::filename(10));
        let (mut closed_log, _) = Log::open_active(&closed_path, 0, RecordLimits::default())
            .expect("opening closed log should succeed");

        closed_log
            .append(&record1)
            .expect("appending record 1 should succeed");

        drop(closed_log);

        std::fs::File::create(&active_path).expect("creating active file should succeed");

        let closed_snapshot =
            std::fs::read(&closed_path).expect("reading closed file should succeed");
        let active_snapshot =
            std::fs::read(&active_path).expect("reading active file should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening segmented log should fail due to filename/first-record mismatch");

        assert!(matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 0,
                actual: 1
            }
        ));

        let closed_after = std::fs::read(&closed_path).expect("reading closed file should succeed");
        let active_after = std::fs::read(&active_path).expect("reading active file should succeed");

        assert_eq!(closed_snapshot, closed_after);
        assert_eq!(active_snapshot, active_after);
    }

    #[test]
    fn segmented_log_open_rejects_closed_record_regression_without_mutation() {
        let dir = tempfile::tempdir().expect("creating temp dir should succeed");
        let dir_path = dir.path();
        let closed_path = dir_path.join(SegmentMetadata::filename(0));
        let active_path = dir_path.join(SegmentMetadata::filename(2));
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record2 = Record::new(1, 1_700_000_000, None, vec![4, 5, 6]);
        let record3 = Record::new(0, 1_700_000_000, None, vec![7, 8, 9]);
        let encoded1 = record1
            .encode(&RecordLimits::default())
            .expect("encoding record 1 should succeed");
        let encoded2 = record2
            .encode(&RecordLimits::default())
            .expect("encoding record 2 should succeed");
        let encoded3 = record3
            .encode(&RecordLimits::default())
            .expect("encoding record 3 should succeed");
        let closed_data = [encoded1, encoded2, encoded3].concat();

        std::fs::write(&closed_path, &closed_data).expect("writing encoded records should succeed");
        std::fs::write(&active_path, []).expect("writing empty active file should succeed");

        let closed_snapshot =
            std::fs::read(&closed_path).expect("reading closed file should succeed");
        let active_snapshot =
            std::fs::read(&active_path).expect("reading active file should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening segmented log with closed record regression should fail");

        assert!(matches!(
            error,
            StorageError::UnexpectedOffset {
                expected: 2,
                actual: 0
            }
        ));
        assert_eq!(
            closed_snapshot,
            std::fs::read(&closed_path).expect("reading closed file should succeed")
        );
        assert_eq!(
            active_snapshot,
            std::fs::read(&active_path).expect("reading active file should succeed")
        );
    }

    #[test]
    fn segmented_log_open_rejects_corrupt_closed_record_without_mutation() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let closed_path = dir_path.join(SegmentMetadata::filename(0));
        let active_path = dir_path.join(SegmentMetadata::filename(2));
        let record1 = Record::new(0, 1_700_000_000, None, vec![1, 2, 3]);
        let record2 = Record::new(1, 1_700_000_001, None, vec![4, 5, 6]);
        let encoded_record1 = record1
            .encode(&RecordLimits::default())
            .expect("encoding record should succeed");
        let encoded_record1_len =
            u64::try_from(encoded_record1.len()).expect("record1 length should fit in u64");
        let encoded_record2 = record2
            .encode(&RecordLimits::default())
            .expect("encoding record should succeed");
        let mutated_encoded_record2 = {
            let mut bytes = encoded_record2.clone();
            bytes[0] ^= 0xFF; // Corrupt the first byte
            bytes
        };
        let encoded_records = [
            encoded_record1.as_slice(),
            mutated_encoded_record2.as_slice(),
        ]
        .concat();

        std::fs::write(&closed_path, &encoded_records).expect("writing closed file should succeed");
        std::fs::write(&active_path, []).expect("writing empty active file should succeed");

        let closed_snapshot =
            std::fs::read(&closed_path).expect("reading closed file should succeed");
        let active_snapshot =
            std::fs::read(&active_path).expect("reading active file should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening segmented log should fail due to corrupt record");

        if let StorageError::CorruptRecord {
            path,
            byte_position,
            source,
        } = error
        {
            assert_eq!(path, closed_path);
            assert_eq!(byte_position, encoded_record1_len);
            assert!(
                matches!(source, CodecError::InvalidMagic),
                "expected codec error to be CorruptRecord"
            );
        } else {
            panic!("expected CorruptRecord error");
        }

        assert_eq!(
            closed_snapshot,
            std::fs::read(&closed_path).expect("reading closed file should succeed")
        );
        assert_eq!(
            active_snapshot,
            std::fs::read(&active_path).expect("reading active file should succeed")
        );
    }

    #[test]
    fn segmented_log_open_rejects_incomplete_closed_tail_without_mutation() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let closed_path = dir_path.join(SegmentMetadata::filename(0));
        let active_path = dir_path.join(SegmentMetadata::filename(1));
        let record0 = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record1 = Record::new(1, 1_700_000_001, Some(vec![40, 50, 60]), vec![4, 5, 6]);
        let encoded_record0 = record0
            .encode(&RecordLimits::default())
            .expect("encoding record0 should succeed");
        let encoded_record1 = record1
            .encode(&RecordLimits::default())
            .expect("encoding record1 should succeed");
        let incomplete_record_1 = &encoded_record1[..1];
        let encoded = [encoded_record0.as_slice(), incomplete_record_1].concat();

        std::fs::write(&closed_path, &encoded).expect("writing closed file should succeed");
        std::fs::write(&active_path, incomplete_record_1)
            .expect("writing active file should succeed");

        let closed_snapshot =
            std::fs::read(&closed_path).expect("reading closed file should succeed");
        let active_snapshot =
            std::fs::read(&active_path).expect("reading active file should succeed");

        let error = SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
            .expect_err("opening segmented log with incomplete closed tail should fail");

        assert!(matches!(
            error,
            StorageError::Codec(CodecError::IncompleteHeader)
        ));
        assert_eq!(
            std::fs::read(&closed_path).expect("reading closed file should succeed"),
            closed_snapshot,
            "closed file should not be truncated"
        );
        assert_eq!(
            std::fs::read(&active_path).expect("reading active file should succeed"),
            active_snapshot,
            "active file should not be modified"
        );
    }

    #[test]
    fn repeated_multi_segment_restarts_preserve_bytes_records_and_next_offset() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let closed_path1 = dir_path.join(SegmentMetadata::filename(0));
        let closed_path2 = dir_path.join(SegmentMetadata::filename(2));
        let active_path = dir_path.join(SegmentMetadata::filename(4));

        let record0 = Record::new(0, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record1 = Record::new(1, 1_700_000_001, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record2 = Record::new(2, 1_700_000_002, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record3 = Record::new(3, 1_700_000_003, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record4 = Record::new(4, 1_700_000_004, Some(vec![10, 20, 30]), vec![1, 2, 3]);
        let record5 = Record::new(5, 1_700_000_005, Some(vec![10, 20, 30]), vec![1, 2, 3]);

        let encoded_record0 = record0
            .encode(&RecordLimits::default())
            .expect("encoding record0 should succeed");
        let encoded_record1 = record1
            .encode(&RecordLimits::default())
            .expect("encoding record1 should succeed");
        let encoded_record2 = record2
            .encode(&RecordLimits::default())
            .expect("encoding record2 should succeed");
        let encoded_record3 = record3
            .encode(&RecordLimits::default())
            .expect("encoding record3 should succeed");
        let encoded_record4 = record4
            .encode(&RecordLimits::default())
            .expect("encoding record4 should succeed");
        let encoded_record5 = record5
            .encode(&RecordLimits::default())
            .expect("encoding record5 should succeed");

        let closed_data_1 = [encoded_record0.as_slice(), encoded_record1.as_slice()].concat();
        let closed_data_2 = [encoded_record2.as_slice(), encoded_record3.as_slice()].concat();
        let active_data = [encoded_record4.as_slice(), encoded_record5.as_slice()].concat();

        std::fs::write(&closed_path1, &closed_data_1)
            .expect("writing closed segment 1 should succeed");
        std::fs::write(&closed_path2, &closed_data_2)
            .expect("writing closed segment 2 should succeed");
        std::fs::write(&active_path, &active_data).expect("writing active segment should succeed");

        let closed_snapshot_1 =
            std::fs::read(&closed_path1).expect("reading closed segment 1 should succeed");
        let closed_snapshot_2 =
            std::fs::read(&closed_path2).expect("reading closed segment 2 should succeed");
        let active_snapshot =
            std::fs::read(&active_path).expect("reading active segment should succeed");
        let mut sucessful_iterations = 0;

        for _ in 0..100 {
            let mut offsets_found = vec![];
            let segmented_log =
                SegmentedLog::open(dir_path, RecordLimits::default(), SegmentConfig::default())
                    .expect("opening segmented log should succeed");
            assert_eq!(
                segmented_log.next_offset(),
                6,
                "the next offset should be 6 after reopening the segmented log"
            );
            let mut asserted_closed_segments = 0;
            let mut asserted_active_segment = 0;
            for closed_segment in segmented_log.closed_segments {
                let scanner = closed_segment
                    .scan()
                    .expect("scanning closed segment should succeed");

                for result in scanner {
                    let record = result.expect("reading record from scanner should succeed");
                    offsets_found.push(record.offset());
                }

                asserted_closed_segments += 1;
            }
            let active_segment = segmented_log.active_segment;
            let scanner = active_segment
                .scan()
                .expect("scanning active segment should succeed");
            for result in scanner {
                let record = result.expect("reading record from scanner should succeed");
                offsets_found.push(record.offset());
            }
            asserted_active_segment += 1;

            assert_eq!(
                asserted_closed_segments, 2,
                "there should be exactly 2 closed segments"
            );
            assert_eq!(
                offsets_found,
                vec![0, 1, 2, 3, 4, 5],
                "the combined record offsets should be [0, 1, 2, 3, 4, 5]"
            );
            assert_eq!(
                asserted_active_segment, 1,
                "there should be exactly 1 active segment"
            );
            assert!(
                std::fs::read(&closed_path1).expect("reading closed segment 1 should succeed")
                    == closed_snapshot_1,
                "closed segment 1 should match its snapshot",
            );
            assert!(
                std::fs::read(&closed_path2).expect("reading closed segment 2 should succeed")
                    == closed_snapshot_2,
                "closed segment 2 should match its snapshot",
            );
            assert!(
                std::fs::read(&active_path).expect("reading active segment should succeed")
                    == active_snapshot,
                "active segment should match its snapshot",
            );

            sucessful_iterations += 1;
        }

        assert_eq!(
            sucessful_iterations, 100,
            "there should be exactly 100 successful iterations"
        );
    }

    #[test]
    fn segmented_log_open_reports_active_length_after_tail_recovery() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let base_offset = 0;
        let path = dir_path.join(SegmentMetadata::filename(base_offset));
        let limits = RecordLimits::default();

        let (mut log, _) = Log::open_active(&path, base_offset, limits)
            .expect("creating the active log should succeed");

        log.append(&Record::new(
            base_offset,
            1_700_000_000,
            Some(vec![10, 20, 30]),
            vec![1, 2, 3],
        ))
        .expect("appending the complete record should succeed");

        let valid_file_len = log
            .len()
            .expect("reading the valid log length should succeed");

        drop(log);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("opening the active log for tail corruption should succeed");

        file.write_all(&[0x52])
            .expect("writing an incomplete header byte should succeed");
        drop(file);

        let discovered_file_len = path
            .metadata()
            .expect("reading the corrupted log metadata should succeed")
            .len();

        assert!(
            discovered_file_len > valid_file_len,
            "the incomplete tail should increase the file length before recovery"
        );

        let segmented_log = SegmentedLog::open(dir_path, limits, SegmentConfig::default())
            .expect("opening the segmented log should recover its active tail");
        let recovered_file_len = path
            .metadata()
            .expect("reading the recovered log metadata should succeed")
            .len();

        assert_eq!(
            recovered_file_len, valid_file_len,
            "active recovery should truncate only the incomplete tail"
        );
        assert_eq!(
            segmented_log.active_segment.metadata.file_len(),
            valid_file_len,
            "active metadata should report the post-recovery file length"
        );
        assert_eq!(
            segmented_log.next_offset(),
            base_offset + 1,
            "next_offset should follow the final complete record"
        );
    }
}
