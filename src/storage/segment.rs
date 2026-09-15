use std::{
    fs::{create_dir_all, read_dir},
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use crate::{
    error::{ConfigurationError, StorageError},
    storage::{
        log::{Log, LogScanner},
        record::RecordLimits,
    },
};

const SEGMENT_SIZE: u64 = 1024 * 1024 * 64; // 64 MB segment size

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
pub struct SegmentMetadata {
    base_offset: u64,
    path: PathBuf,
    byte_len: u64,
}

impl SegmentMetadata {
    pub fn new(base_offset: u64, path: &Path, byte_len: u64) -> Self {
        Self {
            base_offset,
            path: path.to_path_buf(),
            byte_len,
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub fn filename(base_offset: u64) -> String {
        format!("{base_offset:020}.log")
    }

    pub fn parse_base_offset(path: &Path) -> Result<u64, StorageError> {
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
pub struct ClosedSegment {
    metadata: SegmentMetadata,
    log: Log,
}

impl ClosedSegment {
    pub fn new(metadata: SegmentMetadata, log: Log) -> Self {
        Self { metadata, log }
    }

    pub fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        self.log.scan()
    }
}

#[derive(Debug)]
pub struct ActiveSegment {
    metadata: SegmentMetadata,
    log: Log,
}

impl ActiveSegment {
    pub fn new(metadata: SegmentMetadata, log: Log) -> Self {
        Self { metadata, log }
    }

    pub fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub fn scan(&self) -> Result<LogScanner<'_>, StorageError> {
        self.log.scan()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SegmentCandidate {
    base_offset: u64,
    path: PathBuf,
    byte_len: u64,
}

#[derive(Debug)]
pub struct SegmentedLog {
    directory: PathBuf,
    closed_segments: Vec<ClosedSegment>,
    active_segment: ActiveSegment,
    next_offset: u64,
    limits: RecordLimits,
    config: SegmentConfig,
}

impl SegmentedLog {
    pub fn directory(&self) -> &Path {
        self.directory.as_path()
    }

    pub fn closed_segments(&self) -> &[ClosedSegment] {
        &self.closed_segments
    }

    pub fn active_segment(&self) -> &ActiveSegment {
        &self.active_segment
    }

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
            let byte_len = dir_entry.metadata()?.len();

            candidates.push(SegmentCandidate {
                base_offset,
                path,
                byte_len,
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
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{OpenOptions, read},
        io::Write,
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
        let byte_len = 1024;
        let path = Path::new("segment.log");
        let metadata = SegmentMetadata::new(base_offset, path, byte_len);
        assert_eq!(
            metadata.base_offset(),
            base_offset,
            "SegmentMetadata should preserve the base offset"
        );
        assert_eq!(
            metadata.path(),
            path,
            "SegmentMetadata should preserve the path"
        );
        assert_eq!(
            metadata.byte_len(),
            byte_len,
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
        let closed_segment = ClosedSegment::new(metadata, log);

        assert_eq!(
            closed_segment.metadata().base_offset(),
            base_offset,
            "ClosedSegment should expose the correct base offset"
        );
        assert_eq!(
            closed_segment.metadata().path(),
            path,
            "ClosedSegment should expose the correct path"
        );
        assert_eq!(
            closed_segment.metadata().byte_len(),
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
        let byte_len = read(&path).map(|b| b.len()).unwrap_or(0);
        let byte_length = u64::try_from(byte_len).expect("byte length should fit in u64");
        let base_offset = 0;
        let metadata = SegmentMetadata::new(base_offset, &path, byte_length);
        let active_segment = ActiveSegment::new(metadata, log);
        let exposed_metadata = active_segment.metadata();
        assert_eq!(
            exposed_metadata.base_offset(),
            base_offset,
            "ActiveSegment should expose the correct base offset"
        );
        assert_eq!(
            exposed_metadata.path(),
            path,
            "ActiveSegment should expose the correct path"
        );
        assert_eq!(
            exposed_metadata.byte_len(),
            byte_length,
            "ActiveSegment should expose the correct byte length"
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
        let closed_segment = ClosedSegment::new(closed_metadata, closed_log);
        let active_segment = ActiveSegment::new(active_metadata, active_log);
        let segmented_log = SegmentedLog {
            directory: dir_path.to_path_buf(),
            closed_segments: vec![closed_segment],
            active_segment,
            config,
            next_offset: base_offset + 1,
            limits,
        };

        assert_eq!(
            segmented_log.directory(),
            dir_path,
            "segmented log should preserve its partition directory"
        );

        assert_eq!(
            segmented_log.closed_segments().len(),
            1,
            "segmented log should contain one closed segment"
        );

        let exposed_closed = &segmented_log.closed_segments()[0];

        assert_eq!(exposed_closed.metadata().base_offset(), 0);
        assert_eq!(exposed_closed.metadata().path(), closed_path);
        assert_eq!(exposed_closed.metadata().byte_len(), closed_file_len);

        let exposed_active = segmented_log.active_segment();

        assert_eq!(exposed_active.metadata().base_offset(), 1);
        assert_eq!(exposed_active.metadata().path(), active_path);
        assert_eq!(exposed_active.metadata().byte_len(), 0);

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
            segmented_log.closed_segments().is_empty(),
            "there should be no closed segments initially"
        );

        let active_segment = segmented_log.active_segment();
        assert_eq!(active_segment.metadata().base_offset(), 0);
        assert_eq!(active_segment.metadata().path(), path);
        assert_eq!(active_segment.metadata().byte_len(), 0);

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
                candidate.byte_len, 0,
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
            segmented_log.closed_segments().len(),
            2,
            "Unexpected number of closed segments"
        );
        assert_eq!(
            segmented_log
                .closed_segments()
                .iter()
                .map(|segment| segment.metadata.base_offset())
                .collect::<Vec<_>>(),
            vec![base_offset_1, base_offset_0],
            "Closed segments do not have the expected base offsets"
        );
        assert_eq!(
            segmented_log.active_segment().metadata.base_offset(),
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

        let closed_segments = segmented_log.closed_segments();
        assert_eq!(closed_segments[0].metadata().base_offset(), 0);
        assert_eq!(closed_segments[1].metadata().base_offset(), 2);
        assert_eq!(segmented_log.active_segment().metadata().base_offset(), 4);
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
            for closed_segment in segmented_log.closed_segments() {
                let scanner = closed_segment
                    .scan()
                    .expect("scanning closed segment should succeed");

                for result in scanner {
                    let record = result.expect("reading record from scanner should succeed");
                    offsets_found.push(record.offset());
                }

                asserted_closed_segments += 1;
            }
            let active_segment = segmented_log.active_segment();
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

        let valid_byte_len = log
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

        let discovered_byte_len = path
            .metadata()
            .expect("reading the corrupted log metadata should succeed")
            .len();

        assert!(
            discovered_byte_len > valid_byte_len,
            "the incomplete tail should increase the file length before recovery"
        );

        let segmented_log = SegmentedLog::open(dir_path, limits, SegmentConfig::default())
            .expect("opening the segmented log should recover its active tail");
        let recovered_byte_len = path
            .metadata()
            .expect("reading the recovered log metadata should succeed")
            .len();

        assert_eq!(
            recovered_byte_len, valid_byte_len,
            "active recovery should truncate only the incomplete tail"
        );
        assert_eq!(
            segmented_log.active_segment().metadata().byte_len(),
            valid_byte_len,
            "active metadata should report the post-recovery file length"
        );
        assert_eq!(
            segmented_log.next_offset(),
            base_offset + 1,
            "next_offset should follow the final complete record"
        );
    }
}
