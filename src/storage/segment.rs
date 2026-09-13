use std::{
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

    pub fn log(&self) -> &Log {
        &self.log
    }
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
}

#[cfg(test)]
mod tests {
    use std::{fs::read, path::Path};

    use crate::{
        error::{ConfigurationError, StorageError},
        storage::{
            log::Log,
            record::RecordLimits,
            segment::{ActiveSegment, ClosedSegment, SegmentConfig, SegmentMetadata},
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
        let byte_len = 1024;
        let path = Path::new("segment.log");
        let metadata = SegmentMetadata::new(base_offset, path, byte_len);
        let (log, _) = Log::open_active(path, base_offset, RecordLimits::default())
            .expect("opening a missing log path should succeed");
        let closed_segment = ClosedSegment::new(metadata, log);
        let exposed_metadata = closed_segment.metadata();
        assert_eq!(
            exposed_metadata.base_offset(),
            base_offset,
            "ClosedSegment should expose the correct base offset"
        );
        assert_eq!(
            exposed_metadata.path(),
            path,
            "ClosedSegment should expose the correct path"
        );
        assert_eq!(
            exposed_metadata.byte_len(),
            byte_len,
            "ClosedSegment should expose the correct byte length"
        );
    }

    #[test]
    fn active_segment_exposes_supplied_metadata() {
        let dir = tempfile::tempdir().expect("temporary directory should be created");
        let path = dir.path().join("00000000000000000000.log");
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
        todo!(
            "Implement this test in this order:\n\
             1. Create a temporary partition directory and retain its PathBuf.\n\
             2. Create one record at offset 0 and append it to an active Log whose canonical filename has base offset 0.\n\
             3. Drop that writable Log, reopen the file with Log::open_closed(), and construct matching SegmentMetadata using the file's actual byte length.\n\
             4. Move the metadata and read-only Log into a ClosedSegment.\n\
             5. Open an empty active Log at the canonical filename for base offset 1 and construct matching active metadata and ActiveSegment.\n\
             6. Create distinct RecordLimits and SegmentConfig values so the getters cannot pass accidentally by returning defaults.\n\
             7. Construct SegmentedLog directly with one closed segment, the active segment, next offset 1, the limits, the config, and the temporary directory path; this unit-test module may access the parent's private fields.\n\
             8. Assert that directory() returns the partition directory.\n\
             9. Assert that closed_segments() has length 1 and its element has base offset 0.\n\
             10. Assert that active_segment() has base offset 1.\n\
            11. Assert that next_offset(), limits(), and config() expose the supplied values.\n\
             12. Keep the TempDir alive through all assertions and do not add mutable getters or a public unchecked constructor for the test."
        )
    }
}
