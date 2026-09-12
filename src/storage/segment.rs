use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use crate::{error::ConfigurationError, storage::log::Log};

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
}

#[derive(Debug, PartialEq, Eq)]
pub struct ClosedSegment {
    metadata: SegmentMetadata,
}

impl ClosedSegment {
    pub fn new(metadata: SegmentMetadata) -> Self {
        Self { metadata }
    }

    pub fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
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

#[cfg(test)]
mod tests {
    use std::{fs::read, path::Path};

    use crate::{
        error::ConfigurationError,
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
        let closed_segment = ClosedSegment::new(metadata);
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
}
