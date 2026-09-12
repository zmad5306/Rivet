use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use crate::error::ConfigurationError;

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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::{
        error::ConfigurationError,
        storage::segment::{SegmentConfig, SegmentMetadata},
    };

    #[test]
    fn default_segment_config_uses_sixty_four_mibibytes() {
        let config = SegmentConfig::default();
        let expected_byte: u64 = 64 * 1024 * 1024;
        let actual_byte = config.max_segment_bytes();
        assert_eq!(actual_byte, expected_byte);
    }

    #[test]
    fn segment_config_preserves_custom_max_segment_bytes() {
        let max_segment_bytes = 1024; // Small nonzero value suitable for rotation tests
        let config = SegmentConfig::new(max_segment_bytes).expect("Failed to create SegmentConfig");
        let actual_byte = config.max_segment_bytes();
        assert_eq!(actual_byte, max_segment_bytes);
    }

    #[test]
    fn segment_config_rejects_zero_max_segment_bytes() {
        let error = SegmentConfig::new(0)
            .expect_err("constructing SegmentConfig with zero max_segment_bytes should fail");
        assert_eq!(error, ConfigurationError::InvalidSegmentBytes { value: 0 });
    }

    #[test]
    fn segment_metadata_constructor_preserves_all_fields() {
        let base_offset = 42;
        let byte_len = 1024;
        let path = Path::new("segment.log");
        let metadata = SegmentMetadata::new(base_offset, path, byte_len);
        assert_eq!(metadata.base_offset(), base_offset);
        assert_eq!(metadata.path(), path);
        assert_eq!(metadata.byte_len(), byte_len);
    }
}
