use std::path::PathBuf;

use crate::broker::topic::TopicName;

#[derive(Debug, PartialEq, Eq)]
pub enum PartitionError {
    OffsetOverflow,
    ClockBeforeEpoch,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CodecError {
    InvalidMagic,
    UnsupportedVersion,
    KeyTooLarge,
    PayloadTooLarge,
    IncompleteHeader,
    IncompleteBody,
    LengthOverflow,
    InvalidKeyPresence,
    InvalidKeyLength,
    InvalidChecksum,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::InvalidMagic => write!(f, "invalid magic"),
            CodecError::UnsupportedVersion => write!(f, "unsupported version"),
            CodecError::KeyTooLarge => write!(f, "key too large"),
            CodecError::PayloadTooLarge => write!(f, "payload too large"),
            CodecError::IncompleteHeader => write!(f, "incomplete header"),
            CodecError::IncompleteBody => write!(f, "incomplete body"),
            CodecError::LengthOverflow => write!(f, "length overflow"),
            CodecError::InvalidKeyPresence => write!(f, "invalid key presence"),
            CodecError::InvalidKeyLength => write!(f, "invalid key length"),
            CodecError::InvalidChecksum => write!(f, "invalid checksum"),
        }
    }
}

impl std::error::Error for CodecError {}

#[derive(Debug)]
pub enum StorageError {
    Codec(CodecError),
    Io(std::io::Error),
    AppendDisabled,
    OffsetOverflow,
    UnexpectedOffset {
        expected: u64,
        actual: u64,
    },
    CorruptRecord {
        path: PathBuf,
        byte_position: u64,
        source: CodecError,
    },
    InvalidSegmentFilename {
        path: PathBuf,
    },
    UnexpectedSegmentEntry {
        path: PathBuf,
    },
    DuplicateSegmentBaseOffset {
        base_offset: u64,
        first_path: PathBuf,
        second_path: PathBuf,
    },
    UnexpectedSegmentBaseOffset {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    EmptyClosedSegment {
        path: PathBuf,
        base_offset: u64,
    },
    RotationAfterCommit {
        committed_offset: u64,
        next_offset: u64,
        source: Box<StorageError>,
    },
}

impl From<CodecError> for StorageError {
    fn from(err: CodecError) -> Self {
        StorageError::Codec(err)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Codec(err) => write!(f, "codec error: {}", err),
            StorageError::Io(err) => write!(f, "io error: {}", err),
            StorageError::AppendDisabled => write!(f, "append disabled"),
            StorageError::OffsetOverflow => write!(f, "offset overflow"),
            StorageError::UnexpectedOffset { expected, actual } => write!(
                f,
                "unexpected offset: expected {}, actual {}",
                expected, actual
            ),
            StorageError::CorruptRecord {
                path,
                byte_position,
                source,
            } => write!(
                f,
                "corrupt record in {} at byte {}: {}",
                path.display(),
                byte_position,
                source
            ),
            StorageError::InvalidSegmentFilename { path } => {
                write!(f, "invalid segment filename: {}", path.display())
            }
            StorageError::UnexpectedSegmentEntry { path } => {
                write!(
                    f,
                    "unexpected entry in segment directory: {}",
                    path.display()
                )
            }
            StorageError::DuplicateSegmentBaseOffset {
                base_offset,
                first_path,
                second_path,
            } => {
                write!(
                    f,
                    "duplicate segment base offset {}: {} and {}",
                    base_offset,
                    first_path.display(),
                    second_path.display()
                )
            }
            StorageError::UnexpectedSegmentBaseOffset {
                path,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "unexpected segment base offset in {}: expected {}, actual {}",
                    path.display(),
                    expected,
                    actual
                )
            }
            StorageError::EmptyClosedSegment { path, base_offset } => {
                write!(
                    f,
                    "closed segment is empty: {} (base offset {})",
                    path.display(),
                    base_offset
                )
            }
            StorageError::RotationAfterCommit {
                committed_offset,
                next_offset,
                source,
            } => {
                write!(
                    f,
                    "rotation after commit: committed offset {}, next offset {}: {}",
                    committed_offset, next_offset, source
                )
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Codec(err) => Some(err),
            StorageError::Io(err) => Some(err),
            StorageError::AppendDisabled => None,
            StorageError::OffsetOverflow => None,
            StorageError::UnexpectedOffset { .. } => None,
            StorageError::CorruptRecord { source, .. } => Some(source),
            StorageError::InvalidSegmentFilename { .. } => None,
            StorageError::UnexpectedSegmentEntry { .. } => None,
            StorageError::DuplicateSegmentBaseOffset { .. } => None,
            StorageError::UnexpectedSegmentBaseOffset { .. } => None,
            StorageError::EmptyClosedSegment { .. } => None,
            StorageError::RotationAfterCommit { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConfigurationError {
    InvalidSegmentBytes { value: u64 },
}

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigurationError::InvalidSegmentBytes { value } => {
                write!(f, "invalid segment bytes: {}", value)
            }
        }
    }
}

impl std::error::Error for ConfigurationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigurationError::InvalidSegmentBytes { .. } => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TopicNameError {
    InvalidLength { actual: usize },
    InvalidCharacter { character: char },
}

impl std::fmt::Display for TopicNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicNameError::InvalidLength { actual } => write!(
                f,
                "invalid topic name length: {}, 1 to 128 bytes allowed",
                actual
            ),
            TopicNameError::InvalidCharacter { character } => write!(
                f,
                "invalid character '{}' in topic name, only ASCII letters, digits, - and _ allowed.",
                character
            ),
        }
    }
}

impl std::error::Error for TopicNameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TopicNameError::InvalidLength { .. } => None,
            TopicNameError::InvalidCharacter { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum TopicError {
    AlreadyExists {
        name: TopicName,
    },
    NotFound {
        name: TopicName,
    },
    Io {
        source: std::io::Error,
    },
    Storage {
        source: StorageError,
    },
}

impl std::fmt::Display for TopicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicError::AlreadyExists { name } => {
                write!(f, "topic {} already exists", name.as_str())
            }
            TopicError::NotFound { name } => {
                write!(f, "topic {} not found", name.as_str())
            }
            TopicError::Io { source } => {
                write!(f, "I/O error: {}", source)
            }
            TopicError::Storage { source } => {
                write!(f, "storage error: {}", source)
            }
        }
    }
}

impl std::error::Error for TopicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TopicError::AlreadyExists { .. } => None,
            TopicError::NotFound { .. } => None,
            TopicError::Io { source } => Some(source),
            TopicError::Storage { source } => Some(source),
        }
    }
}

impl From<std::io::Error> for TopicError {
    fn from(source: std::io::Error) -> Self {
        TopicError::Io { source }
    }
}

impl From<StorageError> for TopicError {
    fn from(source: StorageError) -> Self {
        TopicError::Storage { source }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn topic_already_exists_preserves_name_and_has_no_source() {
        // Construct AlreadyExists with a validated name. Match the variant,
        // check the stored name, and verify Error::source() returns None.
        todo!("test duplicate-topic error");
    }

    #[test]
    fn topic_not_found_preserves_name_and_has_no_source() {
        // Construct NotFound with a validated name. Match the variant,
        // check the stored name, and verify Error::source() returns None.
        todo!("test missing-topic error");
    }

    #[test]
    fn topic_error_from_io_preserves_kind_and_message() {
        // Convert an I/O error with a chosen kind and message into TopicError.
        // Match Io and check that both the kind and message are preserved.
        todo!("test I/O error conversion");
    }

    #[test]
    fn topic_error_from_storage_preserves_variant_and_fields() {
        // Convert StorageError::UnexpectedOffset with distinct expected/actual
        // offsets. Check the Storage wrapper, inner variant, and both offsets.
        todo!("test storage error conversion");
    }

    #[test]
    fn topic_io_error_exposes_underlying_source() {
        // Call Error::source() on a converted I/O error. Downcast the returned
        // source to std::io::Error and verify its kind and message.
        todo!("test I/O error source");
    }

    #[test]
    fn topic_storage_error_preserves_nested_source_chain() {
        // Wrap an I/O error in StorageError, then convert it into TopicError.
        // The first source should downcast to StorageError; its source should
        // downcast to std::io::Error and retain the original kind and message.
        todo!("test nested storage error source chain");
    }
}
