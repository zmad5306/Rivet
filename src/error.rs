use std::path::PathBuf;

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
