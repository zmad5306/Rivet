use std::path::PathBuf;

use crate::broker::topic::TopicName;

#[derive(Debug)]
pub enum PartitionError {
    OffsetOverflow,
    ClockBeforeEpoch,
    Storage { source: StorageError },
}

impl From<StorageError> for PartitionError {
    fn from(err: StorageError) -> Self {
        PartitionError::Storage { source: err }
    }
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionError::OffsetOverflow => write!(f, "offset overflow"),
            PartitionError::ClockBeforeEpoch => write!(f, "clock before epoch"),
            PartitionError::Storage { source } => write!(f, "storage error: {}", source),
        }
    }
}

impl std::error::Error for PartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PartitionError::Storage { source } => Some(source),
            _ => None,
        }
    }
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
        let message = match self {
            CodecError::InvalidMagic => "invalid magic",
            CodecError::UnsupportedVersion => "unsupported version",
            CodecError::KeyTooLarge => "key too large",
            CodecError::PayloadTooLarge => "payload too large",
            CodecError::IncompleteHeader => "incomplete header",
            CodecError::IncompleteBody => "incomplete body",
            CodecError::LengthOverflow => "length overflow",
            CodecError::InvalidKeyPresence => "invalid key presence",
            CodecError::InvalidKeyLength => "invalid key length",
            CodecError::InvalidChecksum => "invalid checksum",
        };
        f.write_str(message)
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

#[derive(Debug, PartialEq, Eq)]
pub enum CatalogEntryErrorReason {
    InvalidTopicName,
    NotDirectory,
    SymbolicLink,
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
    MissingPartitionDirectory {
        path: PathBuf,
    },
    UnexpectedCatalogEntry {
        path: PathBuf,
        reason: CatalogEntryErrorReason,
    },
    UnsupportedPartitionId {
        requested: u32,
    },
    UnsupportedPartitionCount {
        requested: u32,
    },
    Partition {
        source: PartitionError,
    },
    InvalidTopicName {
        source: TopicNameError,
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
            TopicError::MissingPartitionDirectory { path } => {
                write!(f, "missing partition directory: {}", path.display())
            }
            TopicError::UnexpectedCatalogEntry { path, reason } => {
                let message = match reason {
                    CatalogEntryErrorReason::InvalidTopicName => "invalid topic name",
                    CatalogEntryErrorReason::NotDirectory => "expected a directory",
                    CatalogEntryErrorReason::SymbolicLink => "symbolic links are not supported",
                };
                write!(
                    f,
                    "unexpected catalog entry at {}: {}",
                    path.display(),
                    message
                )
            }
            TopicError::UnsupportedPartitionId { requested } => {
                write!(
                    f,
                    "unsupported partition id: {}, only partition id 0 is supported",
                    requested
                )
            }
            TopicError::UnsupportedPartitionCount { requested } => {
                write!(
                    f,
                    "unsupported partition count: {}, only a count of 1 is supported",
                    requested
                )
            }
            TopicError::Partition { source } => {
                write!(f, "partition error: {}", source)
            }
            TopicError::InvalidTopicName { source } => {
                write!(f, "invalid topic name: {}", source)
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
            TopicError::MissingPartitionDirectory { .. } => None,
            TopicError::UnexpectedCatalogEntry { .. } => None,
            TopicError::UnsupportedPartitionId { .. } => None,
            TopicError::UnsupportedPartitionCount { .. } => None,
            TopicError::Partition { source } => Some(source),
            TopicError::InvalidTopicName { source } => Some(source),
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

impl From<PartitionError> for TopicError {
    fn from(source: PartitionError) -> Self {
        TopicError::Partition { source }
    }
}

impl From<TopicNameError> for TopicError {
    fn from(source: TopicNameError) -> Self {
        TopicError::InvalidTopicName { source }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        broker::topic::TopicName,
        error::{CatalogEntryErrorReason, CodecError, PartitionError, StorageError, TopicError},
    };
    use std::{error::Error, path::PathBuf};

    #[test]
    fn unsupported_partition_id_reports_requested_value_and_has_no_source() {
        let partition_id: u32 = 3;
        let error = TopicError::UnsupportedPartitionId {
            requested: partition_id,
        };

        assert!(
            matches!(&error, TopicError::UnsupportedPartitionId { requested: actual } if actual == &partition_id),
            ""
        );
        assert_eq!(
            &error.to_string(),
            "unsupported partition id: 3, only partition id 0 is supported",
            ""
        );
        assert!(error.source().is_none(), "");
    }

    #[test]
    fn unsupported_partition_count_reports_requested_value_and_has_no_source() {
        let partition_count: u32 = 2;
        let error = TopicError::UnsupportedPartitionCount {
            requested: partition_count,
        };

        assert!(
            matches!(&error, TopicError::UnsupportedPartitionCount { requested: actual } if actual == &partition_count),
            ""
        );
        assert_eq!(
            &error.to_string(),
            "unsupported partition count: 2, only a count of 1 is supported",
            ""
        );
        assert!(error.source().is_none(), "");
    }

    #[test]
    fn unexpected_catalog_entry_invalid_name_preserves_context() {
        let path = PathBuf::from("data").join("bad name");
        let topic_error = TopicError::UnexpectedCatalogEntry {
            path: path.clone(),
            reason: CatalogEntryErrorReason::InvalidTopicName,
        };
        assert!(matches!(
            &topic_error,
            TopicError::UnexpectedCatalogEntry {
                path: actual,
                reason: CatalogEntryErrorReason::InvalidTopicName,
            } if actual == &path
        ));

        let diagnostic = topic_error.to_string();
        assert!(diagnostic.contains("invalid topic name"));
        assert!(diagnostic.contains(&path.display().to_string()));

        assert!(topic_error.source().is_none());
    }

    #[test]
    fn unexpected_catalog_entry_not_directory_preserves_context() {
        let path = PathBuf::from("data").join("orders");
        let topic_error = TopicError::UnexpectedCatalogEntry {
            path: path.clone(),
            reason: CatalogEntryErrorReason::NotDirectory,
        };
        assert!(matches!(
            &topic_error,
            TopicError::UnexpectedCatalogEntry {
                path: actual,
                reason: CatalogEntryErrorReason::NotDirectory,
            } if actual == &path
        ));

        let diagnostic = topic_error.to_string();
        assert!(diagnostic.contains("expected a directory"));
        assert!(diagnostic.contains(&path.display().to_string()));

        assert!(topic_error.source().is_none());
    }

    #[test]
    fn unexpected_catalog_entry_symbolic_link_preserves_context() {
        let path = PathBuf::from("data").join("linked_topic");
        let topic_error = TopicError::UnexpectedCatalogEntry {
            path: path.clone(),
            reason: CatalogEntryErrorReason::SymbolicLink,
        };
        assert!(matches!(
            &topic_error,
            TopicError::UnexpectedCatalogEntry {
                path: actual,
                reason: CatalogEntryErrorReason::SymbolicLink,
            } if actual == &path
        ));

        let diagnostic = topic_error.to_string();
        assert!(diagnostic.contains("symbolic links are not supported"));
        assert!(diagnostic.contains(&path.display().to_string()));

        assert!(topic_error.source().is_none());
    }

    #[test]
    fn missing_partition_directory_preserves_path_and_has_no_source() {
        let path = PathBuf::from("data").join("orders").join("0");
        let error = TopicError::MissingPartitionDirectory { path: path.clone() };

        assert!(
            matches!(&error, TopicError::MissingPartitionDirectory { path: actual } if actual == &path),
            "Expected MissingPartitionDirectory error with path {}",
            path.display()
        );
        assert!(
            error
                .to_string()
                .contains(&format!("missing partition directory: {}", path.display())),
            "Expected error message to include the problem and path"
        );
        assert!(
            error.source().is_none(),
            "Expected no source for MissingPartitionDirectory error"
        );
    }

    #[test]
    fn topic_already_exists_preserves_name_and_has_no_source() {
        let topic_name = "a-topic";
        let name = TopicName::new(topic_name.to_string()).expect("failed to create TopicName");
        let error = TopicError::AlreadyExists { name };
        assert!(
            matches!(&error, TopicError::AlreadyExists { name } if name.as_str() == topic_name),
            "Expected AlreadyExists error with name {}",
            topic_name
        );
        assert!(
            error.source().is_none(),
            "Expected no source for AlreadyExists error"
        );
    }

    #[test]
    fn topic_not_found_preserves_name_and_has_no_source() {
        let topic_name = "a-topic";
        let name = TopicName::new(topic_name.to_string()).expect("failed to create TopicName");
        let error = TopicError::NotFound { name };
        assert!(
            matches!(&error, TopicError::NotFound { name } if name.as_str() == topic_name),
            "Expected NotFound error with name {}",
            topic_name
        );
        assert!(
            error.source().is_none(),
            "Expected no source for NotFound error"
        );
    }

    #[test]
    fn topic_error_from_io_preserves_kind_and_message() {
        let io_error = std::io::Error::other("oh no!");
        let error = TopicError::from(io_error);
        assert!(
            matches!(error, TopicError::Io { source } if source.kind() == std::io::ErrorKind::Other && source.to_string() == "oh no!")
        );
    }

    #[test]
    fn topic_error_from_storage_preserves_variant_and_fields() {
        let storage_error = StorageError::UnexpectedOffset {
            expected: 1,
            actual: 2,
        };
        let error = TopicError::from(storage_error);
        assert!(
            matches!(error, TopicError::Storage { source } if matches!(source, StorageError::UnexpectedOffset { expected: 1, actual: 2 }))
        );
    }

    #[test]
    fn topic_error_from_partition_preserves_nested_source_chain() {
        let storage_error = StorageError::UnexpectedOffset {
            expected: 1,
            actual: 2,
        };
        let partition_error = PartitionError::Storage {
            source: storage_error,
        };
        let error = TopicError::from(partition_error);
        let partition_source = error
            .source()
            .expect("Expected a source for the Partition error")
            .downcast_ref::<PartitionError>()
            .expect("Expected source to be PartitionError");
        let storage_source = partition_source
            .source()
            .expect("Expected a nested source for the Partition error")
            .downcast_ref::<StorageError>()
            .expect("Expected nested source to be StorageError");

        assert!(
            matches!(partition_source, PartitionError::Storage { source } if matches!(source, StorageError::UnexpectedOffset { expected: 1, actual: 2 }))
        );
        assert!(matches!(
            storage_source,
            StorageError::UnexpectedOffset {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn topic_io_error_exposes_underlying_source() {
        let io_error = std::io::Error::other("oh no!");
        let error = TopicError::from(io_error);
        let source = error.source().expect("Expected a source for the I/O error");
        let io_source = source
            .downcast_ref::<std::io::Error>()
            .expect("Expected source to be std::io::Error");
        assert_eq!(io_source.kind(), std::io::ErrorKind::Other);
        assert_eq!(io_source.to_string(), "oh no!");
    }

    #[test]
    fn topic_storage_error_preserves_nested_source_chain() {
        let codec_error = CodecError::IncompleteBody;
        let path = PathBuf::from("data").join("segments").join("file.log");
        let byte_position = u64::MAX;
        let storage_error = StorageError::CorruptRecord {
            path: path.clone(),
            byte_position,
            source: codec_error,
        };
        let error = TopicError::from(storage_error);
        let source = error
            .source()
            .expect("Expected a source for the Storage error");
        let storage_source = source
            .downcast_ref::<StorageError>()
            .expect("Expected source to be StorageError");
        let nested_source = storage_source
            .source()
            .expect("Expected a nested source for the Storage error");
        let codec_source = nested_source
            .downcast_ref::<CodecError>()
            .expect("Expected nested source to be CodecError");
        assert!(
            matches!(storage_source, StorageError::CorruptRecord { path: path_buf, byte_position: byte_pos, source: _ } if path_buf == &path && *byte_pos == byte_position)
        );
        assert!(matches!(codec_source, CodecError::IncompleteBody));
    }

    #[test]
    fn storage_codec_conversion_preserves_typed_source() {
        let error = StorageError::from(CodecError::InvalidChecksum);
        assert!(matches!(
            &error,
            StorageError::Codec(CodecError::InvalidChecksum)
        ));
        let source = error.source().expect("codec cause should be retained");
        assert_eq!(
            source.downcast_ref::<CodecError>(),
            Some(&CodecError::InvalidChecksum)
        );
        assert!(source.source().is_none());
        assert!(error.to_string().contains(&source.to_string()));
    }

    #[test]
    fn storage_io_conversion_preserves_kind_message_and_source() {
        let error = StorageError::from(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot open segment",
        ));
        assert!(matches!(&error, StorageError::Io(inner)
            if inner.kind() == std::io::ErrorKind::PermissionDenied));
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(source.to_string(), "cannot open segment");
        assert!(error.to_string().contains("cannot open segment"));
    }

    #[test]
    fn corrupt_record_diagnostic_includes_location_and_cause() {
        let path = PathBuf::from("data")
            .join("orders")
            .join("0")
            .join("00000000000000000000.log");
        let error = StorageError::CorruptRecord {
            path: path.clone(),
            byte_position: 137,
            source: CodecError::InvalidChecksum,
        };
        let message = error.to_string();
        assert!(message.contains(path.to_str().unwrap()));
        assert!(message.contains("byte 137"));
        assert!(message.contains("invalid checksum"));
        assert_eq!(
            error.source().unwrap().downcast_ref::<CodecError>(),
            Some(&CodecError::InvalidChecksum)
        );
    }

    #[test]
    fn rotation_after_commit_retains_offsets_and_nested_io_cause() {
        let error = StorageError::RotationAfterCommit {
            committed_offset: 41,
            next_offset: 42,
            source: Box::new(StorageError::from(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "rotation destination exists",
            ))),
        };
        assert!(matches!(&error, StorageError::RotationAfterCommit {
            committed_offset: 41, next_offset: 42, source,
        } if matches!(source.as_ref(), StorageError::Io(_))));
        let message = error.to_string();
        assert!(message.contains("committed offset 41"));
        assert!(message.contains("next offset 42"));
        assert!(message.contains("rotation destination exists"));
        let storage_source = error.source().expect("rotation cause should be retained");
        let io_source = storage_source
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(io_source.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(io_source.to_string(), "rotation destination exists");
    }

    #[test]
    fn storage_layout_diagnostics_include_context_without_sources() {
        let first = PathBuf::from("first.log");
        let second = PathBuf::from("second.log");
        let cases = [
            (
                StorageError::InvalidSegmentFilename {
                    path: first.clone(),
                },
                vec!["first.log"],
            ),
            (
                StorageError::UnexpectedSegmentEntry {
                    path: first.clone(),
                },
                vec!["first.log"],
            ),
            (
                StorageError::DuplicateSegmentBaseOffset {
                    base_offset: 23,
                    first_path: first.clone(),
                    second_path: second,
                },
                vec!["23", "first.log", "second.log"],
            ),
            (
                StorageError::UnexpectedSegmentBaseOffset {
                    path: first.clone(),
                    expected: 23,
                    actual: 47,
                },
                vec!["first.log", "expected 23", "actual 47"],
            ),
            (
                StorageError::EmptyClosedSegment {
                    path: first,
                    base_offset: 23,
                },
                vec!["first.log", "23"],
            ),
            (
                StorageError::UnexpectedOffset {
                    expected: 23,
                    actual: 47,
                },
                vec!["expected 23", "actual 47"],
            ),
            (StorageError::AppendDisabled, vec!["append disabled"]),
            (StorageError::OffsetOverflow, vec!["offset overflow"]),
        ];
        for (error, details) in cases {
            assert!(error.source().is_none(), "unexpected source for {error:?}");
            for detail in details {
                assert!(
                    error.to_string().contains(detail),
                    "missing {detail:?} in {error}"
                );
            }
        }
    }

    #[test]
    fn validation_diagnostics_retain_rejected_values_without_sources() {
        let config = super::ConfigurationError::InvalidSegmentBytes { value: 0 };
        assert!(config.to_string().contains('0'));
        assert!(config.source().is_none());
        let length = super::TopicNameError::InvalidLength { actual: 129 };
        assert!(length.to_string().contains("129"));
        assert!(length.source().is_none());
        let character = super::TopicNameError::InvalidCharacter { character: '/' };
        assert!(character.to_string().contains('/'));
        assert!(character.source().is_none());
    }

    #[test]
    fn topic_diagnostics_identify_operation_failure_and_wrapped_cause() {
        let cases = [
            (
                TopicError::AlreadyExists {
                    name: TopicName::new("orders".into()).unwrap(),
                },
                vec!["orders", "already exists"],
            ),
            (
                TopicError::NotFound {
                    name: TopicName::new("payments".into()).unwrap(),
                },
                vec!["payments", "not found"],
            ),
            (
                TopicError::from(std::io::Error::other("disk failure")),
                vec!["disk failure"],
            ),
            (
                TopicError::from(StorageError::UnexpectedOffset {
                    expected: 23,
                    actual: 47,
                }),
                vec!["expected 23", "actual 47"],
            ),
        ];
        for (error, details) in cases {
            for detail in details {
                assert!(
                    error.to_string().contains(detail),
                    "missing {detail:?} in {error}"
                );
            }
        }
    }
}
