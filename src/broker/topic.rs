use std::io::ErrorKind::AlreadyExists;
use std::path::Path;

use crate::broker::partition::Partition;
use crate::error::{TopicError, TopicNameError};
use crate::storage::record::RecordLimits;
use crate::storage::segment::SegmentConfig;

#[derive(Debug, PartialEq, Eq)]
pub struct TopicName {
    value: String,
}

impl TopicName {
    pub fn new(value: String) -> Result<Self, TopicNameError> {
        if value.is_empty() || value.len() > 128 {
            return Err(TopicNameError::InvalidLength {
                actual: value.len(),
            });
        }
        if let Some(character) = value
            .chars()
            .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_')
        {
            return Err(TopicNameError::InvalidCharacter { character });
        }
        Ok(Self { value })
    }
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug)]
pub struct Topic {
    name: TopicName,
    partition: Partition,
}

impl Topic {
    pub fn name(&self) -> &TopicName {
        &self.name
    }

    pub fn create(
        data_root: &Path,
        name: TopicName,
        config: SegmentConfig,
        limits: RecordLimits,
    ) -> Result<Self, TopicError> {
        let topic_directory = data_root.join(name.as_str());

        match std::fs::create_dir(topic_directory.as_path()) {
            Ok(()) => {}
            Err(source) if source.kind() == AlreadyExists => {
                return Err(TopicError::AlreadyExists { name });
            }
            Err(source) => return Err(TopicError::from(source)),
        };

        let partition_directory = topic_directory.join("0");
        let partition = Partition::new(&partition_directory, config, limits)?;

        Ok(Self { name, partition })
    }
}

pub(crate) fn validate_partition_id(requested: u32) -> Result<(), TopicError> {
    if requested == 0 {
        return Ok(());
    }
    Err(TopicError::UnsupportedPartitionId { requested })
}

pub(crate) fn validate_partition_count(requested: u32) -> Result<(), TopicError> {
    if requested == 1 {
        return Ok(());
    }
    Err(TopicError::UnsupportedPartitionCount { requested })
}

#[cfg(test)]
mod tests {

    use crate::broker::topic::{Topic, TopicName};
    use crate::error::{TopicError, TopicNameError};
    use crate::storage::record::RecordLimits;
    use crate::storage::segment::SegmentConfig;

    #[test]
    fn creating_topic_initializes_partition_zero_under_topic_directory() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let topic_name_str = "orders";
        let topic_name =
            TopicName::new(topic_name_str.to_string()).expect("failed to create TopicName");
        let topic = Topic::create(
            data_root.path(),
            topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to create topic");

        let topic_directory = data_root.path().join(topic_name_str);
        let partition_zero_directory = topic_directory.join("0");

        assert_eq!(
            topic.name().as_str(),
            topic_name_str,
            "topic name should match the requested name"
        );
        assert!(
            partition_zero_directory.is_dir(),
            "partition zero directory should exist"
        );
    }

    #[test]
    fn partition_id_zero_is_accepted() {
        let result = super::validate_partition_id(0);
        assert!(result.is_ok(), "partition ID 0 should be accepted");
    }

    #[test]
    fn nonzero_partition_ids_are_rejected() {
        for partition_id in [1, u32::MAX] {
            let error = super::validate_partition_id(partition_id)
                .expect_err("nonzero partition IDs should be rejected");
            assert!(
                matches!(error, TopicError::UnsupportedPartitionId { requested: actual } if partition_id == actual),
                "error should report the rejected partition ID"
            );
        }
    }

    #[test]
    fn partition_count_one_is_accepted() {
        let result = super::validate_partition_count(1);
        assert!(result.is_ok(), "partition count 1 should be accepted");
    }

    #[test]
    fn unsupported_partition_counts_are_rejected() {
        for partition_count in [0, 2, u32::MAX] {
            let error = super::validate_partition_count(partition_count)
                .expect_err("unsupported partition counts should be rejected");
            assert!(
                matches!(error, TopicError::UnsupportedPartitionCount { requested: actual } if partition_count == actual),
                "error should report the rejected partition count"
            );
        }
    }

    #[test]
    fn valid_topic_names_preserve_spelling() {
        let long_name = "a".repeat(128);
        let valid_names = vec![
            "a",
            "Z",
            "0",
            "9",
            "-",
            "_",
            "abc-123_DEF",
            "1234567890",
            long_name.as_str(),
        ];
        let mut number_of_assertions = 0;
        for name in valid_names {
            let topic_name = super::TopicName::new(name.to_string()).unwrap();
            assert_eq!(
                topic_name.as_str(),
                name,
                "topic name spelling should be preserved"
            );
            number_of_assertions += 1;
        }
        assert_eq!(
            number_of_assertions, 9,
            "all valid topic names should be checked"
        );
    }

    #[test]
    fn topic_name_accepts_length_boundaries() {
        let mut number_of_assertions = 0;
        for i in 1..=128 {
            let name = "a".repeat(i);
            let topic_name = super::TopicName::new(name.clone()).unwrap();
            assert_eq!(
                topic_name.as_str(),
                name,
                "topic name should preserve its spelling"
            );
            number_of_assertions += 1;
        }
        assert_eq!(
            number_of_assertions, 128,
            "every valid topic-name length should be checked"
        );
    }

    #[test]
    fn topic_name_rejects_empty_name() {
        let result = super::TopicName::new("".to_string());
        assert!(
            matches!(result, Err(TopicNameError::InvalidLength { actual: 0 })),
            "an empty topic name should be rejected"
        );
    }

    #[test]
    fn topic_name_rejects_name_longer_than_limit() {
        let name = "a".repeat(129);
        let error = super::TopicName::new(name)
            .expect_err("a topic name longer than 128 characters should be rejected");
        assert!(
            matches!(error, TopicNameError::InvalidLength { actual: 129 }),
            "the error should report the actual topic-name length"
        );
    }

    #[test]
    fn topic_name_rejects_invalid_characters() {
        let invalid_topic_names = vec![
            "ü",  // Unicode
            " ",  // Space
            "\t", // Tab
            "\n", // Newline
            "!",  // Punctuation
            "\0", // NUL
        ];
        let mut number_of_assertions = 0;
        for name in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == name.chars().next().unwrap()),
                "invalid topic name should report its invalid character"
            );
            number_of_assertions += 1;
        }
        assert_eq!(
            number_of_assertions, 6,
            "all invalid characters should be checked"
        );
    }

    #[test]
    fn topic_name_rejects_paths_and_traversal() {
        let invalid_topic_names = vec![".", "..", "/", "\\", "/etc/passwd", "../traversal"];
        let mut number_of_assertions = 0;
        for name in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == name.chars().next().unwrap()),
                "path-like topic names should be rejected with their invalid character"
            );
            number_of_assertions += 1;
        }
        assert_eq!(
            number_of_assertions, 6,
            "all path-like topic names should be checked"
        );
    }

    #[test]
    fn topic_name_reports_first_invalid_character() {
        let invalid_topic_names = vec![["valid_prefixü!", "ü"], ["valid_prefix !", " "]];
        let mut number_of_assertions = 0;
        for [name, first_invalid_char] in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == first_invalid_char.chars().next().expect("invalid-character fixture should not be empty")),
                "the error should report the first invalid character"
            );
            number_of_assertions += 1;
        }
        assert_eq!(
            number_of_assertions, 2,
            "all first-invalid-character cases should be checked"
        );
    }
}
