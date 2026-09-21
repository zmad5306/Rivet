use std::io::ErrorKind::AlreadyExists;
use std::path::Path;

use crate::broker::partition::Partition;
use crate::error::{TopicError, TopicNameError};
use crate::storage::record::{PublishInput, Record, RecordLimits};
use crate::storage::segment::SegmentConfig;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub(crate) struct TopicName {
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
pub(crate) struct Topic {
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
    ) -> Result<Topic, TopicError> {
        let topic_directory = data_root.join(name.as_str());

        match std::fs::create_dir(topic_directory.as_path()) {
            Ok(()) => {}
            Err(source) if source.kind() == AlreadyExists => {
                return Err(TopicError::AlreadyExists {
                    name: name.as_str().to_string(),
                });
            }
            Err(source) => return Err(TopicError::Io { source }),
        };

        let partition_directory = topic_directory.join("0");
        let partition = Partition::new(&partition_directory, config, limits)?;

        Ok(Topic { name, partition })
    }

    pub fn publish(&mut self, input: PublishInput) -> Result<u64, TopicError> {
        let offset = self.partition.publish(input)?;
        Ok(offset)
    }

    pub fn read(&self, partition_id: u32, offset: u64) -> Result<Option<Record>, TopicError> {
        validate_partition_id(partition_id)?;
        let record = self.partition.read(offset)?;
        Ok(record)
    }

    pub fn open(
        data_root: &Path,
        name: TopicName,
        config: SegmentConfig,
        limits: RecordLimits,
    ) -> Result<Topic, TopicError> {
        let topic_directory = data_root.join(name.as_str());
        let partition_directory = topic_directory.join("0");

        match std::fs::symlink_metadata(&partition_directory) {
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(TopicError::MissingPartitionDirectory {
                    path: partition_directory,
                });
            }
            Err(source) => return Err(TopicError::Io { source }),
        }

        let partition = Partition::new(&partition_directory, config, limits)?;

        Ok(Topic { name, partition })
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
    use crate::storage::record::{PublishInput, RecordLimits};
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
    fn opening_an_existing_topic_restores_its_name_and_partition_zero_records() {
        let orders = "orders";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let topic_name = TopicName::new(orders.to_string()).expect("failed to create TopicName");
        let key: Vec<u8> = vec![0, 255, 128];
        let payload: Vec<u8> = vec![1, 0, 254, 127];
        let publish_input = PublishInput::new(Some(key.clone()), payload.clone());
        let mut topic = Topic::create(
            data_root.path(),
            topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to create topic");
        let offset = topic
            .publish(publish_input)
            .expect("failed to publish record");

        drop(topic);

        let reopened_topic_name =
            TopicName::new(orders.to_string()).expect("failed to create TopicName");
        let reopened_topic = Topic::open(
            data_root.path(),
            reopened_topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open topic");

        assert_eq!(
            reopened_topic.name().as_str(),
            orders,
            "reopened topic name should match the original name"
        );

        let record = reopened_topic
            .read(0, offset)
            .expect("failed to read record")
            .expect("record should exist");

        assert_eq!(
            record.offset(),
            offset,
            "restored record should have the same offset"
        );
        assert_eq!(
            record.key().expect("record should have a key"),
            &key,
            "restored record should have the same key"
        );
        assert_eq!(
            record.payload(),
            &payload,
            "restored record should have the same payload"
        );
    }

    #[test]
    fn opening_a_topic_without_partition_zero_returns_an_error_without_creating_it() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let orders_directory = data_root.path().join("orders");
        std::fs::create_dir(&orders_directory).expect("failed to create orders directory");
        let partition_zero_directory = orders_directory.join("0");
        assert!(
            !partition_zero_directory.exists(),
            "partition zero directory should not exist"
        );
        let topic_name = TopicName::new("orders".to_string()).expect("failed to create TopicName");
        let error = Topic::open(
            data_root.path(),
            topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected error when opening topic without partition zero");

        assert!(
            matches!(error, TopicError::MissingPartitionDirectory { path } if path == partition_zero_directory)
        );
        assert!(
            !partition_zero_directory.exists(),
            "partition zero directory should still not exist"
        );
    }

    #[test]
    fn duplicate_topic_creation_returns_already_exists() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let topic_name_str = "orders";
        let topic_name =
            TopicName::new(topic_name_str.to_string()).expect("failed to create TopicName");
        let duplicate_topic_name =
            TopicName::new(topic_name_str.to_string()).expect("failed to create TopicName");
        Topic::create(
            data_root.path(),
            topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to create topic");

        let duplicate_topic_result = Topic::create(
            data_root.path(),
            duplicate_topic_name,
            SegmentConfig::default(),
            RecordLimits::default(),
        );

        assert!(
            matches!(duplicate_topic_result, Err(TopicError::AlreadyExists { name }) if name.as_str() == topic_name_str),
            "duplicate topic creation should return AlreadyExists error with the correct topic name"
        );

        let topic_directory = data_root.path().join(topic_name_str);
        let partition_zero_directory = topic_directory.join("0");

        assert!(
            partition_zero_directory.is_dir(),
            "original partition zero directory should still exist"
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
