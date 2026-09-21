pub mod partition;
pub mod topic;

use self::topic::Topic;
use crate::broker::topic::{TopicName, validate_partition_count};
use crate::error::TopicError;
use crate::storage::record::PublishInput;
use crate::storage::{record::RecordLimits, segment::SegmentConfig};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub struct PublishResult {
    topic: String,
    partition: u32,
    offset: u64,
}

impl PublishResult {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[derive(Debug)]
pub struct Broker {
    data_root: PathBuf,
    topics: BTreeMap<String, Topic>,
    segment_config: SegmentConfig,
    record_limits: RecordLimits,
}

impl Broker {
    pub fn new(
        data_root: PathBuf,
        segment_config: SegmentConfig,
        record_limits: RecordLimits,
    ) -> Self {
        Self {
            data_root,
            topics: BTreeMap::new(),
            segment_config,
            record_limits,
        }
    }

    pub fn create_topic(&mut self, name: String, partition_count: u32) -> Result<(), TopicError> {
        validate_partition_count(partition_count)?;
        let topic_name = TopicName::new(name.clone())?;
        let topic = Topic::create(
            &self.data_root,
            topic_name,
            self.segment_config,
            self.record_limits,
        )?;

        self.topics.insert(name, topic);

        Ok(())
    }

    pub fn list_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|k| k.as_str()).collect()
    }

    pub fn publish(
        &mut self,
        topic: &str,
        input: PublishInput,
    ) -> Result<PublishResult, TopicError> {
        if let Some(topic) = self.topics.get_mut(topic) {
            let offset = topic.publish(input)?;
            Ok(PublishResult {
                topic: topic.name().as_str().to_string(),
                partition: 0,
                offset,
            })
        } else {
            Err(TopicError::NotFound {
                name: TopicName::new(topic.to_string())?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::broker::Broker;
    use crate::error::{TopicError, TopicNameError};
    use crate::storage::record::{PublishInput, RecordLimits};
    use crate::storage::segment::SegmentConfig;

    #[test]
    fn create_topic_rejects_unsupported_partition_count_before_creating_directory() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );
        let name = "orders".to_string();
        let error = broker
            .create_topic(name.clone(), 2)
            .expect_err("expected unsupported partition count error");

        assert!(
            matches!(
                error,
                TopicError::UnsupportedPartitionCount { requested: 2 }
            ),
            "expected unsupported partition count error"
        );
        assert!(
            !data_root.path().join(&name).exists(),
            "expected topic directory to not exist"
        );
    }

    #[test]
    fn create_topic_adds_the_initialized_topic_to_the_catalog() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );
        let name = "orders".to_string();

        broker
            .create_topic(name.clone(), 1)
            .expect("failed to create topic");

        let topics = broker.list_topics();
        assert_eq!(topics, vec![name.as_str()]);

        assert!(data_root.path().join(&name).join("0").is_dir());
    }

    #[test]
    fn list_topics_is_empty_then_returns_multiple_topics_in_sorted_order() {
        let topic_name_1 = "zebra";
        let topic_name_2 = "alpha";
        let topic_name_3 = "middle";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );

        assert!(broker.list_topics().is_empty());

        broker
            .create_topic(topic_name_1.to_string(), 1)
            .expect("failed to create topic");
        broker
            .create_topic(topic_name_2.to_string(), 1)
            .expect("failed to create topic");
        broker
            .create_topic(topic_name_3.to_string(), 1)
            .expect("failed to create topic");

        let topics = broker.list_topics();
        assert_eq!(topics, vec![topic_name_2, topic_name_3, topic_name_1]);
    }

    #[test]
    fn create_topic_rejects_an_invalid_raw_name_before_mutating_state() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );
        let error = broker
            .create_topic("bad/name".to_string(), 1)
            .expect_err("expected an error for invalid topic name");

        assert!(
            matches!(error, TopicError::InvalidTopicName { .. }),
            "expected an invalid topic name error"
        );

        let source = error
            .source()
            .expect("expected a source error for invalid topic name")
            .downcast_ref::<TopicNameError>()
            .expect("expected a TopicNameError");

        assert!(
            matches!(source, TopicNameError::InvalidCharacter { character: '/' }),
            "expected an invalid character error for '/'"
        );

        assert!(broker.list_topics().is_empty());
        assert!(!data_root.path().join("bad").exists());
    }

    #[test]
    fn create_topic_rejects_an_existing_file_without_exposing_a_catalog_entry() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );

        let orders_path = data_root.path().join("orders");
        std::fs::write(&orders_path, b"sentinel bytes").expect("failed to create sentinel file");

        let error = broker
            .create_topic("orders".to_string(), 1)
            .expect_err("expected an error for existing file");

        assert!(
            matches!(error, TopicError::AlreadyExists { name } if name.as_str() == "orders"),
            "expected an already exists error"
        );

        assert!(broker.list_topics().is_empty());

        let contents = std::fs::read(&orders_path).expect("failed to read sentinel file");

        assert_eq!(contents, b"sentinel bytes");
    }

    #[test]
    fn publish_to_a_missing_topic_returns_not_found_without_creating_state() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );
        let input = PublishInput::new(None, vec![1, 2, 3]);
        let error = broker
            .publish("missing", input)
            .expect_err("expected an error for missing topic");

        assert!(
            matches!(error, TopicError::NotFound { name } if name.as_str() == "missing"),
            "expected a not found error for the missing topic"
        );
        assert!(broker.list_topics().is_empty());
        assert!(!data_root.path().join("missing").exists());
    }

    #[test]
    fn publish_to_a_known_topic_reports_topic_partition_and_offset() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::new(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        );

        broker
            .create_topic("orders".to_string(), 1)
            .expect("failed to create topic");

        let input = PublishInput::new(Some(vec![0]), vec![1, 2, 3]);
        let result = broker
            .publish("orders", input)
            .expect("failed to publish to topic");

        assert_eq!(result.topic(), "orders");
        assert_eq!(result.partition(), 0);
        assert_eq!(result.offset(), 0);
    }

    #[test]
    fn read_from_a_missing_topic_returns_not_found_without_creating_state() {
        // TODO: Create a temporary data root and construct a Broker with default
        //       segment configuration and record limits.
        // TODO: Call Broker::read for topic "missing", partition 0, and offset 0,
        //       then capture the returned error.
        // TODO: Verify the error is TopicError::NotFound and its TopicName has the
        //       spelling "missing".
        // TODO: Verify list_topics remains empty and data_root/missing does not exist,
        //       proving read did not implicitly create the topic.
        todo!()
    }
}
