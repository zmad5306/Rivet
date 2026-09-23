pub mod partition;
pub mod topic;

use self::topic::Topic;
use crate::broker::topic::{TopicName, validate_partition_count};
use crate::consumer::OFFSET_STORE_DIR;
use crate::error::{CatalogEntryErrorReason, TopicError};
use crate::storage::record::{PublishInput, Record};
use crate::storage::{record::RecordLimits, segment::SegmentConfig};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub struct PublishResult {
    topic: TopicName,
    partition: u32,
    offset: u64,
}

impl PublishResult {
    pub fn topic(&self) -> &str {
        self.topic.as_str()
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
    topics: BTreeMap<TopicName, Topic>,
    segment_config: SegmentConfig,
    record_limits: RecordLimits,
}

impl Broker {
    pub fn open(
        data_root: PathBuf,
        segment_config: SegmentConfig,
        record_limits: RecordLimits,
    ) -> Result<Self, TopicError> {
        std::fs::create_dir_all(&data_root)?;
        let mut topics = BTreeMap::new();
        for read_result in std::fs::read_dir(&data_root)? {
            let entry = read_result?;
            let file_name = entry.file_name();

            if file_name == OFFSET_STORE_DIR {
                continue;
            }

            if entry.file_type()?.is_symlink() {
                return Err(TopicError::UnexpectedCatalogEntry {
                    path: entry.path(),
                    reason: CatalogEntryErrorReason::SymbolicLink,
                });
            }

            if !entry.file_type()?.is_dir() {
                return Err(TopicError::UnexpectedCatalogEntry {
                    path: entry.path(),
                    reason: CatalogEntryErrorReason::NotDirectory,
                });
            }

            let file_name =
                file_name
                    .into_string()
                    .map_err(|_| TopicError::UnexpectedCatalogEntry {
                        path: entry.path(),
                        reason: CatalogEntryErrorReason::InvalidTopicName,
                    })?;

            let topic_name =
                TopicName::new(file_name).map_err(|_| TopicError::UnexpectedCatalogEntry {
                    path: entry.path(),
                    reason: CatalogEntryErrorReason::InvalidTopicName,
                })?;

            let topic = Topic::open(
                &data_root,
                topic_name.clone(),
                segment_config,
                record_limits,
            )?;

            topics.insert(topic_name, topic);
        }

        Ok(Self {
            data_root,
            topics,
            segment_config,
            record_limits,
        })
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

        self.topics.insert(topic.name().clone(), topic);

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
        let topic_name = TopicName::new(topic.to_string())?;
        if let Some(topic) = self.topics.get_mut(&topic_name) {
            let offset = topic.publish(input)?;
            Ok(PublishResult {
                topic: topic.name().clone(),
                partition: 0,
                offset,
            })
        } else {
            Err(TopicError::NotFound {
                name: topic_name.as_str().to_string(),
            })
        }
    }

    pub fn read(
        &self,
        topic: &str,
        partition_id: u32,
        offset: u64,
    ) -> Result<Option<Record>, TopicError> {
        let topic_name = TopicName::new(topic.to_string())?;
        if let Some(topic) = self.topics.get(&topic_name) {
            Ok(topic.read(partition_id, offset)?)
        } else {
            Err(TopicError::NotFound {
                name: topic_name.as_str().to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::Write;

    use crate::broker::Broker;
    use crate::error::{
        CatalogEntryErrorReason, CodecError, PartitionError, StorageError, TopicError,
        TopicNameError,
    };
    use crate::storage::record::{PublishInput, RecordLimits};
    use crate::storage::segment::SegmentConfig;

    #[test]
    fn create_topic_rejects_unsupported_partition_count_before_creating_directory() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
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
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        let name = "orders".to_string();

        broker
            .create_topic(name.clone(), 1)
            .expect("failed to create topic");

        let topics = broker.list_topics();

        assert_eq!(
            topics,
            vec![name.as_str()],
            "expected the created topic to be listed"
        );
        assert!(
            data_root.path().join(&name).join("0").is_dir(),
            "expected the partition directory to exist"
        );
    }

    #[test]
    fn list_topics_is_empty_then_returns_multiple_topics_in_sorted_order() {
        let topic_name_1 = "zebra";
        let topic_name_2 = "alpha";
        let topic_name_3 = "middle";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed initially"
        );

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
        assert_eq!(
            topics,
            vec![topic_name_2, topic_name_3, topic_name_1],
            "expected topics to be listed in sorted order"
        );
    }

    #[test]
    fn opening_a_broker_discovers_an_existing_topic_and_restores_its_record() {
        let key = b"key".to_vec();
        let payload = b"payload".to_vec();
        let input = PublishInput::new(Some(key.clone()), payload.clone());
        let orders = "orders";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create topic");
        let result = broker
            .publish(orders, input)
            .expect("failed to publish record");

        drop(broker);

        let broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        assert_eq!(
            broker.list_topics(),
            vec![orders],
            "expected the reopened broker to list exactly the 'orders' topic"
        );

        let record = broker
            .read(orders, result.partition(), result.offset())
            .expect("failed to read record")
            .expect("expected the record to exist");

        assert_eq!(
            record.key().expect("expected the record to have a key"),
            &key
        );
        assert_eq!(record.payload(), &payload);
        assert_eq!(record.offset(), result.offset());
    }

    #[test]
    fn reopening_a_broker_restores_multiple_topics_and_continues_independent_offsets() {
        let orders = "orders";
        let payments = "payments";
        let order1_key = b"order1_key".to_vec();
        let order2_key = b"order2_key".to_vec();
        let order3_key = b"order3_key".to_vec();
        let payment1_key = b"payment1_key".to_vec();
        let payment2_key = b"payment2_key".to_vec();
        let order1_payload = b"order1_payload".to_vec();
        let order2_payload = b"order2_payload".to_vec();
        let order3_payload = b"order3_payload".to_vec();
        let payment1_payload = b"payment1_payload".to_vec();
        let payment2_payload = b"payment2_payload".to_vec();
        let order1_input = PublishInput::new(Some(order1_key.clone()), order1_payload.clone());
        let order2_input = PublishInput::new(Some(order2_key.clone()), order2_payload.clone());
        let order3_input = PublishInput::new(Some(order3_key.clone()), order3_payload.clone());
        let payment1_input =
            PublishInput::new(Some(payment1_key.clone()), payment1_payload.clone());
        let payment2_input =
            PublishInput::new(Some(payment2_key.clone()), payment2_payload.clone());
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create 'orders' topic");
        broker
            .create_topic(payments.to_string(), 1)
            .expect("failed to create 'payments' topic");

        let order1_result = broker
            .publish(orders, order1_input)
            .expect("failed to publish order1");
        let order2_result = broker
            .publish(orders, order2_input)
            .expect("failed to publish order2");
        let payment1_result = broker
            .publish(payments, payment1_input)
            .expect("failed to publish payment1");

        assert_eq!(order1_result.offset(), 0, "order1 offset should be 0");
        assert_eq!(order2_result.offset(), 1, "order2 offset should be 1");
        assert_eq!(payment1_result.offset(), 0, "payment1 offset should be 0");

        drop(broker);

        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to reopen broker");

        let topics = broker.list_topics();
        assert_eq!(topics, vec![orders.to_string(), payments.to_string()]);

        let order1_record = broker
            .read(orders, 0, 0)
            .expect("failed to read order1 record")
            .expect("expected order1 record to exist");
        let order2_record = broker
            .read(orders, 0, 1)
            .expect("failed to read order2 record")
            .expect("expected order2 record to exist");
        let payment1_record = broker
            .read(payments, 0, 0)
            .expect("failed to read payment1 record")
            .expect("expected payment1 record to exist");

        assert_eq!(
            order1_record.key().expect("failed to get order1 key"),
            order1_key,
            "order1 key mismatch"
        );
        assert_eq!(
            order1_record.payload(),
            order1_payload,
            "order1 payload mismatch"
        );
        assert_eq!(
            order1_record.offset(),
            order1_result.offset(),
            "order1 offset mismatch"
        );

        assert_eq!(
            order2_record.key().expect("failed to get order2 key"),
            order2_key,
            "order2 key mismatch"
        );
        assert_eq!(
            order2_record.payload(),
            order2_payload,
            "order2 payload mismatch"
        );
        assert_eq!(
            order2_record.offset(),
            order2_result.offset(),
            "order2 offset mismatch"
        );

        assert_eq!(
            payment1_record.key().expect("failed to get payment1 key"),
            payment1_key,
            "payment1 key mismatch"
        );
        assert_eq!(
            payment1_record.payload(),
            payment1_payload,
            "payment1 payload mismatch"
        );
        assert_eq!(
            payment1_record.offset(),
            payment1_result.offset(),
            "payment1 offset mismatch"
        );

        let order3_result = broker
            .publish(orders, order3_input)
            .expect("failed to publish order3");
        let payment2_result = broker
            .publish(payments, payment2_input)
            .expect("failed to publish payment2");

        assert_eq!(order3_result.offset(), 2, "order3 offset should be 2");
        assert_eq!(payment2_result.offset(), 1, "payment2 offset should be 1");

        let order3_record = broker
            .read(orders, 0, 2)
            .expect("failed to read order3 record")
            .expect("expected order3 record to exist");
        let payment2_record = broker
            .read(payments, 0, 1)
            .expect("failed to read payment2 record")
            .expect("expected payment2 record to exist");

        assert_eq!(
            order3_record.key().expect("failed to get order3 key"),
            order3_key,
            "order3 key mismatch"
        );
        assert_eq!(
            order3_record.payload(),
            order3_payload,
            "order3 payload mismatch"
        );
        assert_eq!(
            order3_record.offset(),
            order3_result.offset(),
            "order3 offset mismatch"
        );

        assert_eq!(
            payment2_record.key().expect("failed to get payment2 key"),
            payment2_key,
            "payment2 key mismatch"
        );
        assert_eq!(
            payment2_record.payload(),
            payment2_payload,
            "payment2 payload mismatch"
        );
        assert_eq!(
            payment2_record.offset(),
            payment2_result.offset(),
            "payment2 offset mismatch"
        );
    }

    #[test]
    fn repeated_reopening_preserves_an_empty_topic_and_its_first_offset() {
        let orders = "orders";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create orders topic");

        drop(broker);

        let broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to reopen broker");

        assert_eq!(
            broker.list_topics(),
            vec![orders.to_string()],
            "expected the reopened broker to list exactly the 'orders' topic"
        );
        assert!(
            broker
                .read(orders, 0, 0)
                .expect("expected to read partition 0 at offset 0 after the first reopen")
                .is_none(),
            "expected reading partition 0 at offset 0 to return None"
        );

        drop(broker);

        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to reopen broker");

        assert_eq!(
            broker.list_topics(),
            vec![orders.to_string()],
            "expected the second reopened broker to list exactly the 'orders' topic"
        );
        assert!(
            broker
                .read(orders, 0, 0)
                .expect("expected to read partition 0 at offset 0 after the second reopen")
                .is_none(),
            "expected reading partition 0 at offset 0 to return None after the second reopen"
        );

        let order1_key = b"order1".to_vec();
        let order1_payload = b"order1_payload".to_vec();
        let order1_input = PublishInput::new(Some(order1_key.clone()), order1_payload.clone());

        let order1_result = broker
            .publish(orders, order1_input)
            .expect("failed to publish order1");

        assert_eq!(
            order1_result.offset(),
            0,
            "expected the first published order to have offset 0"
        );

        let order1_record = broker
            .read(orders, 0, 0)
            .expect("expected to read the first published order")
            .expect("expected a record at offset 0");

        assert_eq!(
            order1_record
                .key()
                .expect("expected a key for the first published order"),
            order1_key
        );
        assert_eq!(order1_record.payload(), &order1_payload);
        assert_eq!(
            order1_record.offset(),
            0,
            "expected the first published order to have offset 0"
        );
    }

    #[test]
    fn broker_startup_recovers_an_incomplete_active_tail_without_losing_valid_records() {
        let key = b"key".to_vec();
        let payload = b"payload".to_vec();
        let order0 = PublishInput::new(Some(key.clone()), payload.clone());
        let order1 = PublishInput::new(Some(key.clone()), payload.clone());
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic("orders".to_string(), 1)
            .expect("failed to create topic");

        let order0_result = broker
            .publish("orders", order0)
            .expect("failed to publish record");

        drop(broker);

        let segment_byte_len =
            std::fs::metadata(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to get segment metadata")
                .len();

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(data_root.path().join("orders/0/00000000000000000000.log"))
            .expect("failed to open segment for appending");

        file.write_all(&[0])
            .expect("failed to append a single byte to the segment");

        drop(file);

        assert_eq!(
            std::fs::metadata(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to get segment metadata")
                .len(),
            segment_byte_len + 1,
            "expected the segment to be one byte longer after appending"
        );

        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker after appending incomplete byte");

        assert_eq!(
            std::fs::metadata(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to get segment metadata")
                .len(),
            segment_byte_len,
            "expected the segment to be truncated back to its original length after recovery"
        );

        let record = broker
            .read("orders", order0_result.partition(), order0_result.offset())
            .expect("failed to read the first record")
            .expect("expected the first record to exist");

        assert_eq!(
            record.offset(),
            order0_result.offset(),
            "expected the record offset to match the original offset"
        );
        assert_eq!(
            record.key().expect("expected the record to have a key"),
            key,
            "expected the record key to match the original key"
        );
        assert_eq!(
            record.payload(),
            payload,
            "expected the record payload to match the original payload"
        );

        let order1_result = broker
            .publish("orders", order1)
            .expect("failed to publish the second record");

        assert_eq!(
            order1_result.offset(),
            1,
            "expected the second record to have offset 1"
        );
    }

    #[test]
    fn broker_startup_propagates_fatal_record_corruption_without_modifying_the_segment() {
        let orders = "orders";
        let key = vec![1, 2, 3];
        let payload = vec![4, 5, 6, 7];
        let order0_input = PublishInput::new(Some(key.clone()), payload.clone());
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create topic");
        let order0_result = broker
            .publish(orders, order0_input)
            .expect("failed to publish the first record");

        assert_eq!(
            order0_result.offset(),
            0,
            "expected the first record to have offset 0"
        );

        drop(broker);

        let mut file_bytes =
            std::fs::read(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to read the segment file");

        file_bytes[33] ^= 0xFF; // Flip the first byte of the payload
        std::fs::write(
            data_root.path().join("orders/0/00000000000000000000.log"),
            &file_bytes,
        )
        .expect("failed to write the modified segment file");

        let corrupted_file_bytes =
            std::fs::read(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to read the corrupted segment file");

        let topic_error = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error due to corrupted record");

        let partition_error = topic_error
            .source()
            .expect("expected a source error for the topic error")
            .downcast_ref::<PartitionError>()
            .expect("expected a PartitionError");

        let storage_error = partition_error
            .source()
            .expect("expected a source error for the partition error")
            .downcast_ref::<StorageError>()
            .expect("expected a StorageError");

        assert!(
            matches!(storage_error, StorageError::CorruptRecord { byte_position, .. } if byte_position == &u64::try_from(0).expect("failed to convert byte position to u64")),
            "expected a corrupt record error at byte position 0"
        );

        let codec_error = storage_error
            .source()
            .expect("expected a source error for the storage error")
            .downcast_ref::<CodecError>()
            .expect("expected a CodecError");

        assert!(
            matches!(codec_error, CodecError::InvalidChecksum),
            "expected an invalid checksum error for the codec error"
        );

        assert_eq!(
            std::fs::read(data_root.path().join("orders/0/00000000000000000000.log"))
                .expect("failed to read segment file"),
            corrupted_file_bytes,
            "expected the segment file to match the corrupt snapshot"
        );
    }

    #[test]
    fn create_topic_rejects_an_invalid_raw_name_before_mutating_state() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
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

        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed after invalid topic creation"
        );
        assert!(
            !data_root.path().join("bad").exists(),
            "expected the invalid topic directory not to exist"
        );
    }

    #[test]
    fn create_topic_rejects_an_existing_file_without_exposing_a_catalog_entry() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        let orders_path = data_root.path().join("orders");
        std::fs::write(&orders_path, b"sentinel bytes").expect("failed to create sentinel file");

        let error = broker
            .create_topic("orders".to_string(), 1)
            .expect_err("expected an error for existing file");

        assert!(
            matches!(error, TopicError::AlreadyExists { name } if name.as_str() == "orders"),
            "expected an already exists error"
        );

        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed after failing to create a topic due to an existing file"
        );

        let contents = std::fs::read(&orders_path).expect("failed to read sentinel file");

        assert_eq!(
            contents, b"sentinel bytes",
            "expected the sentinel file contents to remain unchanged"
        );
    }

    #[test]
    fn opening_a_broker_rejects_a_file_data_root_without_overwriting_it() {
        let parent_dir = tempfile::tempdir().expect("failed to create temporary parent directory");
        let data_root = parent_dir.path().join("data_root_file");
        std::fs::write(&data_root, b"sentinel bytes").expect("failed to create sentinel file");
        let error = Broker::open(
            data_root.clone(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error for a file data root");

        assert!(
            matches!(error, TopicError::Io { .. }),
            "expected an I/O error for a file data root"
        );

        let contents = std::fs::read(&data_root).expect("failed to read sentinel file");
        assert_eq!(
            contents, b"sentinel bytes",
            "expected the sentinel file contents to remain unchanged"
        );
    }

    #[test]
    fn duplicate_topic_creation_preserves_the_original_catalog_entry_and_record() {
        let orders = "orders";
        let key = vec![0, 255];
        let payload = vec![1, 0, 254];
        let publish_input = PublishInput::new(Some(key.clone()), payload.clone());
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create topic");

        let publish_result = broker
            .publish(orders, publish_input)
            .expect("failed to publish the first record");

        let error = broker
            .create_topic(orders.to_string(), 1)
            .expect_err("expected an already exists error");

        assert!(
            matches!(error, TopicError::AlreadyExists { name } if name.as_str() == "orders"),
            "expected an already exists error"
        );

        let listed_topics = broker.list_topics();

        assert_eq!(
            listed_topics,
            vec![orders.to_string()],
            "expected the original 'orders' topic to still be listed"
        );

        let record = broker
            .read(orders, publish_result.partition(), publish_result.offset())
            .expect("failed to read the original record")
            .expect("expected the original record to exist");

        assert_eq!(
            record.offset(),
            publish_result.offset(),
            "expected the record offset to remain unchanged"
        );
        assert_eq!(
            record.key().expect("expected the record key to exist"),
            key,
            "expected the record key to remain unchanged"
        );
        assert_eq!(
            record.payload(),
            payload,
            "expected the record payload to remain unchanged"
        );
    }

    #[test]
    fn publish_to_a_missing_topic_returns_not_found_without_creating_state() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        let input = PublishInput::new(None, vec![1, 2, 3]);
        let name = "missing";
        let error = broker
            .publish(name, input)
            .expect_err("expected an error for missing topic");

        assert!(
            matches!(error, TopicError::NotFound { name } if name.as_str() == "missing"),
            "expected a not found error for the missing topic"
        );
        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed after attempting to publish to a missing topic"
        );
        assert!(
            !data_root.path().join("missing").exists(),
            "expected the missing topic directory not to exist"
        );
    }

    #[test]
    fn publish_to_a_known_topic_reports_topic_partition_and_offset() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic("orders".to_string(), 1)
            .expect("failed to create topic");

        let input = PublishInput::new(Some(vec![0]), vec![1, 2, 3]);
        let name = "orders";
        let result = broker
            .publish(name, input)
            .expect("failed to publish to topic");

        assert_eq!(
            result.topic(),
            name,
            "expected the result to report the correct topic"
        );
        assert_eq!(
            result.partition(),
            0,
            "expected the result to report the correct partition"
        );
        assert_eq!(
            result.offset(),
            0,
            "expected the result to report the correct offset"
        );
    }

    #[test]
    fn read_from_a_missing_topic_returns_not_found_without_creating_state() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        let name = "missing";
        let error = broker
            .read(name, 0, 0)
            .expect_err("expected an error for missing topic");

        assert!(
            matches!(error, TopicError::NotFound { name } if name.as_str() == "missing"),
            "expected a not found error for the missing topic"
        );
        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed"
        );
        assert!(
            !data_root.path().join("missing").exists(),
            "expected the missing topic directory not to exist"
        );
    }

    #[test]
    fn read_from_a_known_topic_rejects_a_nonzero_partition_id() {
        let orders = "orders";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic(orders.to_string(), 1)
            .expect("failed to create topic");

        let error = broker
            .read(orders, 1, 0)
            .expect_err("expected an error for unsupported partition ID");

        assert!(
            matches!(error, TopicError::UnsupportedPartitionId { requested } if requested == 1),
            "expected an unsupported partition ID error for the requested partition"
        );
    }

    #[test]
    fn read_from_a_known_topic_returns_the_published_record() {
        let topic = "orders";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");
        let key0 = vec![0, 255];
        let key1 = vec![0, 255];
        let payload0 = vec![1, 0, 254];
        let payload1 = vec![1, 0, 254];
        let publish_input0 = PublishInput::new(Some(key0.clone()), payload0.clone());
        let publish_input1 = PublishInput::new(Some(key1.clone()), payload1.clone());

        broker
            .create_topic(topic.to_string(), 1)
            .expect("failed to create topic");

        let publish_result0 = broker
            .publish(topic, publish_input0)
            .expect("failed to publish to topic");

        let record0 = broker
            .read(topic, 0, publish_result0.offset())
            .expect("failed to read from topic")
            .expect("expected a record to be returned");

        assert_eq!(
            record0.offset(),
            publish_result0.offset(),
            "expected the record to have the correct offset"
        );
        assert_eq!(
            record0.key().expect("expected the record to have a key"),
            key0,
            "expected the record to have the correct key"
        );
        assert_eq!(
            record0.payload(),
            payload0,
            "expected the record to have the correct payload"
        );

        let publish_result1 = broker
            .publish(topic, publish_input1)
            .expect("failed to publish to topic");

        let record0 = broker
            .read(topic, 0, publish_result0.offset())
            .expect("failed to read from topic")
            .expect("expected a record to be returned");
        let record1 = broker
            .read(topic, 0, publish_result1.offset())
            .expect("failed to read from topic")
            .expect("expected a record to be returned");

        assert_eq!(
            record0.offset(),
            publish_result0.offset(),
            "expected the record to have the correct offset"
        );
        assert_eq!(
            record0.key().expect("expected the record to have a key"),
            key0,
            "expected the record to have the correct key"
        );
        assert_eq!(
            record0.payload(),
            payload0,
            "expected the record to have the correct payload"
        );

        assert_eq!(
            record1.offset(),
            publish_result1.offset(),
            "expected the record to have the correct offset"
        );
        assert_eq!(
            record1.key().expect("expected the record to have a key"),
            key1,
            "expected the record to have the correct key"
        );
        assert_eq!(
            record1.payload(),
            payload1,
            "expected the record to have the correct payload"
        );
    }

    #[test]
    fn interleaved_publishes_to_multiple_topics_keep_offsets_and_records_isolated() {
        let orders_topic = "orders";
        let payments_topic = "payments";
        let order_key_0 = vec![0x01];
        let payment_key_0 = vec![0x02];
        let order_key_1 = vec![0x03];
        let order_payload_0 = vec![0x10, 0x11];
        let payment_payload_0 = vec![0x20, 0x21];
        let order_payload_1 = vec![0x30, 0x31];
        let orders_input_0 = PublishInput::new(Some(order_key_0.clone()), order_payload_0.clone());
        let payments_input_0 =
            PublishInput::new(Some(payment_key_0.clone()), payment_payload_0.clone());
        let orders_input_1 = PublishInput::new(Some(order_key_1.clone()), order_payload_1.clone());
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic(orders_topic.to_string(), 1)
            .expect("failed to create orders topic");
        broker
            .create_topic(payments_topic.to_string(), 1)
            .expect("failed to create payments topic");

        let order_0_result = broker
            .publish(orders_topic, orders_input_0)
            .expect("failed to publish to orders topic");
        let payment_0_result = broker
            .publish(payments_topic, payments_input_0)
            .expect("failed to publish to payments topic");
        let order_1_result = broker
            .publish(orders_topic, orders_input_1)
            .expect("failed to publish to orders topic");

        assert_eq!(
            order_0_result.offset(),
            0,
            "expected the first order publish to have offset 0"
        );
        assert_eq!(
            payment_0_result.offset(),
            0,
            "expected the first payment publish to have offset 0"
        );
        assert_eq!(
            order_1_result.offset(),
            1,
            "expected the second order publish to have offset 1"
        );

        let order_0_record = broker
            .read(orders_topic, 0, order_0_result.offset())
            .expect("failed to read first order record")
            .expect("expected a record");
        let payment_0_record = broker
            .read(payments_topic, 0, payment_0_result.offset())
            .expect("failed to read first payment record")
            .expect("expected a record");
        let order_1_record = broker
            .read(orders_topic, 0, order_1_result.offset())
            .expect("failed to read second order record")
            .expect("expected a record");
        let order_2_none = broker
            .read(orders_topic, 0, order_1_result.offset() + 1)
            .expect("failed to read third order record");
        let payment_1_none = broker
            .read(payments_topic, 0, payment_0_result.offset() + 1)
            .expect("failed to read second payment record");

        let order_0_record_key = order_0_record
            .key()
            .expect("expected the first order record to have a key");
        let payment_0_record_key = payment_0_record
            .key()
            .expect("expected the first payment record to have a key");
        let order_1_record_key = order_1_record
            .key()
            .expect("expected the second order record to have a key");

        assert_eq!(
            order_0_record_key, order_key_0,
            "expected the first order record to have the correct key"
        );
        assert_eq!(
            order_0_record.payload(),
            &order_payload_0,
            "expected the first order record to have the correct payload"
        );

        assert_eq!(
            payment_0_record_key, payment_key_0,
            "expected the first payment record to have the correct key"
        );
        assert_eq!(
            payment_0_record.payload(),
            &payment_payload_0,
            "expected the first payment record to have the correct payload"
        );

        assert_eq!(
            order_1_record_key, order_key_1,
            "expected the second order record to have the correct key"
        );
        assert_eq!(
            order_1_record.payload(),
            &order_payload_1,
            "expected the second order record to have the correct payload"
        );

        assert!(order_2_none.is_none(), "expected no third order record");
        assert!(
            payment_1_none.is_none(),
            "expected no second payment record"
        );
    }

    #[test]
    fn opening_a_broker_ignores_the_reserved_consumer_offset_directory() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let consumer_offsets_dir = data_root.path().join("__consumer_offsets");
        let consumer_offsets_fraud_detector_dir = consumer_offsets_dir.join("fraud-detector");
        std::fs::create_dir_all(&consumer_offsets_fraud_detector_dir)
            .expect("failed to create nested consumer offsets directory");

        let broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to create broker");

        assert!(
            broker.list_topics().is_empty(),
            "expected no topics to be listed"
        );
        assert!(
            consumer_offsets_dir.exists(),
            "expected the reserved consumer offsets directory to still exist"
        );
        assert!(
            consumer_offsets_fraud_detector_dir.exists(),
            "expected the nested group directory to still exist"
        );
    }

    #[test]
    fn opening_a_broker_with_a_regular_file_child_rejects_the_catalog_without_overwriting_entries()
    {
        let empty = "empty";
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to create broker");

        broker
            .create_topic(empty.to_string(), 1)
            .expect("failed to create topic");

        assert!(
            data_root.path().join(empty).join("0").exists(),
            "expected the valid topic's partition-0 directory to still exist"
        );

        drop(broker);

        let malformed_path = data_root.path().join("malformed");
        let sentinel = b"sentinel";

        std::fs::write(&malformed_path, sentinel).expect("failed to write malformed file");

        let error = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error due to malformed catalog entry");

        assert!(
            matches!(error, TopicError::UnexpectedCatalogEntry { path, reason: CatalogEntryErrorReason::NotDirectory } if path == malformed_path)
        );
        assert!(
            data_root.path().join(empty).join("0").exists(),
            "expected the valid topic's partition-0 directory to still exist"
        );

        let contents = std::fs::read(&malformed_path).expect("failed to read malformed file");
        assert_eq!(
            contents, sentinel,
            "expected the malformed file to still contain the sentinel bytes"
        );
    }

    #[test]
    fn opening_a_broker_with_an_invalid_topic_directory_name_rejects_the_catalog() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let bad_name_path = data_root.path().join("bad.name");
        let partition_0_path = bad_name_path.join("0");

        std::fs::create_dir_all(&partition_0_path).expect("failed to create bad topic directory");

        let error = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error due to invalid topic directory name");

        assert!(
            matches!(error, TopicError::UnexpectedCatalogEntry { path, reason: CatalogEntryErrorReason::InvalidTopicName } if path == bad_name_path)
        );
        assert!(
            bad_name_path.exists(),
            "expected the malformed directory to still exist"
        );
        assert!(
            partition_0_path.exists(),
            "expected the partition-0 directory inside the malformed directory to still exist"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn opening_a_broker_with_a_non_utf8_topic_directory_name_rejects_the_catalog() {
        use std::os::unix::ffi::OsStringExt;

        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let non_utf8_name = std::ffi::OsString::from_vec(vec![0xFF]);
        let non_utf8_path = data_root.path().join(&non_utf8_name);
        let partition_0_path = non_utf8_path.join("0");

        std::fs::create_dir_all(&partition_0_path)
            .expect("failed to create non-UTF-8 topic directory");

        let error = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error due to non-UTF-8 topic directory name");

        assert!(
            matches!(error, TopicError::UnexpectedCatalogEntry { path, reason: CatalogEntryErrorReason::InvalidTopicName } if path == non_utf8_path)
        );
        assert!(
            non_utf8_path.exists(),
            "expected the non-UTF-8 directory to still exist"
        );
        assert!(
            partition_0_path.exists(),
            "expected the partition-0 directory inside the non-UTF-8 directory to still exist"
        );
    }

    #[test]
    fn opening_a_broker_with_a_topic_symlink_rejects_the_catalog_without_following_it() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let target = tempfile::tempdir().expect("failed to create temporary target directory");
        let partition_0_path = target.path().join("0");
        std::fs::create_dir_all(&partition_0_path).expect("failed to create partition 0 in target");
        let symlink_path = data_root.path().join("orders");

        #[cfg(unix)]
        std::os::unix::fs::symlink(target.path(), &symlink_path).expect("failed to create symlink");

        #[cfg(windows)]
        match std::os::windows::fs::symlink_dir(target.path(), &symlink_path) {
            Ok(_) => {}
            Err(error) => {
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    return;
                } else {
                    panic!("unexpected error creating symlink: {:?}", error);
                }
            }
        }

        let error = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect_err("expected an error due to topic symlink");

        assert!(
            matches!(error, TopicError::UnexpectedCatalogEntry { path, reason: CatalogEntryErrorReason::SymbolicLink } if path == symlink_path)
        );

        let metadata =
            std::fs::symlink_metadata(&symlink_path).expect("failed to inspect topic symlink");

        assert!(
            metadata.file_type().is_symlink(),
            "expected the topic entry to remain a symlink"
        );
        assert!(
            partition_0_path.exists(),
            "expected the partition-0 directory inside the target to still exist"
        );
    }

    #[test]
    fn create_topic_rejects_an_existing_topic_symlink_without_following_or_overwriting_it() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        let target = tempfile::tempdir().expect("failed to create temporary target directory");
        let sentinel_path = target.path().join("sentinel");
        std::fs::write(&sentinel_path, b"sentinel").expect("failed to write sentinel file");

        let symlink_path = data_root.path().join("orders");
        #[cfg(unix)]
        std::os::unix::fs::symlink(target.path(), &symlink_path).expect("failed to create symlink");
        #[cfg(windows)]
        match std::os::windows::fs::symlink_dir(target.path(), &symlink_path) {
            Ok(_) => {}
            Err(error) => {
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    return;
                } else {
                    panic!("unexpected error creating symlink: {:?}", error);
                }
            }
        }

        let error = broker
            .create_topic("orders".to_string(), 1)
            .expect_err("expected an error due to existing symlink");
        assert!(matches!(error, TopicError::AlreadyExists { name } if name == "orders"));
        // Verify the Broker catalog remains empty after the failed creation.
        assert!(
            broker.list_topics().is_empty(),
            "expected the broker catalog to remain empty"
        );

        // Inspect `orders` with symlink_metadata and verify it remains a symlink.
        let metadata =
            std::fs::symlink_metadata(&symlink_path).expect("failed to inspect topic symlink");
        assert!(
            metadata.file_type().is_symlink(),
            "expected the topic entry to remain a symlink"
        );

        // Verify the target sentinel file and its exact bytes remain unchanged.
        let sentinel_bytes = std::fs::read(&sentinel_path).expect("failed to read sentinel file");
        assert_eq!(
            sentinel_bytes, b"sentinel",
            "expected the sentinel file to remain unchanged"
        );
    }

    #[test]
    fn create_topic_rejects_a_case_alias_on_case_insensitive_filesystems() {
        let data_root = tempfile::tempdir().expect("failed to create temporary data root");
        let key = vec![10, 20, 30];
        let payload = vec![1, 2, 3];
        let input = PublishInput::new(Some(key.clone()), payload.clone());
        let mut broker = Broker::open(
            data_root.path().to_path_buf(),
            SegmentConfig::default(),
            RecordLimits::default(),
        )
        .expect("failed to open broker");

        broker
            .create_topic("Orders".to_string(), 1)
            .expect("failed to create topic");
        let publish_result = broker
            .publish("Orders".to_string().as_str(), input)
            .expect("failed to publish to Orders");

        assert_eq!(publish_result.offset(), 0);

        let alias_path = data_root.path().join("orders");

        if !alias_path.exists() {
            return;
        }

        let error = broker
            .create_topic("orders".to_string(), 1)
            .expect_err("expected an error due to case-insensitive alias");
        assert!(matches!(error, TopicError::AlreadyExists { name } if name == "orders"));

        assert_eq!(broker.list_topics(), vec!["Orders".to_string()]);

        let record = broker
            .read(
                "Orders",
                publish_result.partition(),
                publish_result.offset(),
            )
            .expect("failed to read record from Orders")
            .expect("expected a record at the given offset");

        assert_eq!(record.offset(), publish_result.offset());
        assert_eq!(record.key().expect("expected a key"), key);
        assert_eq!(record.payload(), payload);
    }
}
