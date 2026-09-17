use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::PartitionError;
use crate::storage::record::{PublishInput, Record, RecordLimits};
use crate::storage::segment::{SegmentConfig, SegmentedLog};

#[derive(Debug)]
pub struct Partition {
    segmented_log: SegmentedLog,
}

impl Partition {
    pub fn new(
        directory: &Path,
        config: SegmentConfig,
        limits: RecordLimits,
    ) -> Result<Self, PartitionError> {
        let segmented_log = SegmentedLog::open(directory, limits, config)?;
        Ok(Self { segmented_log })
    }

    pub fn read(&self, offset: u64) -> Result<Option<Record>, PartitionError> {
        Ok(self.segmented_log.read(offset)?)
    }

    pub fn publish(&mut self, input: PublishInput) -> Result<u64, PartitionError> {
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => return Err(PartitionError::ClockBeforeEpoch),
        };
        let (key, payload) = input.into_parts();
        let offset = self.segmented_log.next_offset();
        let record = Record::new(offset, timestamp, key, payload);

        self.segmented_log.append(&record)?;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::partition::Partition;
    use crate::error::{CodecError, PartitionError, StorageError};
    use crate::storage::record::PublishInput;
    use crate::storage::record::RecordLimits;
    use crate::storage::segment::SegmentConfig;

    #[test]
    fn first_publish_returns_offset_zero() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input = PublishInput::new(key, payload);

        let result = partition.publish(input).expect("failed to publish record");

        assert_eq!(result, 0, "publish should assign offset 0");
    }

    #[test]
    fn consecutive_publishes_return_consecutive_offsets() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key.clone(), payload.clone());
        let input2 = PublishInput::new(key.clone(), payload.clone());
        let input3 = PublishInput::new(key, payload);

        let result1 = partition.publish(input1).expect("failed to publish record");
        let result2 = partition.publish(input2).expect("failed to publish record");
        let result3 = partition.publish(input3).expect("failed to publish record");

        assert_eq!(result1, 0, "publish should assign offset 0");
        assert_eq!(result2, 1, "publish should assign offset 1");
        assert_eq!(result3, 2, "publish should assign offset 2");
    }

    #[test]
    fn read_returns_the_record_at_each_assigned_offset() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![20, 30, 40]);
        let key3: Option<Vec<u8>> = Some(vec![30, 40, 50]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![2, 3, 4];
        let payload3: Vec<u8> = vec![3, 4, 5];
        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);
        let input3 = PublishInput::new(key3, payload3);

        let result1 = partition.publish(input1).expect("failed to publish record");
        let result2 = partition.publish(input2).expect("failed to publish record");
        let result3 = partition.publish(input3).expect("failed to publish record");

        assert_eq!(result1, 0, "publish should assign offset 0");
        assert_eq!(result2, 1, "publish should assign offset 1");
        assert_eq!(result3, 2, "publish should assign offset 2");

        let record1 = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should exist");
        let record2 = partition
            .read(1)
            .expect("record at offset 1 should exist")
            .expect("record at offset 1 should exist");
        let record3 = partition
            .read(2)
            .expect("record at offset 2 should exist")
            .expect("record at offset 2 should exist");

        assert_eq!(
            record1.offset(),
            0,
            "read returns the record at each assigned offset: offset should match the expected value"
        );
        assert_eq!(
            record2.offset(),
            1,
            "read returns the record at each assigned offset: offset should match the expected value"
        );
        assert_eq!(
            record3.offset(),
            2,
            "read returns the record at each assigned offset: offset should match the expected value"
        );
        assert_eq!(
            record1.key(),
            Some(&[10, 20, 30][..]),
            "read returns the record at each assigned offset: key should match the expected value"
        );
        assert_eq!(
            record2.key(),
            Some(&[20, 30, 40][..]),
            "read returns the record at each assigned offset: key should match the expected value"
        );
        assert_eq!(
            record3.key(),
            Some(&[30, 40, 50][..]),
            "read returns the record at each assigned offset: key should match the expected value"
        );
        assert_eq!(
            record1.payload(),
            &[1, 2, 3],
            "read returns the record at each assigned offset: payload should match the expected value"
        );
        assert_eq!(
            record2.payload(),
            &[2, 3, 4],
            "read returns the record at each assigned offset: payload should match the expected value"
        );
        assert_eq!(
            record3.payload(),
            &[3, 4, 5],
            "read returns the record at each assigned offset: payload should match the expected value"
        );
    }

    #[test]
    fn read_from_empty_partition_returns_none() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");

        let result_none = partition.read(0).expect("failed to read from partition");

        assert!(
            result_none.is_none(),
            "reading an unavailable offset should return None"
        );
    }

    #[test]
    fn read_beyond_last_offset_returns_none() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input = PublishInput::new(key, payload);

        let offset = partition.publish(input).expect("failed to publish record");
        let record_none = partition.read(42).expect("failed to read from partition");

        assert_eq!(offset, 0, "publish should assign offset 0");
        assert!(
            record_none.is_none(),
            "reading an unavailable offset should return None"
        );
    }

    #[test]
    fn publish_and_read_preserve_binary_key_and_payload() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![0xFF, 0x00, 0x80]);
        let payload: Vec<u8> = vec![0xFE, 0x00, 0x81];
        let input = PublishInput::new(key, payload);

        let offset = partition.publish(input).expect("failed to publish record");
        let record = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should not be None");

        assert_eq!(offset, 0, "publish should assign offset 0");
        assert_eq!(
            record.key(),
            Some(&[0xFF, 0x00, 0x80][..]),
            "publish and read preserve binary key and payload: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            &[0xFE, 0x00, 0x81],
            "publish and read preserve binary key and payload: payload should match the expected value"
        );
    }

    #[test]
    fn publish_and_read_preserve_absent_and_empty_keys() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key1: Option<Vec<u8>> = None;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key1, payload.clone());
        let input2 = PublishInput::new(key2, payload);

        let result1 = partition.publish(input1).expect("failed to publish input1");
        let result2 = partition.publish(input2).expect("failed to publish input2");

        assert_eq!(result1, 0, "publish should assign offset 0");
        assert_eq!(result2, 1, "publish should assign offset 1");

        let record1 = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should not be None");
        let record2 = partition
            .read(1)
            .expect("record at offset 1 should exist")
            .expect("record at offset 1 should not be None");

        assert_eq!(
            record1.key(),
            None,
            "publish and read preserve absent and empty keys: key should match the expected value"
        );
        assert_eq!(
            record2.key(),
            Some(&[][..]),
            "publish and read preserve absent and empty keys: key should match the expected value"
        );
    }

    #[test]
    fn publish_and_read_preserve_empty_payload() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![];
        let input = PublishInput::new(key, payload);

        let offset = partition.publish(input).expect("failed to publish input");

        assert_eq!(offset, 0, "publish should assign offset 0");

        let record = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should not be None");

        assert!(
            record.payload().is_empty(),
            "empty payload should remain empty"
        );
    }

    #[test]
    fn later_publishes_leave_existing_records_unchanged() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let dir_path = dir.path();
        let config = SegmentConfig::default();
        let limits = RecordLimits::default();

        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key.clone(), payload.clone());
        let input2 = PublishInput::new(key, payload);

        let offset1 = partition
            .publish(input1)
            .expect("publish should assign offset 0");

        let record1_before = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should exist");
        let recorded_key = record1_before.key().map(|bytes| bytes.to_vec());
        let recorded_offset = record1_before.offset();
        let recorded_payload = record1_before.payload().to_vec();
        let recorded_timestamp = record1_before.timestamp();

        let offset2 = partition
            .publish(input2)
            .expect("publish should assign offset 1");

        let record1_after = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should exist");

        assert_eq!(offset1, 0, "publish should assign offset 0");
        assert_eq!(offset2, 1, "publish should assign offset 1");

        assert_eq!(
            record1_after.key(),
            recorded_key.as_deref(),
            "later publish should preserve the existing record key"
        );
        assert_eq!(
            record1_after.offset(),
            recorded_offset,
            "later publish should preserve the existing record offset"
        );
        assert_eq!(
            record1_after.payload(),
            recorded_payload,
            "later publish should preserve the existing record payload"
        );
        assert_eq!(
            record1_after.timestamp(),
            recorded_timestamp,
            "later publish should preserve the existing record timestamp"
        );
    }

    #[test]
    fn rejected_oversized_publish_preserves_records_and_next_offset() {
        let dir = tempfile::tempdir().expect("failed to create temporary directory");
        let dir_path = dir.path();
        let limits = RecordLimits::new(3, 3);
        let config = SegmentConfig::default();
        let mut partition =
            Partition::new(dir_path, config, limits).expect("failed to create partition");
        let input0 = PublishInput::new(None, vec![0; 3]);
        let offset = partition
            .publish(input0)
            .expect("publish should assign offset 0");

        assert_eq!(offset, 0, "publish should assign offset 0");

        let record = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should exist");
        let record_none = partition.read(1).expect("read at offset 1 should succeed");

        assert!(record_none.is_none(), "record at offset 1 should not exist");

        let input1 = PublishInput::new(None, vec![0; 4]);
        let error = partition
            .publish(input1)
            .expect_err("publish should fail due to oversized payload");

        assert!(
            matches!(
                error,
                PartitionError::Storage {
                    source: StorageError::Codec(CodecError::PayloadTooLarge)
                }
            ),
            "publish should fail due to oversized payload"
        );

        let reread_record = partition
            .read(0)
            .expect("record at offset 0 should exist")
            .expect("record at offset 0 should exist");
        let reread_record_none = partition.read(1).expect("read at offset 1 should succeed");

        assert_eq!(
            reread_record, record,
            "record at offset 0 should remain unchanged after rejected publish"
        );
        assert!(
            reread_record_none.is_none(),
            "record at offset 1 should not exist after rejected publish"
        );

        let input2 = PublishInput::new(None, vec![0; 3]);
        let offset2 = partition
            .publish(input2)
            .expect("publish should succeed within payload limit");
        assert_eq!(
            offset2, 1,
            "publish after rejected input should assign offset 1"
        );
    }
}
