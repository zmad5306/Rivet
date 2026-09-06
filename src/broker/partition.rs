use std::time::{SystemTime, UNIX_EPOCH};
 
use crate::storage::record::{PublishInput, Record};
use crate::error::PartitionError;

#[derive(Debug, PartialEq, Eq)]
pub struct Partition {
    records: Vec<Record>,
    next_offset: Option<u64>
}

impl Partition {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            next_offset: Some(0)
        }
    }

    pub fn read(&self, offset: u64) -> Option<&Record> {
        match usize::try_from(offset) {
            Ok(index) => self.records.get(index),
            Err(_) => None,
        }
    }

    pub fn publish(&mut self, input: PublishInput) -> Result<u64, PartitionError> {
        let offset = match self.next_offset {
            Some(offset) => offset,
            None => return Err(PartitionError::OffsetOverflow)
        };
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => return Err(PartitionError::ClockBeforeEpoch),
        };
        let (key, payload) = input.into_parts();
        let record = Record::new(offset, timestamp, key, payload);

        self.records.push(record);

        if offset == u64::MAX {
            self.next_offset = None;
        }
        else {
            self.next_offset = Some(offset + 1);
        }

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::record::PublishInput;
    use crate::broker::partition::Partition;

    #[test]
    fn first_publish_returns_offset_zero() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input = PublishInput::new(key, payload);

        let result = partition.publish(input);

        assert_eq!(result, Ok(0));
    }

    #[test]
    fn consecutive_publishes_return_consecutive_offsets() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key.clone(), payload.clone());
        let input2 = PublishInput::new(key.clone(), payload.clone());
        let input3 = PublishInput::new(key, payload);

        let result1 = partition.publish(input1);
        let result2 = partition.publish(input2);
        let result3 = partition.publish(input3);
        
        assert_eq!(result1, Ok(0));
        assert_eq!(result2, Ok(1));
        assert_eq!(result3, Ok(2));
    }

    #[test]
    fn read_returns_the_record_at_each_assigned_offset() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key.clone(), payload.clone());
        let input2 = PublishInput::new(key.clone(), payload.clone());
        let input3 = PublishInput::new(key, payload);

        let result1 = partition.publish(input1);
        let result2 = partition.publish(input2);
        let result3 = partition.publish(input3);

        assert_eq!(result1, Ok(0));
        assert_eq!(result2, Ok(1));
        assert_eq!(result3, Ok(2));

        let record1 = partition.read(0).expect("record at offset 0 should exist");
        let record2 = partition.read(1).expect("record at offset 1 should exist");
        let record3 = partition.read(2).expect("record at offset 2 should exist");
        
        assert_eq!(record1.offset(), 0);
        assert_eq!(record2.offset(), 1);
        assert_eq!(record3.offset(), 2);
        assert_eq!(record1.payload(), &[1, 2, 3]);
        assert_eq!(record2.payload(), &[1, 2, 3]);
        assert_eq!(record3.payload(), &[1, 2, 3]);
    }

    #[test]
    fn read_from_empty_partition_returns_none() {
        let partition = Partition::new();
        
        let result = partition.read(0);

        assert!(result.is_none());
    }

    #[test]
    fn read_beyond_last_offset_returns_none() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input = PublishInput::new(key, payload);

        let result = partition.publish(input);
        let record = partition.read(42);

        assert_eq!(result, Ok(0));
        assert!(record.is_none());
    }

    #[test]
    fn publish_and_read_preserve_binary_key_and_payload() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![0xFF, 0x00, 0x80]);
        let payload: Vec<u8> = vec![0xFE, 0x00, 0x81];
        let input = PublishInput::new(key, payload);

        let result = partition.publish(input);
        let record = partition.read(0).expect("record at offset 0 should exist");

        assert_eq!(result, Ok(0));
        assert_eq!(record.key(), Some(&[0xFF, 0x00, 0x80][..]));
        assert_eq!(record.payload(), &[0xFE, 0x00, 0x81]);
    }

    #[test]
    fn publish_and_read_preserve_absent_and_empty_keys() {
        let mut partition = Partition::new();
        let key1: Option<Vec<u8>> = None;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let input1 = PublishInput::new(key1, payload.clone());
        let input2 = PublishInput::new(key2, payload);

        let result1 = partition.publish(input1);
        let result2 = partition.publish(input2);

        assert_eq!(result1, Ok(0));
        assert_eq!(result2, Ok(1));

        let record1 = partition.read(0).expect("record at offset 0 should exist");
        let record2 = partition.read(1).expect("record at offset 1 should exist");
        
        assert_eq!(record1.key(), None);
        assert_eq!(record2.key(), Some(&[][..]));
    }

    #[test]
    fn publish_and_read_preserve_empty_payload() {
        let mut partition = Partition::new();
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload: Vec<u8> = vec![];
        let input = PublishInput::new(key, payload);

        let result = partition.publish(input);

        assert_eq!(result, Ok(0));

        let record = partition.read(0).expect("record at offset 0 should exist");
        
        assert!(record.payload().is_empty());
    }

    #[test]
    fn later_publishes_leave_existing_records_unchanged() {
        todo!("Save the first record's field values, publish more events, and verify all its fields, including timestamp, remain unchanged.");
    }

    #[test]
    fn publish_at_offset_limit_returns_overflow_without_changing_state() {
        todo!("After storing a record, set the private counter to u64::MAX from this unit-test module. Assert publishing returns OffsetOverflow and leaves the counter and records unchanged; no huge allocation is needed.");
    }
}
