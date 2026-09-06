 use std::time::{SystemTime, UNIX_EPOCH};
 
 use crate::storage::record::{Event, Record};
 use crate::error::PartitionError;

#[derive(Debug, PartialEq, Eq)]
pub struct Partition {
    records: Vec<Record>,
    current_offset: u64
}

impl Partition {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            current_offset: 0
        }
    }

    pub fn read(&self, offset: u64) -> Option<&Record> {
        match usize::try_from(offset) {
            Ok(index) => self.records.get(index),
            Err(_) => None,
        }
    }

    pub fn publish(&mut self, event: Event) -> Result<u64, PartitionError> {
        if self.current_offset == u64::MAX {
            return Err(PartitionError::OffsetOverflow);
        }

        let offset = self.current_offset;
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(_) => return Err(PartitionError::ClockBeforeEpoch),
        };
        let (key, payload) = event.into_parts();
        let record = Record::new(offset, timestamp, key, payload);

        self.records.push(record);
        self.current_offset += 1;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn first_publish_returns_offset_zero() {
        todo!("Create an empty partition, publish one event, and assert the returned offset is 0.");
    }

    #[test]
    fn consecutive_publishes_return_consecutive_offsets() {
        todo!("Publish A, B, and C and assert the returned offsets are 0, 1, and 2.");
    }

    #[test]
    fn read_returns_the_record_at_each_assigned_offset() {
        todo!("Publish distinct events A, B, and C; read offsets 0, 1, and 2 and verify each record's offset, key, and payload.");
    }

    #[test]
    fn read_from_empty_partition_returns_none() {
        todo!("Read offset 0 from a new partition and assert None.");
    }

    #[test]
    fn read_beyond_last_offset_returns_none() {
        todo!("Publish some events, then read just beyond the last offset and at u64::MAX; assert None.");
    }

    #[test]
    fn publish_and_read_preserve_binary_key_and_payload() {
        todo!("Round-trip a key and payload containing non-UTF-8 bytes and zero bytes; compare the exact bytes.");
    }

    #[test]
    fn publish_and_read_preserve_absent_and_empty_keys() {
        todo!("Publish one event with no key and another with an empty key; verify reads preserve the distinction.");
    }

    #[test]
    fn publish_and_read_preserve_empty_payload() {
        todo!("Publish an event with an empty payload and verify its stored payload is empty.");
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
