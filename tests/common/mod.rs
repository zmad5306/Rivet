use super::Record;

/// A deterministic test fixture with standard fields and a caller-selected offset.
pub fn sample_record(offset: u64) -> Record {
    Record::new(offset, 1_700_000_000, Some(vec![10, 20, 30]), vec![1, 2, 3])
}
