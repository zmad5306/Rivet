
#[derive(Debug, PartialEq, Eq)]
pub enum PartitionError {
    OffsetOverflow, ClockBeforeEpoch
}