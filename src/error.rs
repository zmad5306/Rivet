#[derive(Debug, PartialEq, Eq)]
pub enum PartitionError {
    OffsetOverflow,
    ClockBeforeEpoch,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StorageError {
    InvalidMagic,
    UnsupportedVersion,
    KeyTooLarge,
    PaylodTooLarge,
    IncompleteHeader,
    IncompleteBody,
    LengthOverflow    
}
