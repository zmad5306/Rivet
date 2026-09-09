#[derive(Debug, PartialEq, Eq)]
pub enum PartitionError {
    OffsetOverflow,
    ClockBeforeEpoch,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CodecError {
    InvalidMagic,
    UnsupportedVersion,
    KeyTooLarge,
    PayloadTooLarge,
    IncompleteHeader,
    IncompleteBody,
    LengthOverflow,
    InvalidKeyPresence,
    InvalidKeyLength,
    InvalidChecksum,
}
