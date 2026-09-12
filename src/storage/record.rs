use crate::error::CodecError;
use crc32fast::hash;

const MAGIC: &[u8; 4] = b"RIVT";
const VERSION: u8 = 1;
pub(super) const HEADER_LENGTH: usize = 30;

#[derive(Debug, PartialEq, Eq)]
pub struct RecordLimits {
    max_key_bytes: u32,
    max_payload_bytes: u32,
}

impl RecordLimits {
    pub fn new(max_key_bytes: u32, max_payload_bytes: u32) -> Self {
        Self {
            max_key_bytes,
            max_payload_bytes,
        }
    }
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self::new(1024, 1024 * 1024)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    offset: u64,
    timestamp: u64,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

impl Record {
    pub fn new(offset: u64, timestamp: u64, key: Option<Vec<u8>>, payload: Vec<u8>) -> Self {
        Self {
            offset,
            timestamp,
            key,
            payload,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn encoded_len(&self) -> Result<usize, CodecError> {
        let key_len = self.key().map_or(0, |k| k.len());
        let payload_len = self.payload().len();

        let total_len = HEADER_LENGTH
            .checked_add(key_len)
            .and_then(|len| len.checked_add(payload_len))
            .and_then(|len| len.checked_add(4)) // CRC32 checksum
            .ok_or(CodecError::LengthOverflow)?;

        Ok(total_len)
    }

    fn check_len(len: usize, max: u32, error: CodecError) -> Result<u32, CodecError> {
        match u32::try_from(len) {
            Ok(l) => {
                if l > max {
                    return Err(error);
                }
                Ok(l)
            }
            Err(_) => Err(CodecError::LengthOverflow),
        }
    }

    pub(super) fn encoded_record_len(
        header: &[u8],
        limits: &RecordLimits,
    ) -> Result<usize, CodecError> {
        if header.len() < HEADER_LENGTH {
            return Err(CodecError::IncompleteHeader);
        }

        if header[0..4] != MAGIC[..] {
            return Err(CodecError::InvalidMagic);
        }

        if header[4] != VERSION {
            return Err(CodecError::UnsupportedVersion);
        }

        let key_present = header[21];
        let key_length_bytes: [u8; 4] = header[22..26]
            .try_into()
            .expect("header checked; key length slice is exactly four bytes");
        let payload_length_bytes: [u8; 4] = header[26..30]
            .try_into()
            .expect("header checked; payload length slice is exactly four bytes");

        let key_length = u32::from_be_bytes(key_length_bytes);
        let payload_length = u32::from_be_bytes(payload_length_bytes);

        if key_present != 0 && key_present != 1 {
            return Err(CodecError::InvalidKeyPresence);
        }

        if key_present == 0 && key_length > 0 {
            return Err(CodecError::InvalidKeyLength);
        }

        if key_length > limits.max_key_bytes {
            return Err(CodecError::KeyTooLarge);
        }

        if payload_length > limits.max_payload_bytes {
            return Err(CodecError::PayloadTooLarge);
        }

        let key_len = usize::try_from(key_length).map_err(|_| CodecError::LengthOverflow)?;

        let payload_len =
            usize::try_from(payload_length).map_err(|_| CodecError::LengthOverflow)?;

        let total_len = HEADER_LENGTH
            .checked_add(key_len)
            .and_then(|len| len.checked_add(payload_len))
            .and_then(|len| len.checked_add(4)) // CRC32 checksum
            .ok_or(CodecError::LengthOverflow)?;

        Ok(total_len)
    }

    // Record format v1 (all integers big-endian):
    // [0..4]   magic: b"RIVT"
    // [4]      version: 1
    // [5..13]  offset: u64
    // [13..21] timestamp: u64
    // [21]     key presence: 0 = absent, 1 = present
    // [22..26] key length: u32
    // [26..30] payload length: u32
    // [30..]   key bytes, then payload bytes, then CRC32 checksum (u32)
    //
    // Absent keys require length 0; present keys may be empty.
    // Header: 30 bytes. Total: 34 + key length + payload length.
    // CRC32 covers version through payload, excluding magic and checksum.
    // Slice ranges exclude the ending index.

    pub fn encode(&self, limits: &RecordLimits) -> Result<Vec<u8>, CodecError> {
        let key_present = self.key().map_or(0, |_| 1);
        let key_len = self.key().as_ref().map_or(0, |key| key.len());
        let key_length = Self::check_len(key_len, limits.max_key_bytes, CodecError::KeyTooLarge)?;
        let payload_length = Self::check_len(
            self.payload().len(),
            limits.max_payload_bytes,
            CodecError::PayloadTooLarge,
        )?;
        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&self.offset().to_be_bytes());
        bytes.extend_from_slice(&self.timestamp().to_be_bytes());
        bytes.push(key_present);
        bytes.extend_from_slice(&key_length.to_be_bytes());
        bytes.extend_from_slice(&payload_length.to_be_bytes());
        if let Some(key) = self.key() {
            bytes.extend_from_slice(key);
        }
        bytes.extend_from_slice(self.payload());

        let checksum = hash(&bytes[4..]);

        bytes.extend_from_slice(&checksum.to_be_bytes());

        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], limits: &RecordLimits) -> Result<(Self, usize), CodecError> {
        let record_end = Self::encoded_record_len(bytes, limits)?;

        let offset_bytes: [u8; 8] = bytes[5..13]
            .try_into()
            .expect("header checked; offset slice is exactly eight bytes");

        let timestamp_bytes: [u8; 8] = bytes[13..21]
            .try_into()
            .expect("header checked; timestamp slice is exactly eight bytes");

        let key_length_bytes: [u8; 4] = bytes[22..26]
            .try_into()
            .expect("header checked; key length slice is exactly four bytes");

        let key_length = u32::from_be_bytes(key_length_bytes);

        let key_len = usize::try_from(key_length).map_err(|_| CodecError::LengthOverflow)?;

        let key_present = bytes[21];
        let offset = u64::from_be_bytes(offset_bytes);
        let timestamp = u64::from_be_bytes(timestamp_bytes);

        let key_end = match HEADER_LENGTH.checked_add(key_len) {
            Some(end) => end,
            None => return Err(CodecError::LengthOverflow),
        };

        if bytes.len() < record_end {
            return Err(CodecError::IncompleteBody);
        }

        let payload_end = record_end - 4;

        let checksum_bytes: [u8; 4] = bytes[payload_end..record_end]
            .try_into()
            .expect("record bounds checked; checksum slice is exactly four bytes");

        let checksum = u32::from_be_bytes(checksum_bytes);
        let computed_checksum = hash(&bytes[4..payload_end]);

        if checksum != computed_checksum {
            return Err(CodecError::InvalidChecksum);
        }

        let key = if key_present == 1 {
            Some(bytes[HEADER_LENGTH..key_end].to_vec())
        } else {
            None
        };

        let payload = bytes[key_end..payload_end].to_vec();
        let record = Record::new(offset, timestamp, key, payload);

        Ok((record, record_end))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PublishInput {
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

impl PublishInput {
    pub fn new(key: Option<Vec<u8>>, payload: Vec<u8>) -> Self {
        Self { key, payload }
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn into_parts(self) -> (Option<Vec<u8>>, Vec<u8>) {
        (self.key, self.payload)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::CodecError,
        storage::record::{HEADER_LENGTH, RecordLimits},
    };

    use super::{PublishInput, Record};

    mod common {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
    }
    use common::sample_record;

    #[test]
    fn record_constructor_preserves_all_fields() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record constructor preserves all fields: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record constructor preserves all fields: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record constructor preserves all fields: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record constructor preserves all fields: payload should match the expected value"
        );
    }

    #[test]
    fn record_absent_key_is_preserved() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = None;
        let expected_key: Option<&[u8]> = None;
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record absent key is preserved: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record absent key is preserved: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record absent key is preserved: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record absent key is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn record_empty_key_is_preserved() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![]);
        let expected_key: Option<&[u8]> = Some(&[]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record empty key is preserved: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record empty key is preserved: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record empty key is preserved: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record empty key is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn record_empty_payload_is_preserved() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![];
        let expected_payload: &[u8] = &[];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record empty payload is preserved: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record empty payload is preserved: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record empty payload is preserved: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record empty payload is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn record_non_utf8_bytes_are_preserved() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![0xFF, 0xFE, 0x00]);
        let expected_key: Option<&[u8]> = Some(&[0xFF, 0xFE, 0x00]);
        let payload: Vec<u8> = vec![0xFF, 0xFE, 0x00];
        let expected_payload: &[u8] = &[0xFF, 0xFE, 0x00];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record non utf8 bytes are preserved: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record non utf8 bytes are preserved: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record non utf8 bytes are preserved: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record non utf8 bytes are preserved: payload should match the expected value"
        );
    }

    #[test]
    fn record_integer_boundaries_are_preserved_zero() {
        let offset: u64 = 0;
        let timestamp: u64 = 0;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record integer boundaries are preserved zero: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record integer boundaries are preserved zero: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record integer boundaries are preserved zero: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record integer boundaries are preserved zero: payload should match the expected value"
        );
    }

    #[test]
    fn record_integer_boundaries_are_preserved_max() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(
            record.offset(),
            offset,
            "record integer boundaries are preserved max: offset should match the expected value"
        );
        assert_eq!(
            record.timestamp(),
            timestamp,
            "record integer boundaries are preserved max: timestamp should match the expected value"
        );
        assert_eq!(
            record.key(),
            expected_key,
            "record integer boundaries are preserved max: key should match the expected value"
        );
        assert_eq!(
            record.payload(),
            expected_payload,
            "record integer boundaries are preserved max: payload should match the expected value"
        );
    }

    #[test]
    fn records_with_identical_fields_are_equal() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let record1 = Record::new(offset, timestamp, key1, payload1);
        let record2 = Record::new(offset, timestamp, key2, payload2);

        assert_eq!(record1, record2, "identical fields should compare equal");
    }

    #[test]
    fn records_with_different_offsets_are_unequal() {
        let offset1: u64 = 0;
        let offset2: u64 = 1;
        let timestamp: u64 = 1_700_000_000;
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let record1 = Record::new(offset1, timestamp, key1, payload1);
        let record2 = Record::new(offset2, timestamp, key2, payload2);

        assert_ne!(
            record1, record2,
            "records with different offsets should compare unequal"
        );
    }

    #[test]
    fn records_with_different_timestamps_are_unequal() {
        let offset: u64 = 0;
        let timestamp1: u64 = 1_700_000_000;
        let timestamp2: u64 = 1_700_000_001;
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let record1 = Record::new(offset, timestamp1, key1, payload1);
        let record2 = Record::new(offset, timestamp2, key2, payload2);

        assert_ne!(
            record1, record2,
            "records with different timestamps should compare unequal"
        );
    }

    #[test]
    fn records_with_different_keys_are_unequal() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![20, 30, 40]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let record1 = Record::new(offset, timestamp, key1, payload1);
        let record2 = Record::new(offset, timestamp, key2, payload2);

        assert_ne!(
            record1, record2,
            "records with different keys should compare unequal"
        );
    }

    #[test]
    fn records_with_different_payloads_are_unequal() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![2, 3, 4];

        let record1 = Record::new(offset, timestamp, key1, payload1);
        let record2 = Record::new(offset, timestamp, key2, payload2);

        assert_ne!(
            record1, record2,
            "records with different payloads should compare unequal"
        );
    }

    #[test]
    fn records_with_absent_and_empty_keys_are_unequal() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key1: Option<Vec<u8>> = None;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let record1 = Record::new(offset, timestamp, key1, payload1);
        let record2 = Record::new(offset, timestamp, key2, payload2);

        assert_ne!(
            record1, record2,
            "records with absent and empty keys should compare unequal"
        );
    }

    #[test]
    fn publish_input_constructor_preserves_all_fields() {
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(
            input.key(),
            expected_key,
            "publish input constructor preserves all fields: key should match the expected value"
        );
        assert_eq!(
            input.payload(),
            expected_payload,
            "publish input constructor preserves all fields: payload should match the expected value"
        );
    }

    #[test]
    fn publish_input_absent_key_is_preserved() {
        let key: Option<Vec<u8>> = None;
        let expected_key: Option<&[u8]> = None;
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(
            input.key(),
            expected_key,
            "publish input absent key is preserved: key should match the expected value"
        );
        assert_eq!(
            input.payload(),
            expected_payload,
            "publish input absent key is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn publish_input_empty_key_is_preserved() {
        let key: Option<Vec<u8>> = Some(vec![]);
        let expected_key: Option<&[u8]> = Some(&[]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(
            input.key(),
            expected_key,
            "publish input empty key is preserved: key should match the expected value"
        );
        assert_eq!(
            input.payload(),
            expected_payload,
            "publish input empty key is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn publish_input_empty_payload_is_preserved() {
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![];
        let expected_payload: &[u8] = &[];

        let input = PublishInput::new(key, payload);

        assert_eq!(
            input.key(),
            expected_key,
            "publish input empty payload is preserved: key should match the expected value"
        );
        assert_eq!(
            input.payload(),
            expected_payload,
            "publish input empty payload is preserved: payload should match the expected value"
        );
    }

    #[test]
    fn publish_input_non_utf8_bytes_are_preserved() {
        let key: Option<Vec<u8>> = Some(vec![0xFF, 0xFE, 0x00]);
        let expected_key: Option<&[u8]> = Some(&[0xFF, 0xFE, 0x00]);
        let payload: Vec<u8> = vec![0xFF, 0xFE, 0x00];
        let expected_payload: &[u8] = &[0xFF, 0xFE, 0x00];

        let input = PublishInput::new(key, payload);

        assert_eq!(
            input.key(),
            expected_key,
            "publish input non utf8 bytes are preserved: key should match the expected value"
        );
        assert_eq!(
            input.payload(),
            expected_payload,
            "publish input non utf8 bytes are preserved: payload should match the expected value"
        );
    }

    #[test]
    fn publish_inputs_with_identical_fields_are_equal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_eq!(input1, input2, "identical fields should compare equal");
    }

    #[test]
    fn publish_inputs_with_different_keys_are_unequal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![20, 30, 40]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(
            input1, input2,
            "publish inputs with different keys should compare unequal"
        );
    }

    #[test]
    fn publish_inputs_with_different_payloads_are_unequal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![2, 3, 4];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(
            input1, input2,
            "publish inputs with different payloads should compare unequal"
        );
    }

    #[test]
    fn publish_inputs_with_absent_and_empty_keys_are_unequal() {
        let key1: Option<Vec<u8>> = None;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(
            input1, input2,
            "publish inputs with absent and empty keys should compare unequal"
        );
    }

    #[test]
    fn codec_round_trip_preserves_record() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];
        let limits = RecordLimits::default();

        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes, record,
            "decoded record should match its corresponding original record"
        );
        assert_eq!(
            record_from_bytes.offset(),
            offset,
            "codec round trip preserves record: offset should match the expected value"
        );
        assert_eq!(
            record_from_bytes.timestamp(),
            timestamp,
            "codec round trip preserves record: timestamp should match the expected value"
        );
        assert_eq!(
            record_from_bytes.key(),
            expected_key,
            "codec round trip preserves record: key should match the expected value"
        );
        assert_eq!(
            record_from_bytes.payload(),
            expected_payload,
            "codec round trip preserves record: payload should match the expected value"
        );
    }

    #[test]
    fn codec_encoding_matches_golden_bytes() {
        let record = Record::new(
            0x0102030405060708,
            0x1112131415161718,
            Some(vec![0xAA, 0xBB]),
            vec![0x10, 0x20, 0x30],
        );

        let expected_bytes = [
            0x52, 0x49, 0x56, 0x54, // Magic: RIVT
            0x01, // Version
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Offset
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // Timestamp
            0x01, // Key present
            0x00, 0x00, 0x00, 0x02, // Key length: 2
            0x00, 0x00, 0x00, 0x03, // Payload length: 3
            0xAA, 0xBB, // Key
            0x10, 0x20, 0x30, // Payload
            0x67, 0xAF, 0x80, 0x74, // CRC32
        ];

        let limits = RecordLimits::default();

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        assert_eq!(
            bytes, expected_bytes,
            "encoding should match the specified version 1 wire format exactly"
        );
    }

    #[test]
    fn codec_encoding_is_deterministic() {
        let limits = RecordLimits::default();

        let record = sample_record(0);

        let bytes1 = record
            .encode(&limits)
            .expect("record should encode successfully");

        let bytes2 = record
            .encode(&limits)
            .expect("record should encode successfully");

        assert_eq!(
            bytes2, bytes1,
            "encoding the same record twice should produce identical bytes"
        );
    }

    #[test]
    fn codec_round_trip_preserves_absent_key() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = None;
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let key_present = bytes[21];
        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            key_present, 0,
            "key presence flag should distinguish an absent key from a present empty key"
        );
        assert_eq!(
            record_from_bytes.key(),
            None,
            "codec round trip preserves absent key: key should match the expected value"
        );
    }

    #[test]
    fn codec_round_trip_preserves_empty_key() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let key_present = bytes[21];
        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            key_present, 1,
            "key presence flag should distinguish an absent key from a present empty key"
        );
        assert_eq!(
            record_from_bytes.key(),
            Some(vec![]).as_deref(),
            "codec round trip preserves empty key: key should match the expected value"
        );
    }

    #[test]
    fn codec_round_trip_preserves_empty_payload() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let (record_from_bytes, consumed) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes.payload(),
            &[],
            "codec round trip preserves empty payload: payload should match the expected value"
        );
        assert_eq!(
            consumed,
            bytes.len(),
            "decoder should consume exactly the encoded record bytes"
        );
    }

    #[test]
    fn codec_round_trip_preserves_binary_bytes() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![0x00, 0x80, 0xFF];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes.payload(),
            &[0x00, 0x80, 0xFF],
            "codec round trip preserves binary bytes: payload should match the expected value"
        );
    }

    #[test]
    fn codec_round_trip_preserves_integer_boundaries_zero() {
        let offset: u64 = 0;
        let timestamp: u64 = 0;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes.offset(),
            0,
            "codec round trip preserves integer boundaries zero: offset should match the expected value"
        );
        assert_eq!(
            record_from_bytes.timestamp(),
            0,
            "codec round trip preserves integer boundaries zero: timestamp should match the expected value"
        );
    }

    #[test]
    fn codec_round_trip_preserves_integer_boundaries_max() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");
        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes.offset(),
            u64::MAX,
            "codec round trip preserves integer boundaries max: offset should match the expected value"
        );
        assert_eq!(
            record_from_bytes.timestamp(),
            u64::MAX,
            "codec round trip preserves integer boundaries max: timestamp should match the expected value"
        );
    }

    #[test]
    fn codec_decode_consumes_exactly_one_record() {
        let offset1: u64 = 0;
        let timestamp1: u64 = 0;
        let key1: Option<Vec<u8>> = Some(vec![]);
        let payload1: Vec<u8> = vec![];
        let record1 = Record::new(offset1, timestamp1, key1, payload1);

        let offset2: u64 = u64::MAX;
        let timestamp2: u64 = u64::MAX;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload2: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record2 = Record::new(offset2, timestamp2, key2, payload2);

        let bytes1 = record1
            .encode(&limits)
            .expect("record should encode successfully");
        let bytes2 = record2
            .encode(&limits)
            .expect("record should encode successfully");

        let mut combined = bytes1;
        combined.extend_from_slice(&bytes2);

        let (record_from_bytes1, consumed1) =
            Record::decode(&combined, &limits).expect("first record should decode successfully");

        let (record_from_bytes2, consumed2) = Record::decode(&combined[consumed1..], &limits)
            .expect("second record should decode successfully");

        assert_eq!(
            record_from_bytes1, record1,
            "decoded record should match its corresponding original record"
        );
        assert_eq!(
            record_from_bytes2, record2,
            "decoded record should match its corresponding original record"
        );
        assert_eq!(
            consumed1 + consumed2,
            combined.len(),
            "decoder should consume exactly the encoded record bytes"
        );
    }

    #[test]
    fn codec_decode_rejects_every_truncated_prefix_header() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        for n in 0..30 {
            let truncated = &bytes[0..n];
            let result = Record::decode(truncated, &limits);
            assert_eq!(
                result,
                Err(CodecError::IncompleteHeader),
                "codec decode rejects every truncated prefix header: expected IncompleteHeader for prefix length {n}"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_every_truncated_prefix_body() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![1, 2, 3]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        let mut count = 0;

        for n in 30..(bytes.len() - 4) {
            let truncated = &bytes[0..n];
            let result = Record::decode(truncated, &limits);
            assert_eq!(
                result,
                Err(CodecError::IncompleteBody),
                "codec decode rejects every truncated prefix body: expected IncompleteBody for prefix length {n}"
            );
            count += 1;
        }

        assert!(
            count > 0,
            "test should have exercised at least one truncated body"
        );
    }

    #[test]
    fn codec_decode_rejects_every_truncated_prefix_checksum() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        for n in (bytes.len() - 4)..bytes.len() {
            let truncated = &bytes[0..n];
            let result = Record::decode(truncated, &limits);
            assert_eq!(
                result,
                Err(CodecError::IncompleteBody),
                "codec decode rejects every truncated prefix checksum: expected IncompleteBody for prefix length {n}"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_invalid_magic() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[0] = 0;

        let result = Record::decode(&bytes, &limits);
        assert_eq!(
            result,
            Err(CodecError::InvalidMagic),
            "codec decode rejects invalid magic: expected InvalidMagic"
        );
    }

    #[test]
    fn codec_decode_rejects_corruption_in_every_magic_byte() {
        for n in 0..4 {
            let record = sample_record(0);
            let mut bytes = record
                .encode(&RecordLimits::default())
                .expect("record should encode successfully");
            bytes[n] = 0;
            let result = Record::decode(&bytes, &RecordLimits::default());
            assert_eq!(
                result,
                Err(CodecError::InvalidMagic),
                "codec decode rejects corruption in every magic byte: expected InvalidMagic for byte index {n}"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_unsupported_version() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[4] = 0xFF;

        let result = Record::decode(&bytes, &limits);
        assert_eq!(
            result,
            Err(CodecError::UnsupportedVersion),
            "codec decode rejects unsupported version: expected UnsupportedVersion"
        );
    }

    #[test]
    fn codec_decode_rejects_invalid_key_presence() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        for n in 2..=255 {
            bytes[21] = n;
            let result = Record::decode(&bytes, &limits);
            assert_eq!(
                result,
                Err(CodecError::InvalidKeyPresence),
                "codec decode rejects invalid key presence: expected InvalidKeyPresence for flag {n}"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_absent_key_with_nonzero_length() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![1, 2, 3]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::default();
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[21] = 0;

        let result = Record::decode(&bytes, &limits);
        assert_eq!(
            result,
            Err(CodecError::InvalidKeyLength),
            "codec decode rejects absent key with nonzero length: expected InvalidKeyLength"
        );
    }

    #[test]
    fn codec_accepts_lengths_at_configured_limits() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![1, 2]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        let (record_from_bytes, _) =
            Record::decode(&bytes, &limits).expect("record should decode successfully");

        assert_eq!(
            record_from_bytes, record,
            "decoded record should match its corresponding original record"
        );
    }

    #[test]
    fn codec_encode_rejects_key_over_limit() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![1, 2, 3]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let result = record.encode(&limits);

        assert_eq!(
            result,
            Err(CodecError::KeyTooLarge),
            "codec encode rejects key over limit: expected KeyTooLarge"
        );
    }

    #[test]
    fn codec_encode_rejects_payload_over_limit() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![1, 2]);
        let payload: Vec<u8> = vec![1, 2, 3, 4];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let result = record.encode(&limits);

        assert_eq!(
            result,
            Err(CodecError::PayloadTooLarge),
            "codec encode rejects payload over limit: expected PayloadTooLarge"
        );
    }

    #[test]
    fn codec_decode_rejects_key_over_limit_before_body_allocation() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[22..26].copy_from_slice(&3u32.to_be_bytes());

        let buytes_truncated = &bytes[0..HEADER_LENGTH];

        let result = Record::decode(buytes_truncated, &limits);
        assert_eq!(
            result,
            Err(CodecError::KeyTooLarge),
            "codec decode rejects key over limit before body allocation: expected KeyTooLarge"
        );
    }

    #[test]
    fn codec_decode_rejects_payload_over_limit_before_body_allocation() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[26..30].copy_from_slice(&4u32.to_be_bytes());

        let buytes_truncated = &bytes[0..HEADER_LENGTH];
        let result = Record::decode(buytes_truncated, &limits);
        assert_eq!(
            result,
            Err(CodecError::PayloadTooLarge),
            "codec decode rejects payload over limit before body allocation: expected PayloadTooLarge"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_payload() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[HEADER_LENGTH + 1] ^= 4;

        let result = Record::decode(&bytes, &limits);
        assert_eq!(
            result,
            Err(CodecError::InvalidChecksum),
            "codec decode rejects corrupted payload: expected InvalidChecksum"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_offset() {
        for n in 5..13 {
            let record = sample_record(0);
            let mut bytes = record
                .encode(&RecordLimits::default())
                .expect("encoding should succeed");
            bytes[n] ^= 0xFF;
            let error =
                Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
            assert_eq!(
                error,
                CodecError::InvalidChecksum,
                "codec decode rejects corrupted offset: expected InvalidChecksum"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_corrupted_timestamp() {
        for n in 13..21 {
            let record = sample_record(0);
            let mut bytes = record
                .encode(&RecordLimits::default())
                .expect("encoding should succeed");
            bytes[n] ^= 0xFF;
            let error =
                Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
            assert_eq!(
                error,
                CodecError::InvalidChecksum,
                "codec decode rejects corrupted timestamp: expected InvalidChecksum"
            );
        }
    }

    #[test]
    fn codec_decode_rejects_corrupted_valid_key_presence() {
        let record = Record::new(0, 1_700_000_000, Some(vec![]), vec![1, 2, 3]);
        let mut bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding should succeed");
        bytes[21] = 0;
        let error =
            Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
        assert_eq!(
            error,
            CodecError::InvalidChecksum,
            "codec decode rejects corrupted valid key presence: expected InvalidChecksum"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_key_length() {
        let record = sample_record(0);
        let mut bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding should succeed");
        let encoded_key_len = bytes[22..26]
            .try_into()
            .map(u32::from_be_bytes)
            .expect("slice with incorrect length");
        bytes[22..26].copy_from_slice(&(encoded_key_len - 1).to_be_bytes());
        let error =
            Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
        assert_eq!(
            error,
            CodecError::InvalidChecksum,
            "codec decode rejects corrupted key length: expected InvalidChecksum"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_payload_length() {
        let record = sample_record(0);
        let mut bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding should succeed");
        let encoded_payload_len = bytes[26..30]
            .try_into()
            .map(u32::from_be_bytes)
            .expect("slice with incorrect length");
        bytes[26..30].copy_from_slice(&(encoded_payload_len - 1).to_be_bytes());
        let error =
            Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
        assert_eq!(
            error,
            CodecError::InvalidChecksum,
            "codec decode rejects corrupted payload length: expected InvalidChecksum"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_key() {
        let record = sample_record(0);
        let mut bytes = record
            .encode(&RecordLimits::default())
            .expect("encoding should succeed");
        bytes[30] ^= 0xFF; // Corrupt a key byte
        let error =
            Record::decode(&bytes, &RecordLimits::default()).expect_err("decoding should fail");
        assert_eq!(
            error,
            CodecError::InvalidChecksum,
            "codec decode rejects corrupted key: expected InvalidChecksum"
        );
    }

    #[test]
    fn codec_decode_rejects_corrupted_checksum() {
        let offset: u64 = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let limits = RecordLimits::new(2, 3);
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        let len = bytes.len();

        bytes[len - 1] = 3;

        let result = Record::decode(&bytes, &limits);
        assert_eq!(
            result,
            Err(CodecError::InvalidChecksum),
            "codec decode rejects corrupted checksum: expected InvalidChecksum"
        );
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn codec_length_conversion_rejects_unrepresentable_length() {
        let len =
            usize::try_from(u32::MAX).expect("usize should be able to represent u32::MAX") + 1;
        let result = Record::check_len(len, u32::MAX, CodecError::KeyTooLarge);

        assert_eq!(
            result,
            Err(CodecError::LengthOverflow),
            "codec length conversion rejects unrepresentable length: expected LengthOverflow"
        );
    }

    #[test]
    #[cfg(target_pointer_width = "32")]
    fn codec_decode_rejects_record_size_overflow() {
        let offset = u64::MAX;
        let timestamp: u64 = u64::MAX;
        let key: Option<Vec<u8>> = Some(vec![]);
        let payload: Vec<u8> = vec![];
        let limits = RecordLimits::new(u32::MAX, u32::MAX);
        let record = Record::new(offset, timestamp, key, payload);

        let mut bytes = record
            .encode(&limits)
            .expect("record should encode successfully");

        bytes[22..26].copy_from_slice(&(u32::MAX - 30).to_be_bytes());
        bytes[26..30].copy_from_slice(&1u32.to_be_bytes());

        let result = Record::decode(&bytes[0..HEADER_LENGTH], &limits);
        assert_eq!(
            result,
            Err(CodecError::LengthOverflow),
            "codec decode rejects record size overflow: expected LengthOverflow"
        );
    }
}
