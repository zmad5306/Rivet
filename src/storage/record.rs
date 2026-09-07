use crate::error::StorageError;
use crc32fast::hash;

const MAGIC: &[u8; 4] = b"RIVT";
const VERSION: u8 = 1;
const HEADER_LENGTH: usize = 30;

#[derive(Debug, PartialEq, Eq)]
pub struct RecordLimits {
    max_key_bytes: u32,
    max_payload_bytes: u32,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_payload_bytes: 1024 * 1024,
        }
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

    fn check_len(len: usize, max: u32, error: StorageError) -> Result<u32, StorageError> {
        match u32::try_from(len) {
            Ok(l) => {
                if l > max {
                    return Err(error);
                }
                return Ok(l);
            }
            Err(_) => return Err(StorageError::LengthOverflow),
        }
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

    pub fn encode(&self, limits: &RecordLimits) -> Result<Vec<u8>, StorageError> {
        let key_present = self.key().map_or(0, |_| 1);
        let key_len = self.key().as_ref().map_or(0, |key| key.len());
        let key_length = Self::check_len(key_len, limits.max_key_bytes, StorageError::KeyTooLarge)?;
        let payload_length = Self::check_len(
            self.payload().len(),
            limits.max_payload_bytes,
            StorageError::PaylodTooLarge,
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

    pub fn decode(bytes: &[u8], limits: &RecordLimits) -> Result<(Self, usize), StorageError> {
        if bytes.len() < HEADER_LENGTH {
            return Err(StorageError::IncompleteHeader);
        }

        if &bytes[0..4] != MAGIC {
            return Err(StorageError::InvalidMagic);
        }

        if bytes[4] != VERSION {
            return Err(StorageError::UnsupportedVersion);
        }

        let offset_bytes: [u8; 8] = bytes[5..13]
            .try_into()
            .expect("header checked; offset slice is exactly eight bytes");

        let timestamp_bytes: [u8; 8] = bytes[13..21]
            .try_into()
            .expect("header checked; timestamp slice is exactly eight bytes");

        let key_length_bytes: [u8; 4] = bytes[22..26]
            .try_into()
            .expect("header checked; key length slice is exactly four bytes");

        let payload_length_bytes: [u8; 4] = bytes[26..30]
            .try_into()
            .expect("header checked; payload length slice is exactly four bytes");

        let key_present = bytes[21];
        let offset = u64::from_be_bytes(offset_bytes);
        let timestamp = u64::from_be_bytes(timestamp_bytes);
        let key_length = u32::from_be_bytes(key_length_bytes);
        let payload_length = u32::from_be_bytes(payload_length_bytes);

        if key_present != 0 && key_present != 1 {
            return Err(StorageError::InvalidKeyPresence);
        }

        if key_present == 0 && key_length > 0 {
            return Err(StorageError::InvalidKeyLength);
        }

        if key_length > limits.max_key_bytes {
            return Err(StorageError::KeyTooLarge);
        }

        if payload_length > limits.max_payload_bytes {
            return Err(StorageError::PaylodTooLarge);
        }

        let key_len = match usize::try_from(key_length) {
            Ok(len) => len,
            Err(_) => return Err(StorageError::LengthOverflow),
        };

        let payload_len = match usize::try_from(payload_length) {
            Ok(len) => len,
            Err(_) => return Err(StorageError::LengthOverflow),
        };

        let key_end = match HEADER_LENGTH.checked_add(key_len) {
            Some(end) => end,
            None => return Err(StorageError::LengthOverflow),
        };

        let payload_end = match key_end.checked_add(payload_len) {
            Some(end) => end,
            None => return Err(StorageError::LengthOverflow),
        };

        let record_end = match payload_end.checked_add(4) {
            Some(end) => end,
            None => return Err(StorageError::LengthOverflow),
        };

        if bytes.len() < record_end {
            return Err(StorageError::IncompleteBody);
        }

        let checksum_bytes: [u8; 4] = bytes[payload_end..record_end]
            .try_into()
            .expect("record bounds checked; checksum slice is exactly four bytes");

        let checksum = u32::from_be_bytes(checksum_bytes);
        let computed_checksum = hash(&bytes[4..payload_end]);

        if checksum != computed_checksum {
            return Err(StorageError::InvalidChecksum);
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
    use super::{PublishInput, Record};

    #[test]
    fn record_constructor_preserves_all_fields() {
        let offset: u64 = 0;
        let timestamp: u64 = 1_700_000_000;
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let record = Record::new(offset, timestamp, key, payload);

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record.offset(), offset);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.key(), expected_key);
        assert_eq!(record.payload(), expected_payload);
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

        assert_eq!(record1, record2);
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

        assert_ne!(record1, record2);
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

        assert_ne!(record1, record2);
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

        assert_ne!(record1, record2);
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

        assert_ne!(record1, record2);
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

        assert_ne!(record1, record2);
    }

    #[test]
    fn publish_input_constructor_preserves_all_fields() {
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(input.key(), expected_key);
        assert_eq!(input.payload(), expected_payload);
    }

    #[test]
    fn publish_input_absent_key_is_preserved() {
        let key: Option<Vec<u8>> = None;
        let expected_key: Option<&[u8]> = None;
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(input.key(), expected_key);
        assert_eq!(input.payload(), expected_payload);
    }

    #[test]
    fn publish_input_empty_key_is_preserved() {
        let key: Option<Vec<u8>> = Some(vec![]);
        let expected_key: Option<&[u8]> = Some(&[]);
        let payload: Vec<u8> = vec![1, 2, 3];
        let expected_payload: &[u8] = &[1, 2, 3];

        let input = PublishInput::new(key, payload);

        assert_eq!(input.key(), expected_key);
        assert_eq!(input.payload(), expected_payload);
    }

    #[test]
    fn publish_input_empty_payload_is_preserved() {
        let key: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
        let payload: Vec<u8> = vec![];
        let expected_payload: &[u8] = &[];

        let input = PublishInput::new(key, payload);

        assert_eq!(input.key(), expected_key);
        assert_eq!(input.payload(), expected_payload);
    }

    #[test]
    fn publish_input_non_utf8_bytes_are_preserved() {
        let key: Option<Vec<u8>> = Some(vec![0xFF, 0xFE, 0x00]);
        let expected_key: Option<&[u8]> = Some(&[0xFF, 0xFE, 0x00]);
        let payload: Vec<u8> = vec![0xFF, 0xFE, 0x00];
        let expected_payload: &[u8] = &[0xFF, 0xFE, 0x00];

        let input = PublishInput::new(key, payload);

        assert_eq!(input.key(), expected_key);
        assert_eq!(input.payload(), expected_payload);
    }

    #[test]
    fn publish_inputs_with_identical_fields_are_equal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_eq!(input1, input2);
    }

    #[test]
    fn publish_inputs_with_different_keys_are_unequal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![20, 30, 40]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(input1, input2);
    }

    #[test]
    fn publish_inputs_with_different_payloads_are_unequal() {
        let key1: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let key2: Option<Vec<u8>> = Some(vec![10, 20, 30]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![2, 3, 4];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(input1, input2);
    }

    #[test]
    fn publish_inputs_with_absent_and_empty_keys_are_unequal() {
        let key1: Option<Vec<u8>> = None;
        let key2: Option<Vec<u8>> = Some(vec![]);
        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![1, 2, 3];

        let input1 = PublishInput::new(key1, payload1);
        let input2 = PublishInput::new(key2, payload2);

        assert_ne!(input1, input2);
    }

    // Codec exercises: replace todo!() as each test is implemented.

    #[test]
    fn codec_round_trip_preserves_record() {
        // Encode a record with a nonempty key and payload, decode it, and compare all fields via equality. Assert consumed bytes equals encoded length.
        todo!();
    }

    #[test]
    fn codec_encoding_matches_golden_bytes() {
        // Compare encoding of a small known record against independently specified bytes, including flag, big-endian fields, and CRC32. Do not generate expected bytes with encode.
        todo!();
    }

    #[test]
    fn codec_encoding_is_deterministic() {
        // Encode the same record twice and assert the buffers are identical.
        todo!();
    }

    #[test]
    fn codec_round_trip_preserves_absent_and_empty_keys() {
        // Round-trip None and Some(vec![]) separately. Assert they remain distinct and their presence flags are 0 and 1.
        todo!();
    }

    #[test]
    fn codec_round_trip_preserves_empty_payload() {
        // Round-trip a record with an empty payload and verify the consumed byte count includes the checksum.
        todo!();
    }

    #[test]
    fn codec_round_trip_preserves_binary_bytes() {
        // Round-trip key and payload containing 0x00, 0x80, and 0xFF without text conversion.
        todo!();
    }

    #[test]
    fn codec_round_trip_preserves_integer_boundaries() {
        // Round-trip offset and timestamp values of 0 and u64::MAX.
        todo!();
    }

    #[test]
    fn codec_decode_consumes_exactly_one_record() {
        // Concatenate two encoded records. Decode the first, then decode the remainder using its consumed count. Assert both records and counts.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_every_truncated_prefix() {
        // For every proper prefix of a valid encoded record, expect IncompleteHeader below 30 bytes and IncompleteBody otherwise. Include missing checksum bytes.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_invalid_magic() {
        // Change a magic byte in a complete valid record and expect InvalidMagic.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_unsupported_version() {
        // Change the version byte and expect UnsupportedVersion before checksum validation.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_invalid_key_presence() {
        // Set the presence flag to 2 and 255 in complete records and expect InvalidKeyPresence.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_absent_key_with_nonzero_length() {
        // Set the flag to 0 while the key length is nonzero and expect InvalidKeyLength.
        todo!();
    }

    #[test]
    fn codec_accepts_lengths_at_configured_limits() {
        // Use small custom limits and round-trip key and payload lengths exactly at their maxima. Also cover zero limits with empty fields.
        todo!();
    }

    #[test]
    fn codec_encode_rejects_key_over_limit() {
        // Use a key one byte above a small configured maximum and expect KeyTooLarge.
        todo!();
    }

    #[test]
    fn codec_encode_rejects_payload_over_limit() {
        // Use a payload one byte above a small configured maximum and expect the payload-too-large variant.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_key_over_limit_before_body_allocation() {
        // Supply a complete header claiming a key above the configured limit without supplying its body. Expect KeyTooLarge, not IncompleteBody.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_payload_over_limit_before_body_allocation() {
        // Supply a complete header claiming a payload above the configured limit without supplying its body. Expect the payload-too-large error.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_corrupted_payload() {
        // Flip a payload byte without updating the checksum and expect InvalidChecksum.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_corrupted_checksum() {
        // Flip a stored checksum byte in an otherwise valid record and expect InvalidChecksum.
        todo!();
    }

    #[test]
    fn codec_length_conversion_rejects_unrepresentable_length() {
        // On targets where usize is wider than u32, call check_len with a length above u32::MAX and expect LengthOverflow without allocating a huge vector. Gate this case by target width.
        todo!();
    }

    #[test]
    fn codec_decode_rejects_record_size_overflow() {
        // On a 32-bit target, use permitted u32 header lengths whose combined record size overflows usize. Expect LengthOverflow without allocating a body. Gate this case by target width; two u32 lengths cannot overflow usize on a 64-bit target.
        todo!();
    }
}
