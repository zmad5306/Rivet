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
}

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
