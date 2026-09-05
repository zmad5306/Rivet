#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    offset: u64,
    timestamp: u64,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

impl Record {

    pub fn new(offset: u64, timestamp: u64, key: Option<Vec<u8>>, payload: Vec<u8>) -> Self {
        return Self {
            offset,
            timestamp,
            key,
            payload,
        };
    }

    pub fn offset(&self) -> u64 {
        return self.offset;
    }

    pub fn timestamp(&self) -> u64 {
        return self.timestamp;
    }

    pub fn key(&self) -> Option<&[u8]> {
        return self.key.as_deref();
    }

    pub fn payload(&self) -> &[u8] {
        return self.payload.as_ref();
    }

}

#[derive(Debug, PartialEq, Eq)]
pub struct PublishInput {
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

impl PublishInput {

    pub fn new(key: Option<Vec<u8>>, payload: Vec<u8>) -> Self {
        return Self {
            key,
            payload
        };
    }

    pub fn key(&self) -> Option<&[u8]> {
        return self.key.as_deref();
    }

    pub fn payload(&self) -> &[u8] {
        return self.payload.as_ref();
    }

}

#[test]
fn record_constructor_preserves_all_fields() {
    let offset: u64 = 0;
    let timestamp: u64 = 1_700_000_000;
    let key: Option<Vec<u8>> = Some(vec!(10, 20, 30));
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
    todo!("Construct with None and verify key() returns None.");
}

#[test]
fn record_empty_key_is_preserved() {
    todo!("Construct with Some(empty bytes) and verify the key remains present and empty.");
}

#[test]
fn record_empty_payload_is_preserved() {
    todo!("Construct with an empty payload and verify payload() is empty.");
}

#[test]
fn record_non_utf8_bytes_are_preserved() {
    todo!("Verify both key and payload preserve bytes including 0xFF, 0xFE, and 0x00.");
}

#[test]
fn record_integer_boundaries_are_preserved() {
    todo!("Verify offset and timestamp preserve both 0 and u64::MAX.");
}

#[test]
fn records_with_identical_fields_are_equal() {
    todo!("Construct separate records with identical fields and compare them.");
}

#[test]
fn records_with_different_offsets_are_unequal() {
    todo!("Change only offsets and verify the records are unequal.");
}

#[test]
fn records_with_different_timestamps_are_unequal() {
    todo!("Change only timestamps and verify the records are unequal.");
}

#[test]
fn records_with_different_keys_are_unequal() {
    todo!("Change only keys and verify the records are unequal.");
}

#[test]
fn records_with_different_payloads_are_unequal() {
    todo!("Change only payloads and verify the records are unequal.");
}

#[test]
fn records_with_absent_and_empty_keys_are_unequal() {
    todo!("Compare otherwise identical records with None and Some(empty bytes).");
}

#[test]
fn publish_input_constructor_preserves_all_fields() {
    let key: Option<Vec<u8>> = Some(vec!(10, 20, 30));
    let expected_key: Option<&[u8]> = Some(&[10, 20, 30]);
    let payload: Vec<u8> = vec![1, 2, 3];
    let expected_payload: &[u8] = &[1, 2, 3];

    let input = PublishInput::new(key, payload);

    assert_eq!(input.key(), expected_key);
    assert_eq!(input.payload(), expected_payload);
}

#[test]
fn publish_input_absent_key_is_preserved() {
    todo!("Construct with None and verify key() returns None.");
}

#[test]
fn publish_input_empty_key_is_preserved() {
    todo!("Construct with Some(empty bytes) and verify the key remains present and empty.");
}

#[test]
fn publish_input_empty_payload_is_preserved() {
    todo!("Construct with an empty payload and verify payload() is empty.");
}

#[test]
fn publish_input_non_utf8_bytes_are_preserved() {
    todo!("Verify both key and payload preserve bytes including 0xFF, 0xFE, and 0x00.");
}

#[test]
fn publish_inputs_with_identical_fields_are_equal() {
    todo!("Construct separate inputs with identical fields and compare them.");
}

#[test]
fn publish_inputs_with_different_keys_are_unequal() {
    todo!("Change only keys and verify the inputs are unequal.");
}

#[test]
fn publish_inputs_with_different_payloads_are_unequal() {
    todo!("Change only payloads and verify the inputs are unequal.");
}

#[test]
fn publish_inputs_with_absent_and_empty_keys_are_unequal() {
    todo!("Compare otherwise identical inputs with None and Some(empty bytes).");
}
