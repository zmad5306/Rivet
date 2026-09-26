mod offset_store;

pub(crate) use offset_store::OffsetStore;

use crate::error::ConsumerGroupNameError;

pub(crate) const OFFSET_STORE_DIR: &str = "__consumer_offsets";

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub(crate) struct ConsumerGroupName {
    value: String,
}

impl ConsumerGroupName {
    pub fn new(name: String) -> Result<Self, ConsumerGroupNameError> {
        if name.is_empty() || name.len() > 128 {
            return Err(ConsumerGroupNameError::InvalidLength { actual: name.len() });
        }

        let invalid_char = name
            .chars()
            .find(|&c| !c.is_ascii_alphanumeric() && c != '-' && c != '_');

        if let Some(c) = invalid_char {
            return Err(ConsumerGroupNameError::InvalidCharacter { character: c });
        }

        Ok(Self { value: name })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use crate::{consumer::ConsumerGroupName, error::ConsumerGroupNameError};

    #[test]
    fn consumer_group_name_accepts_valid_boundary_lengths() {
        for len in 1..=128 {
            let name: String = "a".repeat(len);
            let consumer_group_name =
                ConsumerGroupName::new(name.clone()).expect("should succeed for valid length");

            assert_eq!(consumer_group_name.as_str(), name);
        }
    }

    #[test]
    fn consumer_group_name_rejects_lengths_outside_one_through_128_bytes() {
        let empty_name = String::new();
        let too_long_name = "a".repeat(129);

        assert!(
            matches!(
                ConsumerGroupName::new(empty_name),
                Err(ConsumerGroupNameError::InvalidLength { actual: 0 })
            ),
            "empty name should be rejected as InvalidLength"
        );
        assert!(
            matches!(
                ConsumerGroupName::new(too_long_name),
                Err(ConsumerGroupNameError::InvalidLength { actual: 129 })
            ),
            "too long name should be rejected as InvalidLength"
        );
    }

    #[test]
    fn consumer_group_name_rejects_every_disallowed_ascii_character_and_unicode() {
        let disallowed_ascii_chars: Vec<char> = (0..=127)
            .map(char::from)
            .filter(|&c| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
            .collect();

        let mut asserted = false;

        for c in disallowed_ascii_chars {
            let name = c.to_string();
            let result = ConsumerGroupName::new(name.clone());
            assert!(
                matches!(result, Err(ConsumerGroupNameError::InvalidCharacter { character: ch }) if ch == c),
                "character {c:?} should be rejected as InvalidCharacter"
            );
            asserted = true;
        }

        assert!(
            asserted,
            "at least one disallowed ASCII character should have been tested"
        );

        let name = "é".to_string();
        // Pass a non-ASCII character whose UTF-8 byte length remains within 1..=128.
        let result = ConsumerGroupName::new(name.clone());
        assert!(
            matches!(result, Err(ConsumerGroupNameError::InvalidCharacter { character: ch }) if ch == 'é'),
            "character 'é' should be rejected as InvalidCharacter"
        );
    }

    #[test]
    fn consumer_group_name_preserves_a_representative_valid_name() {
        let name = "fraud-detector_2".to_string();
        let consumer_group_name =
            ConsumerGroupName::new(name.clone()).expect("should succeed for valid name");
        assert_eq!(consumer_group_name.as_str(), name);
    }
}
