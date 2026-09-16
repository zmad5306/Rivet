use crate::error::TopicNameError;

#[derive(Debug, PartialEq, Eq)]
pub struct TopicName {
    value: String,
}

impl TopicName {
    pub fn new(value: String) -> Result<Self, TopicNameError> {
        if value.is_empty() || value.len() > 128 {
            return Err(TopicNameError::InvalidLength {
                actual: value.len(),
            });
        }
        if let Some(character) = value
            .chars()
            .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_')
        {
            return Err(TopicNameError::InvalidCharacter { character });
        }
        Ok(Self { value })
    }
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use crate::error::TopicNameError;

    #[test]
    fn valid_topic_names_preserve_spelling() {
        let long_name = "a".repeat(128);
        let valid_names = vec![
            "a",
            "Z",
            "0",
            "9",
            "-",
            "_",
            "abc-123_DEF",
            "1234567890",
            long_name.as_str(),
        ];
        let mut number_of_assertions = 0;
        for name in valid_names {
            let topic_name = super::TopicName::new(name.to_string()).unwrap();
            assert_eq!(topic_name.as_str(), name);
            number_of_assertions += 1;
        }
        assert_eq!(number_of_assertions, 9);
    }

    #[test]
    fn topic_name_accepts_length_boundaries() {
        let mut number_of_assertions = 0;
        for i in 1..=128 {
            let name = "a".repeat(i);
            let topic_name = super::TopicName::new(name.clone()).unwrap();
            assert_eq!(topic_name.as_str(), name);
            number_of_assertions += 1;
        }
        assert_eq!(number_of_assertions, 128);
    }

    #[test]
    fn topic_name_rejects_empty_name() {
        let result = super::TopicName::new("".to_string());
        assert!(matches!(
            result,
            Err(TopicNameError::InvalidLength { actual: 0 })
        ));
    }

    #[test]
    fn topic_name_rejects_name_longer_than_limit() {
        let name = "a".repeat(129);
        let error = super::TopicName::new(name)
            .expect_err("Expected error for name longer than 128 characters");
        assert!(matches!(
            error,
            TopicNameError::InvalidLength { actual: 129 }
        ));
    }

    #[test]
    fn topic_name_rejects_invalid_characters() {
        let invalid_topic_names = vec![
            "ü",  // Unicode
            " ",  // Space
            "\t", // Tab
            "\n", // Newline
            "!",  // Punctuation
            "\0", // NUL
        ];
        let mut number_of_assertions = 0;
        for name in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == name.chars().next().unwrap())
            );
            number_of_assertions += 1;
        }
        assert_eq!(number_of_assertions, 6);
    }

    #[test]
    fn topic_name_rejects_paths_and_traversal() {
        let invalid_topic_names = vec![".", "..", "/", "\\", "/etc/passwd", "../traversal"];
        let mut number_of_assertions = 0;
        for name in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == name.chars().next().unwrap())
            );
            number_of_assertions += 1;
        }
        assert_eq!(number_of_assertions, 6);
    }

    #[test]
    fn topic_name_reports_first_invalid_character() {
        let invalid_topic_names = vec![["valid_prefixü!", "ü"], ["valid_prefix !", " "]];
        let mut number_of_assertions = 0;
        for [name, first_invalid_char] in invalid_topic_names {
            let result = super::TopicName::new(name.to_string());
            assert!(
                matches!(result, Err(TopicNameError::InvalidCharacter { character: c }) if c == first_invalid_char.chars().next().expect("Expected a first invalid character"))
            );
            number_of_assertions += 1;
        }
        assert_eq!(number_of_assertions, 2);
    }
}
