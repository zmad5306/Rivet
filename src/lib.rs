pub fn is_valid_topic_name(name: &str) -> bool {
    !name.is_empty()
}

#[test]
fn empty_not_allowd() {
    assert!(!is_valid_topic_name(""));
}

#[test]
fn any_string_allowed() {
    assert!(is_valid_topic_name("some string"));
}
