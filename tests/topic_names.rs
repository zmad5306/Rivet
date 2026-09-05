use rivet::is_valid_topic_name;

#[test]
fn empty_not_allowd() {
    assert!(!is_valid_topic_name(""));
}

#[test]
fn any_string_allowed() {
    assert!(is_valid_topic_name("some string"));
}
