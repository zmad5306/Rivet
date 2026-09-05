use rivet::version;

#[test]
fn version_returns_non_empty_string() {
    assert!(!version().is_empty());
}
