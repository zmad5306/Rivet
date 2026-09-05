use rivet::version;

#[test]
fn version_is_exposed() {
    assert!(!version().is_empty());
}
