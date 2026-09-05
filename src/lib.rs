pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[test]
fn version_is_exposed() {
    assert!(!version().is_empty());
}
