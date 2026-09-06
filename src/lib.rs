pub mod broker;
pub mod error;
pub mod storage;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn version_returns_non_empty_string() {
        assert!(!version().is_empty());
    }
}
