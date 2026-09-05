#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
