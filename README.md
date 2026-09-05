# Rivet

Rivet is a small, single-node durable event broker built as a Rust systems-programming learning project.

- [Architecture and design](docs/DESIGN.md)
- [AI collaboration contract](AGENTS.md)

Implementation is organized as 24 sequential GitHub milestones. The learner writes production Rust code; AI may explain, review, suggest tests, and help debug without supplying implementations unless explicitly requested.

## Toolchain

Uses stable rust, built and tested with `1.95.0`.

Required tooling may be installed with `rustup component add rustfmt clippy`

### Build

Build the project with `cargo build`.

### Test

Test the project with `cargo test`.

### Format

Format the code with `cargo fmt`.

Check the format of the code with `cargo fmt --check`

### Lint

Check the code for lint issues with `cargo clippy --all-targets -- -D warnings`.

### All Checks

All the previous checks may be run with the `./scripts/check.sh` script.
