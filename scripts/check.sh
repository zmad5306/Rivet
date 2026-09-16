#!/bin/bash
set -e

verbose=false
case "$#:$1" in
    0:) ;;
    1:-v|1:--verbose) verbose=true ;;
    *)
        echo "Usage: $0 [-v|--verbose]" >&2
        exit 2
        ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/.."

check_output=$(mktemp)
trap 'rm -f "$check_output"' EXIT

run_check() {
    local status
    if "$verbose"; then
        "$@"
        return "$?"
    fi

    if "$@" >"$check_output" 2>&1; then
        return 0
    else
        status=$?
        cat "$check_output" >&2
        return "$status"
    fi
}

run_check cargo fmt --check
run_check cargo clippy --all-targets -- -D warnings
run_check cargo test

printf '\033[32m✓ All checks passed.\033[0m\n'
