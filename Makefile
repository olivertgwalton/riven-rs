.PHONY: fmt fmt-check check lint test docs docs-check verify

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

check:
	cargo check --workspace --all-targets

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

docs:
	cargo run --quiet --bin gen-docs

# Fails when docs/plugins is out of date with the plugins' settings schemas.
docs-check:
	cargo run --quiet --bin gen-docs -- --check

verify: fmt-check check lint test docs-check
