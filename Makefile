.PHONY: fmt fmt-check check lint test docs docs-check verify \
        frontend-install frontend-build frontend-check frontend-lint \
        schema schema-check verify-all

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

# Rust only, matching CI's `verify` job. `verify-all` is the local equivalent of
# the full pipeline.
verify: fmt-check check lint test docs-check

# ── Frontend ──────────────────────────────────────────────────────────────────

frontend-install:
	cd frontend && pnpm install --frozen-lockfile

frontend-build:
	cd frontend && pnpm run build

frontend-check:
	cd frontend && pnpm run check

frontend-lint:
	cd frontend && pnpm run lint

# ── Schema ────────────────────────────────────────────────────────────────────

# Regenerates schema.graphql from the resolvers, then the frontend's types from
# it. Both artefacts are committed, so run this after changing the GraphQL API.
schema:
	cargo run --quiet -p riven-api --example dump_schema
	cd frontend && pnpm run codegen

# Fails when either committed artefact is stale — the same check CI runs.
schema-check: schema
	@git diff --quiet -- schema.graphql frontend/src/lib/gql || { \
		echo "schema.graphql or frontend/src/lib/gql is out of date; commit the regenerated files"; \
		git --no-pager diff --stat -- schema.graphql frontend/src/lib/gql; \
		exit 1; \
	}

verify-all: verify frontend-lint frontend-check frontend-build schema-check
