# Contributing

## Engineering Standards

This workspace enforces the following baseline standards:

- All Rust crates in this workspace target the Rust 2024 edition.
- All Rust code must be formatted with `cargo fmt --all` using the Rust 2024 style edition.
- New code must pass `cargo clippy --workspace --all-targets -- -D warnings`.
- Changes must preserve or improve test coverage for the code they touch.
- Configuration and parsing logic should live in shared crates instead of app entrypoints.
- New plugin and workflow behaviour should prefer small, testable units over large inline functions.
- Database access goes through SeaORM (`riven-db`); prefer the query builder and reserve raw `Statement` for SQL the builder can't express (recursive CTEs, window functions, partial-index upserts, etc.).

## Local Workflow

See **[docs/development.md](docs/development.md)** for getting Riven running —
required environment, Docker vs native, and the frontend and schema workflows.

Run the full standards gate before opening or updating a change:

```sh
make verify       # Rust: fmt, check, clippy, tests, generated docs
make verify-all   # the above plus frontend lint/check/build and schema drift
```

Useful individual targets:

```sh
make fmt
make fmt-check
make check
make lint
make test
make docs         # regenerate docs/environment.md and docs/plugins
make schema       # regenerate schema.graphql and the frontend's types
```

`make verify` includes `docs-check`, and `docs/environment.md` records the file
and line of every `env::var` call — so moving code makes it stale. Run
`make docs` and commit the result.

## Testing Expectations

- Add unit tests for parsing, configuration, ranking, and helper logic.
- Add regression tests when fixing bugs.
- Prefer focused tests in the crate that owns the behaviour.
- Avoid merging behaviour changes without an executable assertion unless the code is inherently integration-only.

## Code Review Bar

- Keep functions small enough to reason about without hidden control flow.
- Avoid duplicating configuration merge logic across crates.
- Prefer explicit names over comments for straightforward code.
- Do not weaken lint or test gates to land a change.
