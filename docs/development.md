# Development

Riven is one repo and one process: the Rust binary serves the SvelteKit SPA
itself from `RIVEN_STATIC_DIR`, so the UI and the API are always the same
origin. That is deliberate — it keeps the session cookie first-party, and
WebAuthn binds a passkey to the origin it was registered on, so a second origin
in development would register passkeys that do not work in production.

Two consequences worth knowing before you start:

- **There is no `vite dev` server.** Building the bundle and letting riven serve
  it is the development loop. See [Working on the frontend](#working-on-the-frontend).
- **The binary does not read `.env`.** It reads real environment variables.
  Docker Compose injects `.env` via `env_file`; a native run does not.

## Required configuration

Riven refuses to start without this, and the error names it:

| Variable | Why |
| --- | --- |
| `RIVEN_SETTING__API_KEY` | Authenticates machine callers (Overseerr/Jellyseerr webhooks) and seeds the Stremio addon token. |

Generate it with `openssl rand -hex 32`.

`RIVEN_SETTING__PUBLIC_URL` is not required but matters: it is the origin
passkeys are bound to, and changing it later invalidates every registered
passkey. Its scheme also decides cookie security — an `https` value gets
`Secure` cookies with the `__Host-` prefix, so set it to the URL browsers
actually use rather than an internal address.

`/auth` is rate limited to 100 requests/minute per client, tightened to 3 per
10 seconds on sign-in, sign-up and password reset. Callers are identified by
socket address only — no forwarded-IP header is trusted, since a client could
set one and hand itself a fresh budget — so behind a reverse proxy every
request looks like the proxy and all users share one bucket.

The first account created through the UI becomes the admin, and sign-up closes
permanently after it. Every later account is created by that admin.

## The full stack, in Docker

This is what CI builds and what production runs.

```sh
cp .env.example .env
# set RIVEN_SETTING__API_KEY, RIVEN_STORAGE_PATH
docker compose up --build
```

Starts PostgreSQL, Redis and riven; the image builds both the frontend and the
binary. Everything is on <http://localhost:8080>.

To build only the image:

```sh
docker build -t riven .
```

## Running natively

Faster for iterating on Rust, but you have to supply what Compose otherwise
provides.

**1. Build the bundle once.** `RIVEN_STATIC_DIR` defaults to `./frontend/build`,
which is where `adapter-static` writes, so running from the repo root needs no
configuration:

```sh
make frontend-install frontend-build
```

**2. Export the environment.** The binary has no dotenv:

```sh
set -a; source .env; set +a
```

**3. Reach PostgreSQL and Redis.** The Compose services publish no ports — they
are only reachable on the Compose network, and `RIVEN_SETTING__DATABASE_URL` in
`.env` points at the hostname `postgres`. A native run cannot dial either. Add
an override to publish them:

```yaml
# compose.override.yaml
services:
    postgres:
        ports: ["5432:5432"]
    redis:
        ports: ["6379:6379"]
```

and point `RIVEN_SETTING__DATABASE_URL` at `postgresql://riven:riven@localhost/riven`.
Forwarding into the Compose network with something like `socat` works too, and
avoids exposing the databases on the host.

**4. Run it:**

```sh
cargo run -p riven-app --bin riven
```

> On macOS the `fuser` crate needs `pkg-config` and `PKG_CONFIG_PATH` for
> macFUSE at build time. If those come from a tool manager, run cargo through it
> — e.g. `mise exec -- cargo run …` — or the build fails in `fuser`'s build
> script with advice to install a package you may not want.

## Working on the frontend

```sh
cd frontend && pnpm build
```

Then refresh the browser. `ServeDir` reads from disk per request, so riven does
not need restarting for a frontend change.

`frontend/` has its own `make` targets, also reachable from the root as
`make frontend-*`:

```sh
make frontend-install   # pnpm install --frozen-lockfile
make frontend-lint      # prettier + eslint
make frontend-check     # svelte-check
make frontend-build     # build into frontend/build
```

There is no frontend configuration and no `.env` — the API is same-origin, so
nothing points at it. The one browser-side knob is console verbosity, set at
runtime from devtools rather than baked into the build:

```js
localStorage.setItem("riven:log-level", "4"); // 0 error … 5 trace, -999 silent
```

## Changing the GraphQL API

`schema.graphql` is generated from the resolvers, and the frontend's TypeScript
types are generated from `schema.graphql`. Both are committed so that a frontend
build needs no Rust toolchain. After changing the API:

```sh
make schema
```

`make schema-check` fails when either artefact is stale; CI runs the same check,
so drift cannot ship. Two related guards live in the Rust tests:

- `schema::tests::the_checked_in_schema_matches_the_code` — `schema.graphql` is
  current.
- `schema::tests::every_mutation_is_classified` — every mutation on
  `MutationRoot`, including plugin-contributed ones, appears in the
  `MUTATION_GUARDS` table naming the capability that guards it. Adding a
  resolver without classifying it fails the build. It cannot prove the guard is
  *correct*, but it forces the decision to be made.

## Authorization

The privilege ladder lives in `crates/riven-core/src/auth.rs`, in `riven-core`
rather than `riven-api` because plugins define mutations too and cannot depend
on `riven-api`.

`Capability::minimum_role` is the only place a threshold is written down. Both
halves of authorization read it — the `require(ctx, Capability::X)?` guards that
reject a mutation, and the `viewer` query the UI renders from — so they cannot
disagree. The frontend never derives permissions from a role; it asks `viewer`
what it may do.

When adding a mutation: call `require(…)` as its first statement, and add it to
`MUTATION_GUARDS`.

## Before opening a change

```sh
make verify       # Rust: fmt, check, clippy, tests, generated docs
make verify-all   # the above plus frontend lint/check/build and schema drift
```

`make verify` is what CI's Rust job runs; `verify-all` matches the whole
pipeline. Note that `make verify` includes `docs-check` — `docs/environment.md`
and `docs/plugins/` are generated, and moving code changes the line numbers in
them, so run `make docs` and commit the result.
