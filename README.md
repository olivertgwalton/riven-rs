# Riven RS

Riven-rs is a Rust implementation of the [Riven Media](https://riven.tv) tool.
THIS IS AN ALTERNATIVE, RIVEN-TS IS THE MAIN DEVELOPMENT EFFORT

## Architecture


- **PostgreSQL** stores media items, filesystem entries, streams, requests, settings, and migration state.
- **Redis** caches the Apalis job queues and worker coordination.
- **API Layer** exposes GraphQL subscriptions, webhooks, and the Apalis board UI.
- **FUSE** provides a filesystem for Jellyfin, Emby, Plex, or other media servers.
- **Plugins** provide metadata, content discovery, stream providers, debrid integration, notifications, logs, and media-server hooks.

## Codebase Layout

| Path | Purpose |
| --- | --- |
| `crates/riven-app` | `riven` binary, startup wiring, plugin registration, logging, API, queues, and VFS mount lifecycle. |
| `crates/riven-plugins` | The list of plugins compiled into the binary, plus the `gen-docs` documentation generator. |
| `crates/riven-core` | Shared settings, events, plugin traits/registry, HTTP helpers, downloader config, and domain types. |
| `crates/riven-api` | Axum and async-graphql API server, GraphQL schema, subscriptions, webhooks, media bridge, and board routes. |
| `crates/riven-db` | SeaORM database connection, migrations, entities, and repositories. |
| `crates/riven-queue` | Scheduler, job queue, indexing, scraping, parsing, downloading, and worker logic. |
| `crates/riven-rank` | Filename parsing, stream ranking, and release scoring helpers. |
| `crates/riven-vfs` | FUSE filesystem and stream-aware virtual media files. |
| `crates/plugin-*` | Built-in plugins for metadata, stream providers, media servers, content lists, notifications, dashboard, calendar, logs, and integrations. |
| `frontend` | SvelteKit SPA, built to static files and served by riven itself. Its GraphQL types are generated from `schema.graphql`. |

## Requirements

- Rust toolchain with Rust 2024 edition support.
- PostgreSQL.
- Redis.
- FUSE 3 for VFS mounting.
- Docker and Docker Compose if running the provided container stack.

## Quick Start With Docker Compose

1. Create local configuration:

   ```sh
   cp .env.example .env
   ```

2. Edit `.env` and set at least:

   ```sh
   RIVEN_STORAGE_PATH=/path/on/host/for/riven-storage
   ORIGIN=https://riven.example.com
   RIVEN_SETTING__PUBLIC_URL=https://riven.example.com
   RIVEN_SETTING__API_KEY=<openssl rand -hex 32>
   RIVEN_SETTING__AUTH_SECRET=<openssl rand -hex 32>
   ```

   Riven refuses to start without `API_KEY` and `AUTH_SECRET`. `PUBLIC_URL` is
   the origin passkeys are bound to — changing it later invalidates every
   registered passkey.

   Open the UI once it is up: the first account you create becomes the admin,
   and sign-up closes permanently after it.

3. Start the stack:

   ```sh
   docker compose up --build
   ```

The compose file starts `riven`, PostgreSQL, Redis and Jellyfin. There is no separate frontend container: the image builds the SPA from `frontend/` and riven serves it itself from `RIVEN_STATIC_DIR`, so the UI and the API share one origin — which is what keeps the session cookie first-party. It mounts `${RIVEN_STORAGE_PATH}` into Riven and Jellyfin, grants the Riven container FUSE access, and exposes:

- Riven (UI and API): `http://localhost:8080`
- Jellyfin: `http://localhost:8096`

## Configuration

Core settings are loaded from environment variables with the `RIVEN_SETTING__` prefix. Nested fields use double underscores.

Common settings:

| Variable | Default | Description |
| --- | --- | --- |
| `RIVEN_SETTING__DATABASE_URL` | `postgresql://localhost/riven` | PostgreSQL connection string. |
| `RIVEN_SETTING__REDIS_URL` | `redis://localhost:6379` | Redis connection string. |
| `RIVEN_SETTING__GQL_PORT` | `8080` | API server port. |
| `RIVEN_SETTING__API_KEY` | — | **Required.** Bearer/API key for machine callers; also seeds the Stremio addon token. Riven will not start without it. |
| `RIVEN_SETTING__AUTH_SECRET` | — | **Required**, minimum 32 characters. Signs session tokens; rotating it signs everyone out. |
| `RIVEN_SETTING__PUBLIC_URL` | bind address | Public origin browsers reach riven at. Sets cookie scope and the passkey relying-party ID. |
| `RIVEN_SETTING__LOG_DIRECTORY` | `./logs` | Directory for log output. |
| `RIVEN_SETTING__VFS_MOUNT_PATH` | empty | VFS mount path. |
| `RIVEN_SETTING__FILESYSTEM__MOUNT_PATH` | empty | Preferred VFS mount path. |
| `RIVEN_SETTING__VFS_CACHE_MAX_SIZE_MB` | `0` | VFS chunk cache size. `0` uses the default. |
| `RIVEN_SETTING__CORS_ALLOWED_ORIGINS` | empty | Comma-separated list of CORS origins. If empty, falls back to `ORIGIN`; if both are unset, CORS is permissive (warns on startup). |

Plugin settings use:

```text
RIVEN_PLUGIN_SETTING__<PLUGIN_NAME>__<KEY>
```

Examples:

```sh
RIVEN_PLUGIN_SETTING__TMDB__APIKEY=<tmdb-api-key>
RIVEN_PLUGIN_SETTING__SEERR__URL=http://localhost:5055
RIVEN_PLUGIN_SETTING__SEERR__APIKEY=<seerr-api-key>
RIVEN_PLUGIN_SETTING__STREMTHRU__REALDEBRIDAPIKEY=<real-debrid-api-key>
RIVEN_PLUGIN_SETTING__NOTIFICATIONS__URLS='["discord://webhookId/webhookToken"]'
```

Settings stored in the database override environment values for general and plugin settings after startup.

**You are required to bring your own TMDB, TVDB and Trakt API keys**

## API

When running locally on the default port:

- GraphQL endpoint: `http://localhost:8080/graphql`
- GraphiQL UI: `http://localhost:8080/graphql`
- Apalis board API: `http://localhost:8080/api/v1`
- Apalis board UI: `http://localhost:8080/board`
- Media bridge: `GET` or `HEAD http://localhost:8080/media/{entry_id}`

Every GraphQL request must present a credential: either a signed-in session cookie, or the configured `RIVEN_SETTING__API_KEY` via the `x-api-key` header, an `Authorization: Bearer <key>` header, or an `?api_key=<key>` query parameter (that last one is for Seerr's webhook, which POSTs to `/graphql` calling `seerrHandleWebhook` and cannot set custom headers — see `crates/plugin-seerr`). There is no anonymous access.

Roles come from the session riven verifies against its own store, and each resolver names the capability it needs (`crates/riven-core/src/auth.rs`). The frontend never derives permissions — it asks the `viewer` query what it may do.

## Plugins

Plugins implement the `Plugin` trait in `riven-core`. The set compiled into the binary is the explicit list in `crates/riven-plugins/src/lib.rs`, which is also the order they are registered and dispatched in.

**[Full plugin documentation lives in `docs/plugins`](docs/plugins/README.md)** — one page per plugin, with settings tables generated from each plugin's `settings_schema()`.

The current workspace includes plugins for:

- Metadata: TMDB, TVDB.
- Torrent scrapers: Comet, Torrentio, AIOStreams.
- Usenet: Newznab (search), Usenet (direct NNTP streaming).
- Debrid: StremThru.
- Request and list sources: Seerr, Listrr, MDBList, Trakt.
- Media servers: Plex, Emby, Jellyfin.
- Product features: Calendar, Dashboard, Notifications, Subdl, Webhooks.

To add a built-in plugin: create a new `crates/plugin-*` crate, add it to the workspace members and to `crates/riven-plugins/Cargo.toml`, then add one line to `all_plugins()`. Give it a `settings_schema()` and run `make docs` to generate its page.

## Development Commands

```sh
make fmt          # cargo fmt --all
make fmt-check    # cargo fmt --all --check
make check        # cargo check --workspace --all-targets
make lint         # cargo clippy --workspace --all-targets -- -D warnings
make test         # cargo test --workspace
make docs         # regenerate docs/plugins from the plugins' settings schemas
make docs-check   # fail if docs/plugins is out of date
make verify       # fmt-check, check, lint, test, and docs-check (Rust only)

make frontend-install   # pnpm install --frozen-lockfile
make frontend-lint      # prettier + eslint
make frontend-check     # svelte-check
make frontend-build     # build the SPA into frontend/build

make schema       # regenerate schema.graphql, then the frontend's types from it
make schema-check # fail if either committed artefact is stale
make verify-all   # everything above, matching CI
```

`schema.graphql` and `frontend/src/lib/gql/` are generated and committed. Run
`make schema` after changing the GraphQL API, or CI will fail on the drift.

For a direct release build:

```sh
cargo build --release
```

For the application binary:

```sh
cargo build -p riven-app --bin riven
```

## Database Migrations

Migrations live in `crates/riven-db/migrations` and run automatically during application startup via `sea-orm-migration`.

## Contributing

See `CONTRIBUTING.md` for the project standards. The short version:

- Format with `cargo fmt --all`.
- Keep `cargo clippy --workspace --all-targets -- -D warnings` clean.
- Add focused tests for changed behavior.
- Prefer shared crates for configuration, parsing, and reusable workflow logic.
- Run `make verify` before opening or updating a change.
