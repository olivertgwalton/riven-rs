# Riven RS

Riven-rs is a Rust implementation of the [Riven Media] (https://riven.tv) tool.
THIS IS AN ALTERNATIVE, RIVEN-RS IS THE MAIN DEVELOPMENT EFFORT

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
| `crates/riven-core` | Shared settings, events, plugin traits/registry, HTTP helpers, downloader config, and domain types. |
| `crates/riven-api` | Axum and async-graphql API server, GraphQL schema, subscriptions, webhooks, media bridge, and board routes. |
| `crates/riven-db` | SQLx database connection, migrations, entities, and repositories. |
| `crates/riven-queue` | Scheduler, job queue, indexing, scraping, parsing, downloading, and worker logic. |
| `crates/riven-rank` | Filename parsing, stream ranking, and release scoring helpers. |
| `crates/riven-vfs` | FUSE filesystem and stream-aware virtual media files. |
| `crates/plugin-*` | Built-in plugins for metadata, stream providers, media servers, content lists, notifications, dashboard, calendar, logs, and integrations. |

## Requirements

- Rust toolchain with Rust 2024 edition support.
- PostgreSQL.
- Redis.
- FUSE 3 for VFS mounting.
- Docker and Docker Compose if running the provided container stack.

SQLx runs in offline mode by default through `.cargo/config.toml`, so normal builds use the checked-in `.sqlx` metadata. Regenerate that metadata when changing SQL queries.

## Quick Start With Docker Compose

1. Create local configuration:

   ```sh
   cp .env.example .env
   ```

2. Edit `.env` and set at least:

   ```sh
   RIVEN_STORAGE_PATH=/path/on/host/for/riven-storage
   ORIGIN=https://riven.example.com
   PASSKEY_RP_ID=riven.example.com
   RIVEN_SETTING__API_KEY=<shared-backend-api-key>
   RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET=<shared-frontend-signing-secret>
   AUTH_SECRET=<frontend-auth-secret>
   ```

3. Start the stack:

   ```sh
   docker compose up --build
   ```

The compose file starts `riven`, PostgreSQL, Redis, Jellyfin, and a `riven-frontend` container. It mounts `${RIVEN_STORAGE_PATH}` into Riven and Jellyfin, grants the Riven container FUSE access, and exposes:

- Riven API: `http://localhost:8080`
- Frontend: `http://localhost:3000`
- Jellyfin: `http://localhost:8096`

## Configuration

Core settings are loaded from environment variables with the `RIVEN_SETTING__` prefix. Nested fields use double underscores.

Common settings:

| Variable | Default | Description |
| --- | --- | --- |
| `RIVEN_SETTING__DATABASE_URL` | `postgresql://localhost/riven` | PostgreSQL connection string. |
| `RIVEN_SETTING__REDIS_URL` | `redis://localhost:6379` | Redis connection string. |
| `RIVEN_SETTING__GQL_PORT` | `8080` | API server port. |
| `RIVEN_SETTING__API_KEY` | empty | Optional bearer/API key required by GraphQL. Empty disables API auth. |
| `RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET` | empty | Shared secret used to verify frontend-signed RBAC headers on GraphQL requests. |
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
- Seerr webhook: `POST http://localhost:8080/webhook/seerr`
- Media bridge: `GET` or `HEAD http://localhost:8080/media/{entry_id}`

If `RIVEN_SETTING__API_KEY` is set, GraphQL requests must include the configured key with either the `x-api-key` header or an `Authorization: Bearer <key>` header.

When browser traffic goes through `riven-frontend`, the frontend signs the authenticated user's role claims and the backend verifies them with `RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET` before applying RBAC. `riven-frontend` can read that same variable directly in shared deployments, or `BACKEND_AUTH_SIGNING_SECRET` in standalone frontend deployments. Direct non-frontend API-key clients still work without those signed headers.

## Plugins

Plugins implement the `Plugin` trait in `riven-core` and register themselves with `register_plugin!`. The app crate links every `plugin-*` dependency found in `crates/riven-app/Cargo.toml` so inventory can collect plugin registrations at runtime.

The current workspace includes plugins for:

- Metadata and IDs: TMDB, TVDB.
- Stream providers and debrid: Comet, Torrentio, AIOStreams, StremThru.
- Request and list sources: Seerr, Listrr, MDBList, Trakt.
- Media servers: Plex, Emby, Jellyfin.
- Product features: Calendar, Dashboard, Logs, Notifications.

To add a built-in plugin, create a new `crates/plugin-*` crate, register the plugin, add it to the workspace, and add it as a dependency of `crates/riven-app`.

## Development Commands

```sh
make fmt          # cargo fmt --all
make fmt-check    # cargo fmt --all --check
make check        # cargo check --workspace --all-targets
make lint         # cargo clippy --workspace --all-targets -- -D warnings
make test         # cargo test --workspace
make verify       # fmt-check, check, lint, and test
```

For a direct release build:

```sh
SQLX_OFFLINE=true cargo build --release
```

For the application binary:

```sh
cargo build -p riven-app --bin riven
```

## Database Migrations

Migrations live in `crates/riven-db/migrations` and run automatically during application startup. Because SQLx offline mode is enabled by default, query changes should be accompanied by updated `.sqlx` metadata.

## Contributing

See `CONTRIBUTING.md` for the project standards. The short version:

- Format with `cargo fmt --all`.
- Keep `cargo clippy --workspace --all-targets -- -D warnings` clean.
- Add focused tests for changed behavior.
- Prefer shared crates for configuration, parsing, and reusable workflow logic.
- Run `make verify` before opening or updating a change.
