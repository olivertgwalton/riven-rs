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
   ```

   Riven refuses to start without `API_KEY`. `PUBLIC_URL` is the origin
   passkeys are bound to — changing it later invalidates every registered
   passkey.

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
| `RIVEN_SETTING__PUBLIC_URL` | bind address | Public origin browsers reach riven at. Sets cookie scope and the passkey relying-party ID. |
| `RIVEN_SETTING__LOG_DIRECTORY` | `./logs` | Directory for log output. |
| `RIVEN_SETTING__VFS_MOUNT_PATH` | empty | VFS mount path. |
| `RIVEN_SETTING__FILESYSTEM__MOUNT_PATH` | empty | Preferred VFS mount path. |
| `RIVEN_SETTING__FILESYSTEM__SYMLINK_PATH` | empty | Where to materialise the library as real directories holding symlinks into the VFS. Empty disables it. See [Symlink library](#symlink-library). |
| `RIVEN_SETTING__VFS_CACHE_MAX_SIZE_MB` | `0` | VFS chunk cache size. `0` uses the default. |
| `RIVEN_SETTING__CORS_ALLOWED_ORIGINS` | empty | Comma-separated list of CORS origins. If empty, falls back to `ORIGIN`; if both are unset, CORS is permissive (warns on startup). |
| `RIVEN_SETTING__OIDC_PROVIDERS` | `[]` | Sign in via PocketID, Authelia, Keycloak, or any other OIDC-compliant identity provider. See [OIDC sign-in](#oidc-sign-in) below. |

### Symlink library

The VFS is mounted read-only and answers only for paths the database knows
about, so a media server has nowhere to put the files it authors beside a
title: a theme song, an `.nfo`, trickplay tiles, an extracted subtitle. Every
one of those writes fails with `Read-only file system`, and the server reports
it as a plugin or a scan that quietly does nothing.

Setting `filesystem.symlink_path` materialises the library a second way: real
directories, over the same layout the VFS serves, with every media file a
symlink into the mount. The server is pointed at that tree instead. It reads
the media through the symlink -- so the bytes still stream from the VFS, and
nothing is copied -- and writes its sidecars into a directory that is genuinely
on disk.

Two paths, so they must not be nested; riven refuses to reconcile if either
contains the other.

**It is off by default, and turning it on is four lines plus a settings
change.** Nothing about an existing deployment changes until you opt in:
`symlink_path` defaults to empty, and Compose keeps the VFS exactly where it
was, parking the (unused, empty) library volume at `/mnt/riven-library`.

To enable it, in `.env`:

```
RIVEN_VFS_MOUNT=/mnt/riven-vfs
RIVEN_LIBRARY_MOUNT=/mnt/riven
RIVEN_SETTING__FILESYSTEM__MOUNT_PATH=/mnt/riven-vfs
RIVEN_SETTING__FILESYSTEM__SYMLINK_PATH=/mnt/riven
```

The VFS moves to `/mnt/riven-vfs` and the tree takes `/mnt/riven`, which is
deliberate: the media servers keep the library path they already have, so
switching costs no rescan and no item identifiers. The mount has to be visible
in the consuming container at the same path riven wrote into the links, or
every link dangles.

**Mind the order, and mind the database.** A `filesystem` block saved in the
`general` settings row overrides the two `RIVEN_SETTING__FILESYSTEM__*`
variables entirely, so on an instance that has ever saved settings the paths
must be changed there instead. And apply the settings *before* recreating the
containers: with the new mounts but the old `mount_path`, riven mounts the VFS
into the empty library volume, where it does not propagate, and the media
servers see an empty library.

The tree is reconciled at startup, whenever the library settles after a change,
and on a slow sweep underneath both. Reconciling never removes a regular file
-- only symlinks it would have created itself. A directory left holding nothing
but sidecars is kept rather than tidied away, so a title downloaded again finds
its theme song already there.

### OIDC sign-in

`RIVEN_SETTING__OIDC_PROVIDERS` is a JSON array, one entry per identity
provider. Riven never hardcodes a provider's endpoint layout: `issuer` is
resolved to `authorization_endpoint`/`token_endpoint`/`userinfo_endpoint` via
`{issuer}/.well-known/openid-configuration` at startup, so any spec-compliant
issuer works — PocketID, Authelia, Keycloak, Authentik, Zitadel, and so on.
Configure as many providers as you like; each needs a unique `id`.

```sh
RIVEN_SETTING__OIDC_PROVIDERS='[{"id":"pocketid","name":"PocketID","issuer":"https://pocketid.example.com","client_id":"<client-id>","client_secret":"<client-secret>"}]'
```

| Field | Required | Description |
| --- | --- | --- |
| `id` | yes | Becomes both the callback path segment and the linked account's `provider_id`. Changing it after users have signed in orphans their existing links. |
| `name` | no | Shown on the login button, e.g. "PocketID". Falls back to `id` when empty. |
| `issuer` | yes | Must exactly match the provider's advertised issuer. Usually just an origin (`https://pocketid.example.com`), but a provider hosting multiple issuers under one domain puts a path on it too — a Keycloak realm is `https://keycloak.example.com/realms/<realm>`. A trailing slash is trimmed either way. |
| `client_id` / `client_secret` | yes | From the OAuth client you register on the provider. |
| `scopes` | no | Defaults to `["openid", "profile", "email"]`, which is all riven reads (`sub`, `email`, `name`, `picture`, `email_verified`). |
| `disable_sign_up` | no | Default `false`: a first-time sign-in from this provider registers a new account, same as password/Plex sign-in always has. Set `true` to require an admin-created account (Admin → Users → Create User) with a matching email first — the OIDC sign-in only links to it, it never creates one. Use this when the provider's own user base is broader than who should have riven access. |
| `trust_unverified_email` | no | **Read the warning below before setting `true`.** Default `false`. |

On the provider side, register this exact redirect URI (riven never varies it):

```text
{RIVEN_SETTING__PUBLIC_URL}/auth/callback/{id}
```

e.g. `https://riven.example.com/auth/callback/pocketid`.

A provider that fails discovery at startup (unreachable, not actually OIDC) is
logged and simply omitted from the login page rather than failing the whole
instance — these are optional sign-in methods layered on top of the built-in
password/passkey/Plex ones.

**Account linking and `trust_unverified_email`.** A sign-in whose email
matches an existing account auto-links to it — but only when the provider
reports `email_verified: true`, or when the provider is listed with
`trust_unverified_email: true`. This default is what a spec-compliant OIDC
client is expected to do: without it, a stranger who could get an
*unconfirmed* address on some provider to match an existing riven account
could take that account over. Turning it on for a provider is only as safe as
your confidence that every account on it is one you vetted yourself — e.g. a
self-hosted IdP with no self-registration, where you created every user by
hand. **PocketID is a common case that needs it**: it has no email
confirmation flow, so it never reports `email_verified: true`, and without
this flag linking fails every time.

The failure is quiet, so know what to look for: riven redirects the browser to
`/?error=sign-in-failed` and the login page shows no message, because the
reason is deliberately not handed to the client. The server log carries it:

```
OIDC sign-in failed … you@example.com is unverified at the provider;
refusing to link it to an existing account
```

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

**[Full development setup lives in `docs/development.md`](docs/development.md)** —
running natively vs in Docker, the required environment, working on the
frontend, and the schema/authorization workflows.

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
