# Security review — riven-rs backend + frontend

Date: 2026-07-31 · Commit: `e8887e7` · Scope: `crates/` (axum/GraphQL API, auth, plugins) and `frontend/` (SvelteKit SPA), plus packaging.

> **Status: eleven of twelve findings fixed in the working tree. Finding 3
> (auth rate limiting) was deliberately left unfixed — see the note on it
> below.** Finding 1 was confirmed against the running instance before the fix
> (`GET /api/v1/queues` answered `200` with no credential; `/graphql` correctly
> answered `401`). The fixes compile, `cargo test --workspace` and
> `pnpm run check` pass, and clippy is clean — but they have **not** been
> exercised against a live instance, which needs a rebuild and restart. No
> configuration changes are required to deploy them.

Threat model assumed: a private, self-hosted media server that is nonetheless reachable on a network — often port-forwarded or behind a reverse proxy — with several accounts of differing privilege (`User` / `Manager` / `Admin`).

---

## Summary

The core authentication and authorization work is genuinely good. Sessions are verified against the store rather than trusted from a header, the `role_from_user` mapping fails closed, an unconfigured API key matches nothing, secret comparisons are constant-time, every GraphQL **mutation** carries a capability guard, there is no `unsafe` in the workspace, `cargo audit` is clean, and the frontend has no `{@html}`, no `innerHTML`, no token in `localStorage`, and no open redirect.

The problems are at the edges of that design: two HTTP surfaces that were mounted outside the auth story entirely, credentials that leak through read paths nobody guarded, and a rate limiter that does not limit anything.

Eleven were fixed. The twelfth — the rate limiter — was left as an accepted risk after the fix turned out to carry a worse failure mode than the flaw; finding 3 records why.

| # | Severity | Finding |
|---|---|---|
| 1 | **Critical** | Job board API and UI mounted with no authentication; allows arbitrary job injection |
| 2 | **High** | Plex / Emby / Jellyfin admin tokens returned to every authenticated user |
| 3 | ~~High~~ | Auth rate limiting is trivially bypassed and doubles as a DoS lever — **accepted, not fixed** |
| 4 | Medium | Plex PIN poll is unauthenticated and enumerable → sign-in hijack |
| 5 | Medium | Debrid direct-download URLs readable by the lowest-privilege role |
| 6 | Medium | No GraphQL depth/complexity limit over recursive types |
| 7 | Medium | Container and compose hardening |
| 8–12 | Low | Response headers, cookie `Secure`, fail-open helper, info disclosure, stale docs |

---

## 1. Critical — unauthenticated job board API and UI

`crates/riven-api/src/server.rs:222-223`

```rust
.nest("/api/v1", board_api.with_state(()))
.nest("/board", board_ui.with_state(()))
```

Neither nest has an auth layer, and neither handler checks a credential. Everything registered at `server.rs:139-148` is exposed: the index, scrape, parse, download, rank-streams and process-media-item queues, plus every plugin hook queue.

Reachable with no credential at all:

- `GET /api/v1/queues`, `/api/v1/tasks`, `/api/v1/workers`, `/api/v1/overview`, `/api/v1/events` (SSE)
- `GET /api/v1/queues/{queue}/tasks`, `/stats`, `/workers`, `/tasks/{task_id}`
- **`PUT /api/v1/queues/{queue}/tasks`** — `push_task`, which deserializes the request body straight into a job and enqueues it
- `GET /board/*` — the full board UI

The read side dumps job payloads, which for the download queue carry provider identifiers and stream URLs. The write side is worse: an unauthenticated attacker can drive scrapes and downloads on the instance's debrid account and usenet providers, burn API quota, and fill the queues.

CORS is not a mitigation here — it constrains browser JS, not `curl`.

Verified against `apalis-board-api-1.0.0-rc.8`, `src/framework/axum.rs:250-269`: the crate ships no authentication of its own.

**Fix:** put the same `authorize_request` check in front of both nests (a `from_fn_with_state` middleware requiring `Capability::ManageSettings` is the natural bar), or drop `RegisterRoute`/`ServeUI` from the router unless an operator explicitly opts in.

Note also `board::board_assets_middleware` (`server.rs:229`) is a top-level layer that serves any path containing a `.` from the embedded board assets, ahead of routing and ahead of any auth. That is how board assets reach the browser today; it also means the board's own asset names shadow SPA paths.

---

## 2. High — media-server admin tokens handed to every authenticated user

`crates/plugin-plex/src/client.rs:106`

```rust
image_url: thumb
    .as_ref()
    .map(|thumb| format!("{plex_url}{thumb}?X-Plex-Token={token}")),
```

`crates/plugin-emby-jellyfin/src/lib.rs:308`

```rust
image_url: item_id
    .map(|id| format!("{base_url}/Items/{id}/Images/Primary?api_key={api_key}")),
```

Both are returned by `activePlaybackSessions`, which has no guard (`crates/plugin-dashboard/src/lib.rs:168`) — so any account, including the lowest `User` role whose only capability is `RequestItems`, can read them. The token is the one riven was configured with: a Plex server token and an Emby/Jellyfin API key are both administrative credentials for those servers.

It does not stop at the API. The frontend renders the value as an image source (`frontend/src/lib/components/dashboard/watching-now-card.svelte:101`, queried at `frontend/src/routes/(protected)/dashboard/+page.ts:60`), so the credential also lands in the DOM, browser history and cache, and — with no `Referrer-Policy` set (finding 8) — in the `Referer` of anything that page subsequently loads.

**Fix:** don't put the token in the URL. Proxy the thumbnail through riven (`/media/thumb/{server}/{item_id}`, server-side credential, session-authenticated), or omit `image_url` entirely. Guarding `activePlaybackSessions` is worth doing anyway but does not fix this on its own — an admin's browser would still carry the token.

---

## 3. High — auth rate limiting does not limit

`crates/riven-api/src/server/authn.rs:81-157` never configures `RateLimitConfig`, so the default applies: 100 requests / 60s, keyed by

```rust
// better-auth-rs crates/core/src/middleware/rate_limit.rs:102
fn client_key(req: &AuthRequest) -> String {
    req.headers.get("x-forwarded-for")
        .or_else(|| req.headers.get("x-real-ip"))
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}
```

The key is entirely attacker-controlled. There is no trusted-proxy list and no fallback to the real socket address, so:

- **Bypass:** rotate `X-Forwarded-For` per request and get a fresh bucket every time. `POST /auth/sign-in/email` becomes unrate-limited, which matters because riven runs with `password_min_length(8)` and no account lockout.
- **DoS:** deployed without a proxy (nothing sets the header), *every* client shares the single `"unknown"` bucket. One caller at 100 req/min locks all users out of sign-in, passkey verification, and session refresh.

### Accepted, not fixed

A riven-side limiter was written and then removed, deliberately. Recording the reasoning because the finding is otherwise the kind of thing that gets "fixed" badly twice.

The first attempt keyed buckets on the peer address and required `RIVEN_SETTING__TRUSTED_PROXIES` to be set when riven sits behind a reverse proxy. That is the obvious design and it is a bad one: forget the setting and every connection arrives from the proxy's address, so all users share one bucket — and anyone who knows the hostname can spend it and lock the whole instance out of signing in. A brute-force defence whose failure mode is an outage available to any passer-by is worse than no defence, and the failure is silent.

That is fixable — trust private-range peers by default, and treat an unattributable request as *unlimited* rather than putting it in a shared bucket — but the owner's call was that the complexity is not worth it on a private instance, and that is a reasonable read.

**What that leaves.** better-auth's own limiter is still wired in (100 requests/60s, keyed on `X-Forwarded-For`), so it works against an attacker who does not think about it and is bypassable by one who does. Online password guessing is therefore bounded only by Argon2/scrypt cost, which is meaningful — each attempt costs the server ~19 MiB and two passes — but that cuts both ways as a resource-exhaustion vector.

**If this is ever revisited**, the cheap mitigations that do not carry the shared-bucket risk are: raise `password_min_length` above 8, put the rate limiting in the reverse proxy where the client address is already known (Caddy's `rate_limit`, nginx's `limit_req`), or require 2FA for admin accounts.

---

## 4. Medium — Plex PIN poll is unauthenticated and enumerable

`crates/riven-api/src/server.rs:207-208`, handler at `crates/riven-api/src/server/plex.rs:184`

`GET /auth/plex/poll/{pin_id}` takes any integer, needs no credential, and on success **sets a session cookie for the matched user** (`plex.rs:225-241`).

Plex ties a PIN to the `X-Plex-Client-Identifier` that created it — but riven's identifier is instance-constant by design (`plex.rs:57-64`), so every PIN this instance mints can be polled by anyone who reaches this endpoint with the right id. Plex PIN ids are sequential global integers, and an attacker can call `POST /auth/plex/start` themselves to learn the current range. They then scan nearby ids; when a legitimate user of the instance approves their sign-in, the attacker's poll returns that user's session cookie. Polling is not destructive, so no race needs to be won.

The email-confirmation check at `plex.rs:264-283` is a good defence against a *different* attack and does not help here — the identity is genuine, it is just being claimed by the wrong requester.

Secondary: both `/auth/plex/start` and `/auth/plex/poll` make unauthenticated outbound requests to plex.tv on demand with no rate limit.

**Fix:** bind the PIN to its initiator. Have `start` mint a random nonce, store `pin_id → nonce` server-side (with a short TTL), return only the nonce to the caller, and have `poll` take the nonce rather than the raw `pin_id`. Rate-limit both endpoints.

---

## 5. Medium — debrid direct-download URLs readable by the lowest role

`schema.graphql:281-310` exposes `downloadUrl`, `streamUrl`, `providerDownloadId` and `sourceId` on `FileSystemEntry`, which is returned by resolvers that carry no guard:

- `filesystemEntries` — `crates/riven-api/src/schema/queries/media.rs:171`
- `vfsEntry` — `crates/riven-api/src/schema/vfs.rs:55`
- `streams` on every typed item — `crates/riven-api/src/schema/typed_items.rs:31,56,96,144`

A `streamUrl` is a bearer capability: whoever holds it downloads the file directly from the debrid CDN, bypassing riven's own access checks entirely, and it identifies the debrid account. A `User` who is meant only to request titles can enumerate the whole library's links.

This is a deliberate design point elsewhere — all authenticated users can *read* the library — but the URL fields are a different class of data from titles and posters.

**Fix:** gate the three URL-ish fields behind `Capability::ScrapeItems` (a `#[graphql(guard)]` on the field, or a resolver that returns `None` below the threshold), keeping the rest of the type readable.

---

## 6. Medium — no GraphQL depth or complexity limit

`crates/riven-api/src/schema.rs:56` builds the schema with no `limit_depth`, no `limit_complexity`, and introspection left on.

The schema is cyclic: `Show.seasons → Season.show → Show.seasons …`, and `Season.episodes → Episode.season → …` (`typed_items.rs:65,105,117,153`). Each hop is a database round trip, so a modest nesting depth costs an exponential number of queries. Any authenticated account — including `User` — can flatten the instance and its Postgres with one request.

**Fix:** `.limit_depth(15).limit_complexity(1000)` on the builder, tuned against the frontend's real queries (`frontend/src/routes/**/+page.ts` are the deepest).

---

## 7. Medium — container and compose hardening

`Dockerfile` and `docker-compose.yaml`:

- The runtime image declares no `USER`, so riven runs as **root**, combined with `cap_add: SYS_ADMIN`, `devices: /dev/fuse`, `security_opt: apparmor:unconfined`, and a `propagation: shared` bind mount. FUSE needs most of this, but the combination means any RCE in riven is a straightforward host compromise rather than a container one. Adding a non-root `USER` with only `/dev/fuse` access removes the cheapest half of that.
- `POSTGRES_PASSWORD=riven` in `.env.example` is a fine placeholder but reads as a default; worth a comment saying it should be changed.

**Correction — Redis and IPv6.** The first draft of this review flagged two things here that do not hold up, and they were checked afterwards rather than assumed:

- *"`enable_ipv6: true` makes unpublished ports reachable."* Not on this host. The network is assigned `fd07:b51a:cc66:d000::/64`, which is `fd00::/8` — a Unique Local Address, not globally routable. Docker/OrbStack assigns a ULA prefix, not a global one, so unpublished stays unreachable. Still worth re-checking if riven is ever run somewhere that hands out a global prefix.
- *"Redis needs `requirepass`."* Withdrawn. Redis publishes no ports (`127.0.0.1:6379` is closed on the host) and shares its network with exactly `riven`, `postgres` and `redis` — one trust domain. Reaching it means already having execution inside one of those three containers, where `.env` and the password are both readable anyway. A password was added and then reverted: it guarded nothing that was not already lost, while costing a breaking config change and two values that must stay in sync or the stack will not boot. Leaving an internal Redis unauthenticated is also the norm for self-hosted compose stacks.

  This would change if Redis were ever published to a host port, shared with another project's containers, or moved onto a flat network like Kubernetes pod networking.

`.env` itself is correctly gitignored and has never been committed (checked `git log --all -- .env`).

---

## 8–12. Low

**8. No security response headers.** Nothing in `crates/` sets `Content-Security-Policy`, `X-Frame-Options`, `X-Content-Type-Options` or `Referrer-Policy`; the SPA is served by a bare `ServeDir` (`server.rs:154`). XSS risk is genuinely low — no `{@html}`, no `innerHTML`, no `eval` anywhere in `frontend/src` — but the admin UI is framable (clickjacking against destructive mutations), and the missing `Referrer-Policy` is what lets finding 2's token escape via `Referer`. A `SetResponseHeaderLayer` with `frame-ancestors 'none'`, `nosniff` and `strict-origin-when-cross-origin` is cheap.

**9. Session cookie `Secure` follows `public_url`'s scheme.** `AuthConfig::base_url` sets `session.cookie_secure = base_url.starts_with("https://")` (better-auth-rs `crates/core/src/config.rs:910`). An operator behind a TLS-terminating proxy who sets `RIVEN_SETTING__PUBLIC_URL` to the internal `http://` address gets a session cookie with no `Secure` flag, and no warning. Worth logging loudly at startup when `public_url` is `http://` and the host is not loopback.

**10. `verify_addon_token` fails open.** `crates/riven-core/src/stremio.rs:40-42` returns `true` for an empty API key. The sole call site guards against it (`server/auth.rs:80-85`) and `main.rs:85` rejects a blank key at startup, so this is not currently exploitable — but a fail-open primitive in a verification helper is a trap for the next caller. Return `false` and let the caller decide.

**11. Operational disclosure to any authenticated user.** Unguarded reads: `nntpProviders` (provider hostnames, ports, connection counts — `queries/usenet_health.rs:232`), `debridAccountInfo` (account email, subscription status — `plugin-dashboard/src/lib.rs:153`), and the `notifications` subscription (`subscriptions/notifications.rs:57`). Low impact on a private instance, but none of it is a `User`'s business.

**12. Stale documentation.** `.env.example:27-31` documents `RIVEN_SECRET_KEY` / `RIVEN_SECRET_KEY_PATH`; neither string appears anywhere in `crates/`. Leftover from the TypeScript frontend — an operator could reasonably think a secret is being used that isn't.

---

## Checked and clean

- **SQL injection** — all 49 raw-SQL sites use `Statement::from_sql_and_values` with bound parameters or fixed strings; no `format!` interpolation into a query. The one `execute_unprepared` (`first_user.rs:66`) interpolates a compile-time `i64` constant.
- **Mutation authorization** — every mutation in `schema/mutations/*` and the one plugin mutation (`plugin-seerr/src/webhook.rs:29`) calls `require(...)` or `require_settings_access(...)` as its first statement. Capability thresholds are single-sourced in `Capability::minimum_role` and the `viewer` query is derived from the same table, with a test pinning the two together.
- **Session handling** — verified against the store, expiry and ban both enforced (`server/auth.rs:182-201`), unknown roles fall to least privilege, bearer-vs-cookie precedence is tested.
- **CSRF** — better-auth's `CsrfMiddleware` is wired in by default (`better-auth-rs src/core/auth.rs:156`) and validates `Origin`/`Referer` on state-changing `/auth` calls. `/graphql` has no origin check of its own but is protected by the session cookie's `SameSite=Lax` default and by `GET /graphql` never executing operations.
- **Password hashing** — Argon2 for new hashes, scrypt verification for migrated ones, constant-time comparison, NFKC normalization, and both run on `spawn_blocking` (`server/legacy_password.rs`).
- **Path traversal** — the `logs` query only globs `riven.log*` inside a fixed directory and is admin-guarded; VFS query paths resolve against the DB-backed layout, not the host filesystem.
- **Frontend XSS / storage** — no `{@html}`, `innerHTML`, `outerHTML`, `eval` or `new Function` in `frontend/src`; no credential in `localStorage`/`sessionStorage`; every `goto()` target is a static path (no open redirect); the GraphQL client is same-origin and cookie-only, with no API key in the browser.
- **Secret hygiene in logs** — no credential is logged; `plugin-stremthru/src/newznab.rs:62` explicitly strips `apikey` before logging a query, and Plex/Emby use headers rather than query strings on their server-to-server calls. (Finding 2 is the exception, and it is a response body, not a log.)
- **Dependencies** — `cargo audit`: one advisory, `RUSTSEC-2026-0173`, `proc-macro-error2` unmaintained, build-time only. `pnpm audit`: one low, `cookie <0.7.0`, dev-dependency only and not in the static bundle.
- **Memory safety** — zero `unsafe` blocks in the workspace.

---

## What was changed

| # | Fix |
|---|---|
| 1 | `require_board_admin` middleware on both `/api/v1` and `/board` (`server/board.rs`). Bar is `ManageSettings`; accepts a session or the API key. The board's four static bundle files stay public and are documented as inert. |
| 2 | New `RivenEvent::ArtworkRequested` → `HookResponse::Artwork` hook, a `GET /artwork/{server}?ref=` proxy (`server/artwork.rs`), and `artwork_path` in `riven-core`. Plugins emit a riven-relative path; the Plex token and Emby key never leave the backend. Both plugins validate the reference before building a URL, cap the body at 8 MB, and refuse a non-`image/*` content type so the route can't be turned into same-origin HTML. |
| 3 | **Not fixed — accepted.** See the finding above for what was tried, why it was removed, and what mitigations remain. |
| 4 | `start` now returns an opaque 256-bit handle, held in-process with a 10-minute TTL and dropped once the sign-in completes; `poll` takes the handle, not the Plex PIN id. Frontend updated in both call sites. |
| 5 | `downloadUrl` / `streamUrl` moved to a `ComplexObject` that resolves them to `null` below `ScrapeItems`. Returns null rather than erroring, because the frontend's client throws on any `errors` entry. |
| 6 | `.limit_depth(15).limit_complexity(2000)` on the schema builder. |
| 7 | Non-root `USER riven` in the Dockerfile, and `POSTGRES_PASSWORD` noted as worth changing. The Redis password and the IPv6 exposure claim were both withdrawn on inspection — see the correction under finding 7. |
| 8 | `frame-ancestors 'none'`, `X-Frame-Options: DENY`, `nosniff` and `strict-origin-when-cross-origin` on every response. |
| 9 | Startup warning when `public_url` is `http://` and not loopback, naming the consequence (session cookie without `Secure`). |
| 10 | `verify_addon_token` returns `false` for an empty key instead of `true`; the test that pinned the fail-open behaviour now pins the opposite. |
| 11 | `nntpProviders` and `debridAccountInfo` return empty below admin — empty rather than an error, since `nntpProviders` shares a query with four panels non-admins still see. |
| 12 | Stale `RIVEN_SECRET_KEY` block removed from `.env.example`. |

## Deploying these fixes

**No configuration changes are required.** Rebuild and restart is enough.

Worth checking after the restart: that an admin can still open `/board`, that a non-admin's dashboard still renders (the usenet panel should show, with the provider list empty), and that artwork appears on the "Watching Now" card.
