# ── Base layer with toolchain + cargo-chef ────────────────────────────────────
FROM rust:alpine AS chef
# openssl-dev/-libs-static are for webauthn-rs, which better-auth's passkey
# plugin pulls in via webauthn-attestation-ca and which links OpenSSL rather
# than rustls. Static libs because this is a musl target.
RUN apk add --no-cache musl-dev fuse3-dev fuse3-static pkgconf openssl-dev openssl-libs-static
RUN cargo install cargo-chef --locked
WORKDIR /app

# ── Planner: distill Cargo.{toml,lock} into a dep-only recipe ─────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Builder: cook deps from recipe (cached unless lockfile/toml changes), ─────
#    then compile the actual binary against the source tree.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target,sharing=locked \
    SQLX_OFFLINE=true cargo chef cook --release --recipe-path recipe.json

COPY . .
# target/ is a cache mount and is wiped after the RUN, so copy the binary out
# to a stable path before the layer ends.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target,sharing=locked \
    SQLX_OFFLINE=true cargo build --release --locked --bin riven && \
    cp target/release/riven /riven

# ── Frontend bundle ───────────────────────────────────────────────────────────
# Built here, from `frontend/` in this repo. It used to be `FROM
# ${FRONTEND_IMAGE}` pointing at a `riven-frontend:bundle` tag built by hand from
# a sibling checkout — which meant CI had no such tag, buildx resolved the name
# against Docker Hub, and the build died on `pull access denied`.
#
# Manifest files are copied on their own so the install layer caches on the
# lockfile rather than on every source edit.
FROM node:26-alpine AS frontend
ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH
# pnpm 11 re-checks dependency state before running a script and prompts before
# purging anything it thinks is stale; there is no tty here to answer it.
ENV CI=true
RUN npm install -g --force corepack@latest && corepack enable
WORKDIR /app

COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml frontend/.npmrc ./
RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    pnpm install --frozen-lockfile

COPY frontend/ .
RUN pnpm run build

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache fuse3 ca-certificates

COPY --from=builder /riven /usr/local/bin/riven

# riven serves the SPA itself: `ServeDir` with an `index.html` fallback, so deep
# links into client-side routes resolve without a separate web server. Serving
# both from one origin is the point — the session cookie stays first-party and
# there is no proxy hop in front of /media or /stremio.
COPY --from=frontend /app/build /riven/frontend
ENV RIVEN_STATIC_DIR=/riven/frontend

RUN mkdir -p /logs && \
    echo "user_allow_other" >> /etc/fuse.conf

ENV SQLX_OFFLINE=true

ENTRYPOINT ["riven"]
