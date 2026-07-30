# Built from the riven-frontend repo, which emits a `scratch` image holding only
# the static files. Declared here because an ARG used by a FROM must precede the
# first FROM in the file. Build it first:
#   docker build -t riven-frontend:bundle ../riven-frontend
ARG FRONTEND_IMAGE=riven-frontend:bundle

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
# Built from the riven-frontend repo, which now emits a `scratch` image holding
# only the static files. Build it first:
#   docker build -t riven-frontend:bundle ../riven-frontend
# Override with `--build-arg FRONTEND_IMAGE=` to pin a registry tag.
FROM ${FRONTEND_IMAGE} AS frontend

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache fuse3 ca-certificates

COPY --from=builder /riven /usr/local/bin/riven

# riven serves the SPA itself: `ServeDir` with an `index.html` fallback, so deep
# links into client-side routes resolve without a separate web server. Serving
# both from one origin is the point — the session cookie stays first-party and
# there is no proxy hop in front of /media or /stremio.
COPY --from=frontend /dist /riven/frontend
ENV RIVEN_STATIC_DIR=/riven/frontend

RUN mkdir -p /logs && \
    echo "user_allow_other" >> /etc/fuse.conf

ENV SQLX_OFFLINE=true

ENTRYPOINT ["riven"]
