# ── Base layer with toolchain + cargo-chef ────────────────────────────────────
FROM rust:alpine AS chef
RUN apk add --no-cache musl-dev fuse3-dev fuse3-static pkgconf
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
# `[patch.crates-io] apalis-redis = { path = "vendor/apalis-redis" }` in the
# root Cargo.toml means cargo-chef's recipe references a path dep that has to
# exist on disk during cook. Bring vendor/ in here (it's small) so the cook
# step can resolve it; this only invalidates the cook-layer cache when vendor/
# itself changes, which is rare.
COPY vendor vendor
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    SQLX_OFFLINE=true cargo chef cook --release --recipe-path recipe.json

COPY . .
# target/ is a cache mount and is wiped after the RUN, so copy the binary out
# to a stable path before the layer ends.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    SQLX_OFFLINE=true cargo build --release --locked --bin riven && \
    cp target/release/riven /riven

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache fuse3 ca-certificates

COPY --from=builder /riven /usr/local/bin/riven

RUN mkdir -p /logs && \
    echo "user_allow_other" >> /etc/fuse.conf

ENV SQLX_OFFLINE=true

ENTRYPOINT ["riven"]
