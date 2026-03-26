FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev fuse3-dev fuse3-static pkgconf

WORKDIR /app
COPY . .

RUN SQLX_OFFLINE=true cargo build --release

FROM alpine:3.21

RUN apk add --no-cache fuse3 ca-certificates

COPY --from=builder /app/target/release/riven /usr/local/bin/riven

RUN mkdir -p /mnt/riven /logs && \
    echo "user_allow_other" >> /etc/fuse.conf

ENV RIVEN_SETTING__VFS_MOUNT_PATH=/mnt/riven \
    RIVEN_SETTING__LOG_DIRECTORY=/logs \
    RIVEN_SETTING__GQL_PORT=3000 \
    SQLX_OFFLINE=true

EXPOSE 3000

ENTRYPOINT ["riven"]
