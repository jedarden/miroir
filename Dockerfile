# ── Stage 1: Build static musl binaries ──────────────────────────────────────
FROM rust:1.87-slim-bookworm AS builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/miroir
COPY . .

RUN cargo build --release --target x86_64-unknown-linux-musl -p miroir-proxy -p miroir-ctl

RUN strip -s target/x86_64-unknown-linux-musl/release/miroir-proxy \
        && strip -s target/x86_64-unknown-linux-musl/release/miroir-ctl

# ── Stage 2: Scratch image with static binary ───────────────────────────────
FROM scratch
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.source=https://github.com/jedarden/miroir
LABEL org.opencontainers.image.version=${VERSION}
LABEL org.opencontainers.image.revision=${REVISION}
LABEL org.opencontainers.image.licenses=MIT

COPY --from=builder /usr/src/miroir/target/x86_64-unknown-linux-musl/release/miroir-proxy /miroir-proxy
EXPOSE 7700 9090
ENTRYPOINT ["/miroir-proxy"]
CMD ["--config", "/etc/miroir/config.yaml"]
