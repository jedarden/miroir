# ── Scratch image with static musl binary ───────────────────────────────────
# Plan §7: expects miroir-proxy-linux-amd64 to be built by CI and placed in build context
# Build step (plan §7 cargo-build template):
#   apt-get install -qy musl-tools
#   rustup target add x86_64-unknown-linux-musl
#   cargo build --release --target x86_64-unknown-linux-musl --features miroir-core/kafka-sink -p miroir-proxy
#   cargo build --release --target x86_64-unknown-linux-musl --features miroir-core/kafka-sink -p miroir-ctl
#   sha256sum miroir-proxy-linux-amd64 > miroir-proxy-linux-amd64.sha256
FROM scratch
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.source=https://github.com/jedarden/miroir
LABEL org.opencontainers.image.version=${VERSION}
LABEL org.opencontainers.image.revision=${REVISION}
LABEL org.opencontainers.image.licenses=MIT

COPY miroir-proxy-linux-amd64 /miroir-proxy
EXPOSE 7700 9090
ENTRYPOINT ["/miroir-proxy"]
CMD ["--config", "/etc/miroir/config.yaml"]
