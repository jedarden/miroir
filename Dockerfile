# miroir-proxy — scratch base, static musl binary
# Build:  cargo build --release --target x86_64-unknown-linux-musl -p miroir-proxy
#         strip -s target/x86_64-unknown-linux-musl/release/miroir-proxy
# Image:  docker build -t miroir-proxy .
FROM scratch
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.source=https://github.com/jedarden/miroir
LABEL org.opencontainers.image.version=${VERSION}
LABEL org.opencontainers.image.revision=${REVISION}
LABEL org.opencontainers.image.licenses=MIT

COPY target/x86_64-unknown-linux-musl/release/miroir-proxy /miroir-proxy
EXPOSE 7700 9090
ENTRYPOINT ["/miroir-proxy"]
CMD ["--config", "/etc/miroir/config.yaml"]
