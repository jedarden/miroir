# miroir-proxy - scratch base, static musl binary
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
