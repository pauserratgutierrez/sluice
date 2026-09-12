# syntax=docker/dockerfile:1

# Sluice is pure Go with no cgo, so the runtime image is a scratch-adjacent distroless-style alpine with just the binary and CA certificates. There is no libpg_query dependency: the Tier B predicate compiler uses pgplex/pgparser, and anything it does not recognise falls to Tier C. Correctness is preserved either way, and the binary stays static.

FROM golang:1.26-alpine AS builder

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module graph.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/go/pkg/mod \
  go mod download

COPY . .

ARG VERSION=dev
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/go/pkg/mod \
  CGO_ENABLED=0 GOOS=linux \
  go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/sluice ./cmd/sluice \
  && CGO_ENABLED=0 GOOS=linux \
  go build -trimpath -ldflags "-s -w" \
    -o /out/issuer-stub ./cmd/issuer-stub

# Harness-only. Must stay before `runner` so an untargeted build (GHCR) still
# produces the sluice runtime image.
FROM alpine:3.22 AS issuer-stub

RUN apk add --no-cache ca-certificates tzdata \
  && addgroup -g 65532 -S sluice \
  && adduser -u 65532 -S -G sluice sluice

COPY --from=builder /out/issuer-stub /issuer-stub

USER 65532:65532
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
  CMD ["/issuer-stub", "-healthcheck"]
ENTRYPOINT ["/issuer-stub"]

FROM alpine:3.22 AS runner

RUN apk add --no-cache ca-certificates tzdata \
  && addgroup -g 65532 -S sluice \
  && adduser -u 65532 -S -G sluice sluice

COPY --from=builder /out/sluice /sluice

USER 65532:65532
EXPOSE 4000

# The binary already implements -healthcheck: it GETs /healthz (registered both
# prefixed and unprefixed) and exits 0/1. No curl/wget in the image.
# Compose overrides these timings for the harness; this is the standalone default.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
  CMD ["/sluice", "-healthcheck"]

LABEL org.opencontainers.image.title="sluice" \
  org.opencontainers.image.description="Realtime data-streaming server for PostgreSQL" \
  org.opencontainers.image.source="https://github.com/pauserratgutierrez/sluice" \
  org.opencontainers.image.licenses="UNLICENSED"

ENTRYPOINT ["/sluice"]
