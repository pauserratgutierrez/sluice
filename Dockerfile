# syntax=docker/dockerfile:1

# Sluice is pure Go with no cgo, so the runtime image is a scratch-adjacent
# distroless-style alpine with just the binary and CA certificates. There is no
# libpg_query dependency: the Tier B predicate compiler uses a hand-written
# parser over a whitelisted grammar, and anything it does not recognise falls
# to Tier C. Correctness is preserved either way, and the binary stays static.

FROM golang:1.26-alpine AS builder

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# graph.
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
      -o /out/sluice ./cmd/sluice

FROM alpine:3.22 AS runner

RUN apk add --no-cache ca-certificates tzdata \
  && addgroup -g 65532 -S sluice \
  && adduser -u 65532 -S -G sluice sluice

COPY --from=builder /out/sluice /sluice

USER 65532:65532
EXPOSE 4000

ENTRYPOINT ["/sluice"]
