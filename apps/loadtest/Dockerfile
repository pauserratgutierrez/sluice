# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/loadtest ./cmd/loadtest

FROM alpine:3.22

RUN apk add --no-cache ca-certificates wget \
 && adduser -D -u 65532 loadtest

COPY --from=build /out/loadtest /usr/local/bin/loadtest

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/loadtest"]
