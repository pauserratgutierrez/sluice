#!/usr/bin/env bash
# Builds sluice:pprof, the working tree's Sluice with pprof handlers on
# SLUICE_PPROF_ADDR (cmd/sluice/pprof.go, build tag pprof), for profiling runs:
#
#   scripts/build-pprof.sh
#   scripts/run.sh rooms SLUICE_IMAGE=sluice:pprof SLUICE_PULL_POLICY=never LOAD_PPROF_AT=5m ...
#
# It cross-compiles on the host with one core (a load test may be running) and
# copies the binary into the same runtime image as the release's.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .build
version=$(git describe --tags --always 2>/dev/null || echo dev)
(cd ../.. && GOMAXPROCS=1 GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
	go build -p 1 -tags pprof -trimpath -ldflags "-s -w -X main.version=${version}-pprof" \
	-o apps/loadtest/.build/sluice ./cmd/sluice)
cat >.build/Dockerfile <<'DOCKERFILE'
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
  && addgroup -g 65532 -S sluice \
  && adduser -u 65532 -S -G sluice sluice
COPY sluice /sluice
USER 65532:65532
EXPOSE 4000 6060
ENTRYPOINT ["/sluice"]
DOCKERFILE
docker build -q -t sluice:pprof .build
