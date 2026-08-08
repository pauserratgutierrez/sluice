#!/bin/sh
# Generate the harness secrets without needing a local Go toolchain.
#
#   sh deploy/keygen.sh              # print .env lines
#   sh deploy/keygen.sh >> .env      # append them
set -eu

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GO_IMAGE=${GO_IMAGE:-golang:1.26-alpine}

exec docker run --rm \
  -v "${REPO_ROOT}:/src:ro" \
  -w /src \
  -e GOFLAGS=-mod=mod \
  -e GOCACHE=/tmp/gocache \
  -e GOMODCACHE=/tmp/gomod \
  "${GO_IMAGE}" \
  go run ./cmd/keygen "$@"
