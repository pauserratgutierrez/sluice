# Independent realtime load-test app
#
# Consumes a published Sluice image (`ghcr.io/pauserratgutierrez/sluice:0.3.0` by
# default, `SLUICE_IMAGE` to override). It does not import the Sluice Go module
# and does not share the `deploy/` harness. The only coupling is the public
# protocol: JWT + POST `/stream` (SSE) + SQL writes and, for signalling, POST
# `/publish`, POST `/presence`, and `pg_logical_emit_message`.
#
#   make rls-a | rls-b | rls-c | issuer | broadcast | presence | mixed | kick | idle
#   make local P=idle    # the same profile against an image built from this tree

PROFILES := rls-a rls-b rls-c issuer broadcast presence mixed kick idle
COMPOSE := docker compose
COMPOSE_PROFILES := $(addprefix --profile ,$(PROFILES))
P ?= rls-a

.PHONY: $(PROFILES) local down nuke logs test

$(PROFILES):
	$(COMPOSE) --profile $@ up --build --abort-on-container-exit --exit-code-from runner-$@

local:
	docker build -t sluice:local ../..
	SLUICE_IMAGE=sluice:local SLUICE_PULL_POLICY=never \
		$(COMPOSE) --profile $(P) up --build --abort-on-container-exit --exit-code-from runner-$(P)

down:
	$(COMPOSE) $(COMPOSE_PROFILES) down --remove-orphans

nuke:
	$(COMPOSE) $(COMPOSE_PROFILES) down -v --remove-orphans

logs:
	$(COMPOSE) $(COMPOSE_PROFILES) logs --tail=200

test:
	go test ./...
