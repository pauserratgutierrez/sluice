# Independent realtime load-test app
#
# Consumes the published Sluice image (`ghcr.io/pauserratgutierrez/sluice:0.1.4`).
# It does not import the Sluice Go module and does not share the `deploy/` harness.
# The only coupling is the public protocol: JWT + POST `/stream` (SSE) + SQL writes
# and, for signalling, POST `/publish`, POST `/presence`, and `pg_logical_emit_message`.
#
#   make rls-a | rls-b | rls-c | issuer | broadcast | presence | mixed | kick

PROFILES := rls-a rls-b rls-c issuer broadcast presence mixed kick
COMPOSE := docker compose
COMPOSE_PROFILES := $(addprefix --profile ,$(PROFILES))

.PHONY: $(PROFILES) down nuke logs test

$(PROFILES):
	$(COMPOSE) --profile $@ up --build --abort-on-container-exit --exit-code-from runner-$@

down:
	$(COMPOSE) $(COMPOSE_PROFILES) down --remove-orphans

nuke:
	$(COMPOSE) $(COMPOSE_PROFILES) down -v --remove-orphans

logs:
	$(COMPOSE) $(COMPOSE_PROFILES) logs --tail=200

test:
	go test ./...
