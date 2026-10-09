#!/usr/bin/env bash
# Runs one load-test profile and leaves the stack down afterwards.
#
#   scripts/run.sh [-e env/pin-20cpu.env] [-l] [-f] <profile> [VAR=value ...]
#
#   -e FILE   load a core layout (or any other variables) before the run
#   -l        build the working tree as sluice:local and test that
#   -f        start from an empty database volume (after a schema change)
#   VAR=value set any LOAD_* / SLUICE_* knob for this run
#
# The report lands in results/; Sluice's log of the run in results/logs/.
# Works from Git Bash on Windows and from any Linux shell with Docker
# Compose v2.
set -euo pipefail
cd "$(dirname "$0")/.."

fresh=0
local_build=0
while getopts "e:lf" opt; do
	case "$opt" in
	e)
		set -a
		# shellcheck disable=SC1090
		. "$OPTARG"
		set +a
		;;
	l) local_build=1 ;;
	f) fresh=1 ;;
	*) exit 2 ;;
	esac
done
shift $((OPTIND - 1))
profile="${1:?usage: scripts/run.sh [-e file] [-l] [-f] <profile> [VAR=value ...]}"
shift
for kv in "$@"; do
	export "${kv?}"
done

if [ "$local_build" = 1 ]; then
	docker build -t sluice:local ../..
	export SLUICE_IMAGE=sluice:local SLUICE_PULL_POLICY=never
fi

compose=(docker compose --profile "$profile")
if [ "$fresh" = 1 ]; then
	"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi

# Detached, then wait for the runner: with --abort-on-container-exit, the
# reconnect profile's restart of Sluice would end the whole run.
cleanup() { "${compose[@]}" down --remove-orphans >/dev/null 2>&1 || true; }
trap 'cleanup; exit 130' INT TERM
status=0
if "${compose[@]}" up -d --build; then
	"${compose[@]}" logs -f --no-color "runner-$profile" &
	follow=$!
	"${compose[@]}" wait "runner-$profile" >/dev/null || status=$?
	kill "$follow" 2>/dev/null || true
	wait "$follow" 2>/dev/null || true
else
	status=$?
fi

mkdir -p results/logs
stamp=$(date -u +%Y%m%dT%H%M%SZ)
for svc in sluice-rooms sluice-rls sluice-issuer; do
	if "${compose[@]}" ps -a --services 2>/dev/null | grep -qx "$svc"; then
		"${compose[@]}" logs --no-color --no-log-prefix "$svc" >"results/logs/${profile}-${LOAD_LABEL:-run}-${stamp}-${svc}.log" 2>&1 || true
	fi
done
cleanup
exit "$status"
