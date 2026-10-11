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
# The report lands in results/ (RESULTS_DIR); Sluice's log of the run in
# results/logs/. Works from Git Bash on Windows and from any Linux shell with
# Docker Compose v2.
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
export RESULTS_DIR="${RESULTS_DIR:-./results}"
# Compose reads a bare relative path as the name of a volume.
case "$RESULTS_DIR" in
/* | ./* | ../* | [A-Za-z]:*) ;;
*) RESULTS_DIR="./$RESULTS_DIR" ;;
esac
mkdir -p "$RESULTS_DIR"

# The host's power plan goes into every report: on a laptop it moves the
# numbers more than most knobs.
if [ -z "${HOST_POWER_PLAN:-}" ]; then
	if command -v powercfg >/dev/null 2>&1; then
		HOST_POWER_PLAN=$(MSYS_NO_PATHCONV=1 powercfg /getactivescheme 2>/dev/null | tr -d '\r' |
			sed -n 's/.*: *\([0-9a-fA-F-]*\) *(\(.*\)).*/\2 \1/p')
	elif [ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
		HOST_POWER_PLAN="governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
	fi
	export HOST_POWER_PLAN
fi

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

# Every run starts from a new replication slot. A slot keeps what a previous
# run left undispatched (a run that saturated Sluice can leave hundreds of MB),
# and the next Sluice would replay it, setup's TRUNCATE included, into the
# new run's streams. With PostgreSQL up and no Sluice yet, the load test's
# slots are inactive and can go.
status=0
if "${compose[@]}" up -d --wait db >/dev/null 2>&1; then
	"${compose[@]}" exec -T db psql -U postgres -d postgres -tAc \
		"select slot_name from pg_replication_slots where slot_name like 'load\_%' and not active and pg_drop_replication_slot(slot_name) is not null" ||
		true
fi
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
