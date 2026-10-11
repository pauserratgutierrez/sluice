#!/usr/bin/env bash
# Runs a plan: one load-test run per line, in order, one at a time.
#
#   scripts/queue.sh plans/phase2.plan [results/phase2/raw]
#
# A plan line is
#
#   <label> <profile> [-e FILE] [VAR=value ...]
#
# and a line starting with # is a comment. A run whose label already has a
# report in the results directory is skipped, so a plan interrupted halfway
# picks up where it stopped. Every line's outcome is appended to queue.log
# in the results directory.
set -uo pipefail
cd "$(dirname "$0")/.."

plan="${1:?usage: scripts/queue.sh <plan> [results dir]}"
export RESULTS_DIR="${2:-./results}"
mkdir -p "$RESULTS_DIR"
log="$RESULTS_DIR/queue.log"

while IFS= read -r line || [ -n "$line" ]; do
	line="${line%%#*}"
	line="$(printf '%s' "$line" | tr -d '\r')"
	read -r -a words <<<"$line"
	[ "${#words[@]}" -ge 2 ] || continue
	label="${words[0]}"
	profile="${words[1]}"
	args=("${words[@]:2}")
	if ls "$RESULTS_DIR"/*-"$label"-2*.json* >/dev/null 2>&1; then
		echo "$(date -u +%FT%TZ) skip $label (already measured)" | tee -a "$log"
		continue
	fi
	opts=()
	vars=()
	i=0
	while [ "$i" -lt "${#args[@]}" ]; do
		case "${args[$i]}" in
		-e) opts+=(-e "${args[$((i + 1))]}"); i=$((i + 2)) ;;
		-l | -f) opts+=("${args[$i]}"); i=$((i + 1)) ;;
		*) vars+=("${args[$i]}"); i=$((i + 1)) ;;
		esac
	done
	echo "$(date -u +%FT%TZ) start $label $profile ${args[*]}" | tee -a "$log"
	start=$(date +%s)
	scripts/run.sh "${opts[@]}" "$profile" LOAD_LABEL="$label" "${vars[@]}" </dev/null
	status=$?
	echo "$(date -u +%FT%TZ) done $label status=$status took=$(($(date +%s) - start))s" | tee -a "$log"
done <"$plan"
