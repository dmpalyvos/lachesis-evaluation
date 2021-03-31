#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 #reps #duration_min"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage

REPS="$1"
DURATION="$2"
DATE_CODE=$(date +%j_%H%M)
EXPERIMENT_FOLDER="${COMMIT_CODE}_${DATE_CODE}"

./scripts/run.py ./scripts/templates/StormStats.yaml -d "$DURATION" -r "$REPS" --statisticsHost "$(hostname)" -c "$DATE_CODE"

./reproduce/plot.py --plots qs-comparison qs-hist --path "data/output/$EXPERIMENT_FOLDER"