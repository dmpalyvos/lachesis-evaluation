#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 #reps #duration_min kafka_host"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage
[[ -z $3 ]] && usage

REPS="$1"
DURATION="$2"
KAFKA_HOST="$3"
DATE_CODE=$(date +%j_%H%M)
EXPERIMENT_FOLDER="${COMMIT_CODE}_${DATE_CODE}"

./scripts/run.py ./scripts/templates/LiebreSynthetic20QBlocking.yaml -d "$DURATION" -r "$REPS" --statisticsHost "$(hostname)" --kafkaHost "$KAFKA_HOST" -c "$DATE_CODE"

./reproduce/plot.py --plots liebre-20q-blocking --path "data/output/$EXPERIMENT_FOLDER"