#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 #reps #duration_min kafka_host graphite_host"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage
[[ -z $3 ]] && usage
[[ -z $4 ]] && usage

REPS="$1"
DURATION="$2"
KAFKA_HOST="$3"
STATISTICS_HOST="$4"
DATE_CODE=$(date +%j_%H%M)
COMMIT_CODE=$(git rev-parse --short HEAD)
EXPERIMENT_FOLDER="${COMMIT_CODE}_${DATE_CODE}"


../flink-1.11.2/bin/stop-cluster.sh

./scripts/run.py ./scripts/templates/MultiSpeServer.yaml -d "$DURATION" -r "$REPS" --statisticsHost "$STATISTICS_HOST" --kafkaHost "$KAFKA_HOST" -c "$DATE_CODE"

./reproduce/plot.py --plots multi-spe --path "data/output/$EXPERIMENT_FOLDER"
