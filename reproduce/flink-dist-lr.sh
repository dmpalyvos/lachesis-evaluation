#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 #reps #duration_min kafka_host"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage
[[ -z $3 ]] && usage
[[ -z $4 ]] && usage

REPS="$1"
DURATION="$2"
KAFKA_HOST="$3"
STATISTICS_HOST="$3"

DATE_CODE=$(date +%j_%H%M)
EXPERIMENT_FOLDER="${COMMIT_CODE}_${DATE_CODE}"

declare -a EXPERIMENTS=("FlinkLinearRoadKafkaDist4" "FlinkLinearRoadKafkaDist2" "FlinkLinearRoadKafkaDist1")

for experiment in "${EXPERIMENTS[@]}"; do
  echo "$experiment"
  sleep 10
  ./scripts/run.py ./scripts/templates/distributed/"$experiment".yaml -d "$DURATION" -r "$REPS" --statisticsHost "$STATISTICS_HOST" --kafkaHost "$KAFKA_HOST" -c "$DATE_CODE"
done

./reproduce/plot.py --plots distributed --path "data/output/$EXPERIMENT_FOLDER"