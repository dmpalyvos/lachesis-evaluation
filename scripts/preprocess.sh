#!/usr/bin/env bash

shopt -s nullglob # Prevent null globs

ROOT_FOLDER=$1

PREPROCESS_FILES=("throughput-raw" "sink-throughput-raw" "latency-raw" "latency-sampled"  "end-latency-raw" "end-latency-sampled")
COPY_FILES=("throughput" "sink-throughput" "external-rate" "latency" "end-latency" "input-queue" "output-queue" "external-queue" "cpu" "memory" "schedule-external" "schedule-internal" "cg-schedule-external" "cg-schedule-internal" "scheduler-cpu" "scheduler-memory" "graphite-cpu" "reads" "writes" "thread-cpu")
EXTENSION=".csv"


if [[ -z ${ROOT_FOLDER} ]]; then
  echo "Please provide root (commit) folder as argument"
  exit 1
fi

if [[ ! -e ${ROOT_FOLDER} ]]; then
  echo "ERROR: Directory $ROOT_FOLDER does not exist!"
  exit 1
fi

for experiment in "$ROOT_FOLDER"/**/; do
  rm "$experiment"/*.csv 2> /dev/null
  echo ">>> Processing $experiment"
  for execution in "$experiment"/**/; do
    executionCode=$(basename "$execution")
    rep=$executionCode
    counter=0
    for metric in "${PREPROCESS_FILES[@]}"; do
    # echo $metric
      for file in  "$execution/${metric}_"*.csv; do
        baseName=$(basename "$file" "$EXTENSION")
        nodeName=${baseName#${metric}_}
        awk -v rep="$rep" -v nodeName="$nodeName" -F "," 'NR > 1 {print rep "," nodeName "," $0}' "$file" >> "${experiment}/${metric}.csv"
        (( counter++ ))
      done
    done
    for metric in "${COPY_FILES[@]}"; do
        if [[ -e "$execution/$metric.csv" ]]; then
          awk -v rep="$rep" -F "," '{print rep "," $0}' "${execution}/$metric.csv" >> "${experiment}/${metric}.csv"
        fi
        (( counter++ ))
    done
  done
done
