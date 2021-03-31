#!/usr/bin/env bash

set -e

QUERIES=("storm_queries/VoipStream" "storm_queries/LinearRoad" "flink_queries/VoipStream" "flink_queries/LinearRoad")

for query in "${QUERIES[@]}"; do
  echo "Compiling $query..."
  cd "$query"
  mvn clean package
  cd -
done

