#!/usr/bin/env bash

declare -a LR_ARGS=()
declare -a VS_ARGS=()



# Interprent the rate as the percentage of the total rate defined above
while :; do
    case $1 in
        --) # End of all options.
            shift
            break
            ;;
        *) 
            LR_ARGS+=("$1")
            VS_ARGS+=("$1")
            ;;
    esac
    shift
done

flinkPid=""
stormPid=""


STORM_VS_CONFIG="BASEDIRHERE/scheduling-queries/storm_queries/VoipStream/Configurations/seqs_kafka_odroid_multi.json"
FLINK_LR_CONFIG="BASEDIRHERE/scheduling-queries/flink_queries/LinearRoad/Configurations/seqs_kafka_odroid_multi.json"

on_exit() {
  kill "$flinkPid" 2> /dev/null
  kill "$stormPid" 2> /dev/null
}

trap on_exit EXIT

set -x 

BASEDIRHERE/flink-1.11.2/bin/flink run --class LinearRoad.LinearRoad BASEDIRHERE/scheduling-queries/flink_queries/LinearRoad/target/LinearRoad-1.0.jar --conf "$FLINK_LR_CONFIG"  "${LR_ARGS[@]}" &

flinkPid="$!"

BASEDIRHERE/distributed-apache-storm-1.2.3/bin/storm jar BASEDIRHERE/scheduling-queries/storm_queries/VoipStream/target/VoipStream-1.0-SNAPSHOT.jar -Xmx800m -Dname=Storm VoipStream.VoipStream --conf "$STORM_VS_CONFIG" "${VS_ARGS[@]}" &

stormPid="$!"

wait "$flinkPid" "$stormPid"
