#!/usr/bin/env bash

declare -a LR_ARGS=()
declare -a VS_ARGS=()
declare -a SYN_ARGS=()



# Interprent the rate as the percentage of the total rate defined above
while :; do
    case $1 in
        --) # End of all options.
            shift
            break
            ;;
        --time) 
	    # Slightly different argument in liebre experiments
            LR_ARGS+=("$1" "$2")
            VS_ARGS+=("$1" "$2")
            SYN_ARGS+=("--durationSeconds" "$2")
	    shift
            ;;
        --kafkaHost) 
	    # Slightly different argument in liebre experiments
            LR_ARGS+=("$1" "$2")
            VS_ARGS+=("$1" "$2")
            SYN_ARGS+=("$1" "${2%:9092}")
	    shift
            ;;
        --statisticsHost) 
	    # Only needed for liebre
            SYN_ARGS+=("$1" "$2")
	    shift
            ;;
        *) 
            LR_ARGS+=("$1")
            VS_ARGS+=("$1")
            SYN_ARGS+=("$1")
            ;;
    esac
    shift
done

flinkPid=""
stormPid=""
liebrePid=""


STORM_VS_CONFIG="BASEDIRHERE/scheduling-queries/storm_queries/VoipStream/Configurations/seqs_kafka_odroid_multi.json"
FLINK_LR_CONFIG="BASEDIRHERE/scheduling-queries/flink_queries/LinearRoad/Configurations/seqs_kafka_odroid_multi.json"
SYN_KAFKA_TOPIC="liebre-syn"

on_exit() {
  kill "$flinkPid" 2> /dev/null
  kill "$stormPid" 2> /dev/null
  kill "$liebrePid" 2> /dev/null
}

trap on_exit EXIT

set -x 

# Start Liebre Queries
java -Dname=Liebre -Xmx8g -cp ./liebre_queries/liebre-experiments-latest.jar queries.synthetic.queries.Chain --randomSeed 0 --cost 400 --selectivity 1 --chainLength 3 --nqueries 20 --varianceFactor 5 --sourceAffinity 0,1,2,3,4,5,6,7 --workerAffinity 0,1,2,3,4,5,6,7 --maxThreads 4 --autoFlush --kafkaTopic "$SYN_KAFKA_TOPIC" "${SYN_ARGS[@]}" &

liebrePid="$!"

# Start Flink Queries

BASEDIRHERE/flink-1.11.2/bin/flink run --class LinearRoad.LinearRoad BASEDIRHERE/scheduling-queries/flink_queries/LinearRoad/target/LinearRoad-1.0.jar --conf "$FLINK_LR_CONFIG"  "${LR_ARGS[@]}" &

flinkPid="$!"

# Start Storm Queries

BASEDIRHERE/distributed-apache-storm-1.2.3/bin/storm jar BASEDIRHERE/scheduling-queries/storm_queries/VoipStream/target/VoipStream-1.0-SNAPSHOT.jar -Xmx8g -Dname=Storm VoipStream.VoipStream --conf "$STORM_VS_CONFIG" "${VS_ARGS[@]}" &

stormPid="$!"

wait "$flinkPid" "$stormPid" "$liebrePid"
