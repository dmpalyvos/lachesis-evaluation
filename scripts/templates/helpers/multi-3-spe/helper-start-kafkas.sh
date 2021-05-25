#!/usr/bin/env bash


KAFKA_COMMAND_LOG="/home/dimitris/lachesis-experiments/scheduling-queries/kafka-source/command.log"
STORM_VS_CONFIG="/home/dimitris/lachesis-experiments/scheduling-queries/storm_queries/VoipStream/Configurations/seqs_kafka_odroid_multi.json"
FLINK_LR_CONFIG="/home/dimitris/lachesis-experiments/scheduling-queries/flink_queries/LinearRoad/Configurations/seqs_kafka_odroid_multi.json"
VS_KAFKA_TOPIC="storm-vs"
LR_KAFKA_TOPIC="flink-lr"
SYN_KAFKA_TOPIC="liebre-syn"
KAFKA_HOST=""

declare -a LR_ARGS=()
declare -a VS_ARGS=()

LR_MAX_RATE="20000"
VS_MAX_RATE="15000"
SYN_MAX_RATE="15000"

# Interprent the rate as the percentage of the total rate defined above
while :; do
    case $1 in
        --rate) 
            percentage="$2"
            (( percentage > 100 )) && { echo "Please define rate as percentage!"; exit 1; }
            lr_rate=$(( (percentage * LR_MAX_RATE) / 100 ))
            vs_rate=$(( (percentage * VS_MAX_RATE) / 100 ))
            syn_rate=$(( (percentage * SYN_MAX_RATE) / 100 ))
            echo "LR Rate = $lr_rate"
            echo "VS Rate = $vs_rate"
            echo "SYN Rate = $syn_rate"
            VS_ARGS+=("$1")
            VS_ARGS+=("$vs_rate")
            LR_ARGS+=("$1")
            LR_ARGS+=("$lr_rate")
            SYN_ARGS+=("$1")
            SYN_ARGS+=("$syn_rate")
            shift
            ;;
        --kafka) 
            KAFKA_HOST="$2"
            shift
            ;;
        --) # End of all options.
            shift
            break
            ;;
        *) 
            LR_ARGS+=("$1")
            VS_ARGS+=("$1")
            SYN_ARGS+=("$1")
            ;;
    esac
    shift
done

[[ -n "$KAFKA_HOST" ]] || { echo "Please provide kafka host with --kafka"; exit 1; }

set -x 

ssh "$KAFKA_HOST" "/home/dimitris/lachesis-experiments/scheduling-queries/kafka-source/start-source.sh --topic $VS_KAFKA_TOPIC --configFile $STORM_VS_CONFIG ${VS_ARGS[@]} &>> $KAFKA_COMMAND_LOG &"

ssh "$KAFKA_HOST" "/home/dimitris/lachesis-experiments/scheduling-queries/kafka-source/start-source.sh --topic $LR_KAFKA_TOPIC --configFile $FLINK_LR_CONFIG ${LR_ARGS[@]} &>> $KAFKA_COMMAND_LOG &"

ssh "$KAFKA_HOST" "/home/dimitris/lachesis-experiments/scheduling-queries/kafka-source/start-source.sh --topic $SYN_KAFKA_TOPIC --inputFile DUMMY ${SYN_ARGS[@]}  &>> $KAFKA_COMMAND_LOG &"
