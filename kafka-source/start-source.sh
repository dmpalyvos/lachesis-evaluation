#!/usr/bin/env bash

SOURCE_WAIT_SECONDS=60
TOPIC="experiment"
PARTITIONS="1"
KAFKA_DIR="BASEDIRHERE/kafka_2.13-2.7.0"
SOURCE_JAR="BASEDIRHERE/scheduling-queries/kafka-source/target/kafka-source-1.0-SNAPSHOT.jar"
args=("$@")
# Check if custom topic, #partitions specified
# [WARNING] Optional arguments should be passed first, otherwise they will be ignored!
while :; do
    case $1 in
        --topic) 
            TOPIC="$2"
            shift
            ;;
        --partitions) 
            PARTITIONS="$2"
            shift
            ;;
        --sleep)
            SOURCE_WAIT_SECONDS="$2"
            shift
            ;;
        --) # End of all options.
            break
            ;;
        *) 
            # Ignore all arguments after first unknown one
            break
            ;;
    esac
    shift
done

on_exit() {
  pkill -f 'source.KafkaFileSource' # Clean up java source on exit
  "$KAFKA_DIR"/bin/kafka-topics.sh --delete --topic "$TOPIC" --bootstrap-server localhost:9092 
}

trap on_exit EXIT

set -x
date

until "$KAFKA_DIR"/bin/kafka-topics.sh --create --topic "$TOPIC" --partitions "$PARTITIONS" --bootstrap-server localhost:9092
do
  "$KAFKA_DIR"/bin/kafka-topics.sh --delete --topic "$TOPIC" --bootstrap-server localhost:9092 
done

sleep "$SOURCE_WAIT_SECONDS"

java -Xmx500m -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$SOURCE_JAR" source.KafkaFileSource "${args[@]}"



