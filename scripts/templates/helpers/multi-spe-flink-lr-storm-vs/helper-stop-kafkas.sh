#!/usr/bin/env bash

KAFKA_HOST=""
KAFKA_COMMAND_LOG="BASEDIRHERE/scheduling-queries/kafka-source/command.log"

# Convert LR rate based on VS rate
while :; do
    case $1 in
        --kafka) 
            KAFKA_HOST="$2"
            shift
            ;;
        --) # End of all options.
            shift
            break
            ;;
        *) 
            break
            ;;
    esac
    shift
done

[[ -n "$KAFKA_HOST" ]] || { echo "Please provide kafka host with --kafka"; exit 1; }

set -x 

ssh "$KAFKA_HOST" "pkill -f start-source.sh &>> $KAFKA_COMMAND_LOG &"