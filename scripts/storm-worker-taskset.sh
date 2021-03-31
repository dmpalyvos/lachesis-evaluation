#!/usr/bin/env bash

# WARNING: This needs to be a superset of any string included in the scheduler command
STORM_WORKER_PATTERN="org.apache.storm.daemon.worker"

while [[ ! $(pgrep -f storm.daemon.worker) ]]; do
    sleep 10
    done

echo "Forcing storm worker taskset..."
pgrep -f "$STORM_WORKER_PATTERN" | xargs -I {} taskset -apc 4-7 {} &> /dev/null
sleep 30
# Reapply in case more workers started in the meanwhile
pgrep -f "$STORM_WORKER_PATTERN" | xargs -I {} taskset -apc 4-7 {} &> /dev/null

