#!/usr/bin/env bash

# CLI ARGS
usage() { echo "Usage: $0 COMMANDS_FILE " 1>&2; exit 1; }

function configError() {
  echo "Error in experiment configuration: $1"
  exit 1
}

# NOTE: Experiment uses same list of workers as the distributed flink experiment
STORM="BASEDIRHERE/distributed-apache-storm-1.2.3/bin/storm"
FORCE_STORM_TASKSET="BASEDIRHERE/scheduling-queries/scripts/storm-worker-taskset.sh"
FORCE_LACHESIS_TASKSET="pgrep -f Dname=Lachesis | xargs -I {} sudo taskset -apc 0-3 {}"

readarray -t EXECUTOR_NODES < BASEDIRHERE/distributed-flink-1.11.2/conf/workers

# Experiment script
EXPERIMENT_COMMANDS=$1
if [ -z "$EXPERIMENT_COMMANDS" ]; then
  usage
fi
if [ ! -e "$EXPERIMENT_COMMANDS" ]; then
  echo "Experiment file $1 does not exist!"
  exit 1
fi
shift 


source "$EXPERIMENT_COMMANDS"
# Defines SPE_COMMAND, SCHEDULER_COMMAND, UTILIZATION_COMMAND, KAFKA_START_COMMAND, KAFKA_STOP_COMMAND,
# DURATION_SECONDS, EXPERIMENT_YAML, STATISTICS_FOLDER, STATISTICS_HOST, JOB_NAME

scheduler_pid=""
spe_pid=""
sleep_pid=""

checkStatusAndExit() {
  echo "Checking status and exiting"
  wait "$spe_pid"
  spe_exit_code="$?" 
  # Detect if scheduler exited correctly
  if [[ -n $scheduler_pid ]]; then 
    wait "$scheduler_pid"
    scheduler_exit_code="$?"
    # Exit codes 1-127 indicate JVM error. Exit codes >= 128 indicate signal
    if (( scheduler_exit_code > 0 && scheduler_exit_code < 128 )); then
      echo "[EXEC] Scheduler exited with error: $scheduler_exit_code"
      exit "$scheduler_exit_code"
    fi
  fi
  if (( spe_exit_code > 0 && spe_exit_code < 128)); then
    echo "[EXEC] SPE exited with error: $spe_exit_code"
    exit "$spe_exit_code"
  fi
  echo "[EXEC] Success"
  exit 0
}

function executeForAllNodes {
  for node in "${EXECUTOR_NODES[@]}"; do
    if [[ $node == "$(hostname)" || $node == "localhost" ]]; then
      eval "$1" &
    else
      ssh "$node" "$1 &>> BASEDIRHERE/scheduling-queries/remote.log &"
    fi
  done
}

function executeForAllNodesNoOutput {
  for node in "${EXECUTOR_NODES[@]}"; do
    if [[ $node == "$(hostname)" || $node == "localhost" ]]; then
      eval "$1" &> /dev/null &
    else
      ssh "$node" "$1 &> /dev/null &"
    fi
  done
}

function copyResultsFromAllNodes() {
  for node in "${EXECUTOR_NODES[@]}"; do
    if [[ $node == "$(hostname)" || ${node} == "localhost" ]]; then
      continue
    fi
    echo "[*] Copying results from $node"
    scp -r "${node}:${STATISTICS_FOLDER}/*" "$STATISTICS_FOLDER/"
  done
}

clearActiveProcs() {
  kill "$sleep_pid"
  "$STORM" kill "$JOB_NAME" -w 0 &> /dev/null 
  executeForAllNodes "BASEDIRHERE/scheduling-queries/scripts/kill-helpers.sh"
  [[ -n $KAFKA_STOP_COMMAND ]] && eval "$KAFKA_STOP_COMMAND"
  REAL_DURATION=$SECONDS
  echo "[EXEC] Experiment Duration: $REAL_DURATION"
  python3 "scripts/graphite_to_csv.py" --host "$STATISTICS_HOST" --experiment "$EXPERIMENT_YAML" --destination "$STATISTICS_FOLDER" --duration "$REAL_DURATION"
  copyResultsFromAllNodes
  checkStatusAndExit
}

trap clearActiveProcs SIGINT SIGTERM

ssh "$STATISTICS_HOST" "BASEDIRHERE/scheduling-queries/scripts/clear_graphite.sh &>> BASEDIRHERE/scheduling-queries/remote.log &"

executeForAllNodes "mkdir -p $STATISTICS_FOLDER"

[[ -n $KAFKA_START_COMMAND ]] && eval "$KAFKA_START_COMMAND"

SECONDS=0
eval "$SPE_COMMAND &"
spe_pid="$!"

executeForAllNodes "$UTILIZATION_COMMAND"

sleep 20 # Wait a bit for the tasks to be deployed
if [[ -n $SCHEDULER_COMMAND ]]; then
    executeForAllNodes "$SCHEDULER_COMMAND"
    sleep 10 
    executeForAllNodesNoOutput "$FORCE_LACHESIS_TASKSET"
fi

executeForAllNodes "$FORCE_STORM_TASKSET"

sleep "$DURATION_SECONDS" &
sleep_pid="$!"
wait "$sleep_pid"

clearActiveProcs