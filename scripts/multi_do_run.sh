#!/usr/bin/env bash

# CLI ARGS
usage() { echo "Usage: $0 COMMANDS_FILE " 1>&2; exit 1; }

function configError() {
  echo "Error in experiment configuration: $1"
  exit 1
}

START_CLUSTER="BASEDIRHERE/flink-1.11.2/bin/start-cluster.sh"
STOP_CLUSTER="BASEDIRHERE/flink-1.11.2/bin/stop-cluster.sh"

FORCE_GC_CMD="jcmd | grep org.apache.flink.runtime.taskexecutor.TaskManagerRunner | cut -d ' ' -f 1 | xargs -I {} jcmd {} GC.run"
FORCE_FLINK_TASKSET="jcmd | grep flink | cut -d ' ' -f 1 | xargs -I {} taskset -apc 4-7 {}"
FORCE_STORM_TASKSET="pgrep -f Dname=Storm | xargs -I {} taskset -apc 4-7 {}"
FORCE_LACHESIS_TASKSET="pgrep -f Dname=Lachesis | xargs -I {} sudo taskset -apc 0-3 {}"

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
# DURATION_SECONDS, EXPERIMENT_YAML, STATISTICS_FOLDER, STATISTICS_HOST

scheduler_pid=""
spe_pid=""
utilization_pid=""
job_stopper_pid=""

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

clearActiveProcs() {
  [[ -n $scheduler_pid ]] && sudo pkill -P "$scheduler_pid"
  [[ -n $utilization_pid ]] && kill "$utilization_pid"
  [[ -n $spe_pid ]] && kill "$spe_pid"
  [[ -n $job_stopper_pid ]] && kill "$job_stopper_pid"
  [[ -n $KAFKA_STOP_COMMAND ]] && eval "$KAFKA_STOP_COMMAND"
  python3 "scripts/flinkJobStopper.py" 5 &
  REAL_DURATION=$SECONDS
  echo "[EXEC] Experiment Duration: $REAL_DURATION"
  python3 "scripts/graphite_to_csv.py" --host "$STATISTICS_HOST" --experiment "$EXPERIMENT_YAML" --destination "$STATISTICS_FOLDER" --duration "$REAL_DURATION"
  checkStatusAndExit
}

restartFlinkCluster() {
  ## Restat Flink Cluster
  eval "$STOP_CLUSTER"
  sleep 15
  # Make absolutely sure that there is no leftover TaskManager
  pgrep -f "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" | xargs -I {} kill -9 {}
  eval "$START_CLUSTER"
  sleep 15
}

trap clearActiveProcs SIGINT SIGTERM

restartFlinkCluster

ssh "$STATISTICS_HOST" "BASEDIRHERE/scheduling-queries/scripts/clear_graphite.sh &>> BASEDIRHERE/scheduling-queries/remote.log &"
eval "$FORCE_GC_CMD" > /dev/null
eval "$FORCE_FLINK_TASKSET" > /dev/null

[[ -n $KAFKA_START_COMMAND ]] && eval "$KAFKA_START_COMMAND"

SECONDS=0
eval "$SPE_COMMAND &"
spe_pid="$!"
sleep 5
eval "$FORCE_STORM_TASKSET" > /dev/null

eval "$UTILIZATION_COMMAND &"
utilization_pid="$!"


if [[ -n $SCHEDULER_COMMAND ]]; then
    eval "$SCHEDULER_COMMAND &"
    scheduler_pid="$!"
    sleep 3
    eval "$FORCE_LACHESIS_TASKSET" > /dev/null 
fi

python3 "scripts/flinkJobStopper.py" "$DURATION_SECONDS" &
job_stopper_pid="$!"

wait "$spe_pid"
clearActiveProcs