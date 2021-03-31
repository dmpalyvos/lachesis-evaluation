#!/usr/bin/env bash

GRAPHITE_HOST="$1"
[[ -z $GRAPHITE_HOST ]] && GRAPHITE_HOST="localhost"

GRAPHITE_PORT=2003
HOSTNAME=$(hostname)
NUMBER_CPUS=$(nproc)
TOTAL_MEMORY=$(awk '/MemTotal/ {print $2 / 1024}' /proc/meminfo)

recordUtilization() {
    CPUMEM=$(top -b -n 1 -p"$(pgrep -d, -f "$1")" 2> /dev/null)
    CPUMEM=$(echo "$CPUMEM" | awk -v ncpu="$NUMBER_CPUS" -v nmem="$TOTAL_MEMORY" 'BEGIN{ cpu = 0; memory = 0; } NR > 7 { cpu+=$9; memory += $10 } END { print cpu/ncpu, memory*nmem/100; }')
    CPU=$(echo "$CPUMEM" | cut -d' ' -f1)
    MEM=$(echo "$CPUMEM" | cut -d' ' -f2)
    TIME_SECONDS=$(date +%s)
    echo "$2.$HOSTNAME.utilization.cpu.percent $CPU $TIME_SECONDS" | nc -N "$GRAPHITE_HOST" "$GRAPHITE_PORT" 2> /dev/null
    echo "$2.$HOSTNAME.utilization.memory.mb $MEM $TIME_SECONDS" | nc -N "$GRAPHITE_HOST" "$GRAPHITE_PORT" 2> /dev/null
}

while sleep 1; do
    recordUtilization "name=Lachesis" "lachesis"
    recordUtilization "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" "flink"
    recordUtilization "name=Storm" "Storm"
    recordUtilization "/opt/graphite" "graphite"
done