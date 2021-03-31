#!/usr/bin/env bash

sudo pkill -f Lachesis
pkill -f 'scheduling-queries/scripts/utilization-*'
pkill -f 'storm-worker-taskset'
