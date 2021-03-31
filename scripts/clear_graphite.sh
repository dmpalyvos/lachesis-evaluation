#!/usr/bin/env bash

if cd 'BASEDIRHERE/scheduling-queries/grafana-graphite/graphite/storage/whisper'; then
  if [[ $(basename "$PWD") == "whisper" ]]; then
    echo "Cleaning up graphite..."
    sudo rm -rf ./*
  else
    echo "ERROR cleaning up graphite!"
  fi
fi
 
