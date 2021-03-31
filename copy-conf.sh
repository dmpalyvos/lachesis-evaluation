#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") source dst leader_host graphite_host
EOF
  exit 1
}

[[ $# -eq 4 ]] || usage 
[[ -n $1 ]] || usage
[[ -n $2 ]] || usage
[[ -n $3 ]] || usage
[[ -n $4 ]] || usage

cp "$1" "$2"
perl -i -pe "s/LEADER_HOST/$3/g" "$2"
perl -i -pe "s/GRAPHITE_HOST/$4/g" "$2"
