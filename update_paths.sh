#!/usr/bin/env bash

[[ -z $1 ]] && { echo "Usage: $0 base_dir (single quoted and backslashes escaped)"; exit 1; }

find . -name "*.json" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
find . -name "*.yaml" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
find . -name "*do_run.sh" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
find . -name "clear_graphite.sh" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
find . -name "start-source.sh" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
find . -name "helper-*.sh" -exec perl -pi -e "s/BASEDIRHERE/$1/g" {} +
