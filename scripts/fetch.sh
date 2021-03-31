#!/bin/bash

if [[ -z $1 ]]; then
  echo "Please provide node!"
  exit 1
fi
if [[ -z $2 ]]; then
  echo "Please provide commit #!"
  exit 1
fi

[[ -e data/remote/$2 ]] && { echo "Commit already exists!"; exit 1; }
ssh $1 "cd scheduling-queries/data/output/ && zip -r $2.zip $2"
scp -r "$1:scheduling-queries/data/output/$2.zip" "../data/remote"
cd "../data/remote" || exit 1
unzip "$2.zip"
