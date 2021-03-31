#!/usr/bin/env bash

shopt -s nullglob # Prevent null globs

function usage() {
    echo "Usage: $0 path oldstr newstr"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage
[[ -z $3 ]] && usage

ROOT_FOLDER=$1
OLD="$2"
NEW="$3"


if [[ ! -e ${ROOT_FOLDER} ]]; then
  echo "ERROR: Directory $ROOT_FOLDER does not exist!"
  exit 1
fi

echo "Will replace $OLD with $NEW in $ROOT_FOLDER"
read -p "Press [ENTER] to continue"

for folder in "$ROOT_FOLDER"/**/; do
    name=$(basename "$folder")
    newName=${name//$OLD/$NEW}
    cmd="mv $ROOT_FOLDER/$name $ROOT_FOLDER/$newName"
    echo "$cmd"
    eval "$cmd"
done
