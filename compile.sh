#!/usr/bin/env bash

set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

# Liebre Queries

echo "Compiling liebre queries"
sleep 2
cd "$script_dir/liebre_queries/"
git clone https://github.com/vincenzo-gulisano/Liebre
cd Liebre
git checkout 7ffd5b2869
mvn clean install
cd "../liebre-experiments"
mvn clean package
cp target/liebre-experiments-1.1.0-SNAPSHOT-jar-with-dependencies.jar "$script_dir/liebre_queries/liebre-experiments-latest.jar"

echo "Compiling VS & LR"
sleep 2
cd "$script_dir"
# Flink & Storm Queries
./scripts/compile-vs-lr.sh

# Kafka source
echo "Compiling Kafka Source"
sleep 2
cd kafka-source
mvn clean package


cd "$script_dir/.."

# EdgeWise Queries
echo "Compiling EdgeWise Queries"
sleep 2
git clone https://github.com/dmpalyvos/EdgeWISE-Benchmarks
cd EdgeWISE-Benchmarks
wget -O dataset.zip https://chalmersuniversity.box.com/shared/static/m83jtmozhg58obau9xb8ly5qcgz6csl6.zip
unzip dataset.zip
rm dataset.zip
# If it fails the first time rerun once more and it should succeed
mvn compile package -DskipTests || mvn compile package -DskipTests

cd "$script_dir/lachesis"
# Lachesis
echo "Compiling Lachesis"
sleep 2
git clone https://github.com/dmpalyvos/lachesis
cd lachesis
mvn clean package
cp target/lachesis-0.1.jar ..

