#!/usr/bin/env bash

set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") distributed_leader distributed_graphite
EOF
  exit
}

[[ $# -eq 2 ]] || usage 
[[ -n $1 ]] || usage
[[ -n $2 ]] || usage

LOCAL_LEADER="localhost"
LOCAL_GRAPHITE="localhost"
DISTRIBUTED_LEADER="$1"
DISTRIBUTED_GRAPHITE="$2"

COPY_CONF="$script_dir/copy-conf.sh"


cd "$script_dir"
sudo cp -r grafana-graphite/conf-graphite/ grafana-graphite/graphite/conf
sudo chown -R root:root grafana-graphite/graphite/conf 

cd "$script_dir/.."

# Flink
wget https://archive.apache.org/dist/flink/flink-1.11.2/flink-1.11.2-bin-scala_2.11.tgz
tar xvf flink-1.11.2-bin-scala_2.11.tgz
cp -R flink-1.11.2 distributed-flink-1.11.2
"$COPY_CONF" scheduling-queries/configs/flink-conf.yaml flink-1.11.2/conf/flink-conf.yaml "$LOCAL_LEADER" "$LOCAL_GRAPHITE"
"$COPY_CONF" scheduling-queries/configs/flink-conf.yaml distributed-flink-1.11.2/conf/flink-conf.yaml "$DISTRIBUTED_LEADER" "$DISTRIBUTED_GRAPHITE"
rm flink-1.11.2-bin-scala_2.11.tgz

# Storm 1.2.3
wget https://ftp.acc.umu.se/mirror/apache.org/storm/apache-storm-1.2.3/apache-storm-1.2.3.tar.gz
tar xvf apache-storm-1.2.3.tar.gz
cp -R apache-storm-1.2.3 distributed-apache-storm-1.2.3
"$COPY_CONF" scheduling-queries/configs/storm-1.2.3.yaml apache-storm-1.2.3/conf/storm.yaml "$LOCAL_LEADER" "$LOCAL_GRAPHITE"
"$COPY_CONF" scheduling-queries/configs/storm-1.2.3.yaml distributed-apache-storm-1.2.3/conf/storm.yaml "$DISTRIBUTED_LEADER" "$DISTRIBUTED_GRAPHITE"
rm apache-storm-1.2.3.tar.gz

# Storm 1.1.0 (for EdgeWise experiments)
wget -O apache-storm-1.1.0.zip https://chalmersuniversity.box.com/shared/static/hixzpb8u3dmggm5w89la0mlxfwol79rm.zip
unzip apache-storm-1.1.0.zip
"$COPY_CONF" scheduling-queries/configs/storm-1.1.0.yaml apache-storm-1.1.0/conf/storm.yaml "$LOCAL_LEADER" "$LOCAL_GRAPHITE"
rm apache-storm-1.1.0.zip

wget -O apache-storm-edgewise-1.1.0.zip https://chalmersuniversity.box.com/shared/static/u3gykk8how2fjbsslt0k2jmmgn09vtae.zip
unzip apache-storm-edgewise-1.1.0.zip
"$COPY_CONF" scheduling-queries/configs/storm-edgewise-1.1.0.yaml apache-storm-edgewise-1.1.0/conf/storm.yaml "$LOCAL_LEADER" "$LOCAL_GRAPHITE"
rm apache-storm-edgewise-1.1.0.zip


echo "Downloading kafka..."
cd "$script_dir/.."
wget https://ftp.acc.umu.se/mirror/apache.org/kafka/2.7.0/kafka_2.13-2.7.0.tgz
tar -xzf kafka_2.13-2.7.0.tgz
echo 'auto.create.topics.enable=false' >> kafka_2.13-2.7.0/config/server.properties


cd "$script_dir"
./compile.sh
