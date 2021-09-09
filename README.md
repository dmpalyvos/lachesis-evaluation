# Evaluation Artifacts for Lachesis

This repository contains all the necessary scripts to execute the experimental evaluation of: ***"Lachesis: A Middleware for Customizing OS Scheduling of Stream Processing Queries"***. 

The implementation of Lachesis is in a separate repository, [acessible here](https://github.com/dmpalyvos/lachesis).

The scripts in this repository will automatically fetch any other required repositories, so you only need to follow the instructions below to reproduce the experiments presented in the paper.

Related repositories:

- [EdgeWise-Benchmarks (Lachesis version)](https://github.com/dmpalyvos/EdgeWISE-Benchmarks)
- [Liebre](https://github.com/vincenzo-gulisano/Liebre)

## Environment Setup

### Requirements

The experiments have been executed on Odroid-XU4 devices running Ubuntu 18.04.5 LTS (kernel 4.14.180-178). They should compile and run on any device with a recent version of Linux, albeit with different performance results. 

The following are some minimum system requirements:

- Java 8
- Python 3.7 
- docker, docker-compose (see [instructions](https://docs.docker.com/engine/install/ubuntu/) and [post-installation](https://docs.docker.com/engine/install/linux-postinstall/))
- cgroup-tools (`sudo apt-get install cgroup-tools` in Ubuntu)
- passwordless sudo (both for Lachesis and for various helper scripts that interact with docker)
- passwordless ssh between nodes (necessary to manage remote infrastructure, e.g., flink cluster, kafka producer, graphite)


### Setup 

This process needs to be repeated in all nodes that participate in the experiments (even if they just run Graphite and/or Kafka). **The directory structure needs to be identical in all nodes for the automation scripts to work!**

#### Clone the Repository

Create a new folder for the experimental infrastructure and clone the repository inside that folder with the name `scheduling-queries` (mandatory). In the following, we assume that you name the folder lachesis-experiments, as shown below:

```bash
mkdir lachesis-experiments
cd lachesis-experiments
git clone https://github.com/dmpalyvos/lachesis-evaluation scheduling-queries
```

Specify the absolute path of the folder containing the repo **(and NOT the folder of the repo!)** using `update_paths.sh` (escaping slashes):
```bash
# For example, if you cloned the repository 
# into /home/yourusername/lachesis-experiments/scheduling-queries
# then you should run

cd ~/lachesis-experiments/scheduling-queries
./update_paths.sh '\/home\/yourusername\/lachesis-experiments'
```

#### Install the Python Requirements

The scripts require `pandas, matplotlib, seaborn, numpy, pyYAML, tqdm`. Using a virtual environment is suggested. 

You can use `pip install -r requirements.txt` to install all requirements automatically, or, if you only want to run the experiments and plot them in a different machine, `pip install -r requirements-minimal.txt` (see the troubleshooting section if you have problems installing the requirements in ARM devices).

####  Run the auto-setup Script 

Run the setup script, which will download and configure Storm, Flink, Liebre, Kafka, Graphite, the EdgeWise queries and compile everything.
The only arguments are (1) the hostname/IP of the node that will be running the SPE leader (Storm's nimbus and Flink's JobManager) in the distributed experiment and (2) the hostname/IP of the node that hosts graphite in the multi-SPE and distributed experiment. For the local experiments it is assumed that everything runs locally (you can still change the SPE configuration files manually later if needed).

```bash
./auto_setup.sh SPE_LEADER_HOSTNAME REMOTE_GRAPHITE_HOSTNAME
```

#### Configure Distributed Nodes

To be able to run distributed experiments, you need to also edit `lachesis-experiments/distributed-flink-1.11.2/conf/workers` and add the hostnames of at least 4 nodes that are also setup with the above procedure.

## Running Experiments

**DISCLAIMER**

> When cleaning up graphite DB after each experiment, a script runs `sudo rm -rf` on the graphite DB folder. 
> We have tried to make the script as safe as possible, but we do not take responsibility for any data loss. Use at your own risk!

- Each experiment has a respective script in the `reproduce` folder that runs it and plots the relevant figures automatically. 
- The table below describes the exact configurations of each experiment, the paper figures that it generates, the CLI arguments of each script, as well as the preparation actions you need to do before starting the experiment. The durations are always specified in minutes. 
- In the paper, reps >= 5 and duration >= 10 min for all experiments.
- When passing hostnames as arguments, it is safer to use `$(hostname)` instead of `localhost` to make sure the scripts work without issues.



### Detailed Experiment Configurations

| Figures | SPE | SPE Mode | Leader | Graphite | Source | Script | Args | Prep
--------- | ---- | -------- | ------------------| --------- | ------ | --- | --- | --- | 
| 5, 6 | Storm 1.1.0 | local | - | local | local | edgewise-etl.sh | reps duration | GL
| 7, 8 | Storm 1.1.0 | local | - | local | local | edgewise-stats.sh | reps duration | GL
| 1, 9, 13a | Storm 1.2.3 | local | - | local | kafka (remote) | storm-lr.sh | reps duration kafka_host | GL, K
| 10, 13b | Storm 1.2.3 | local | - | local | kafka (remote) | storm-vs.sh | reps duration kafka_host | GL, K
| 11, 13c | Flink 1.11.2 | cluster | local | local | kafka (remote) | flink-lr.sh | reps duration kafka_host | GL, K
| 12, 13d | Flink 1.11.2 | cluster | local | local | kafka (remote) | flink-vs.sh | reps duration kafka_host | GL, K
| 14 | Liebre 0.1.2 | local | - | local | kafka (remote) | liebre-20q.sh | reps duration kafka_host | GL, K
| 15 | Liebre 0.1.2 | local | - | local | kafka (remote) | liebre-20q-period.sh | reps duration kafka_host | GL, K
| 16 | Liebre 0.1.2 | local | - | local | kafka (remote) | liebre-20q-blocking.sh | reps duration kafka_host | GL, K
| 17 | Storm 1.2.3, Flink 1.11.2 | cluster | remote | remote | kafka (remote) | storm-dist-lr.sh, flink-dist-lr.sh | reps duration kafka_host graphite_host | GR, K, SD
| 18 | Storm 1.2.3, Flink 1.11.2, Liebre 0.1.2 | local (Storm), cluster (Flink) | local | remote | kafka (remote) | multi-spe-server.sh | reps duration kafka_host graphite_host | GR, K, M

### Preparation Cheatsheet

#### GL – Starting Graphite locally in the node that runs the experiment

In a separate terminal, run
```bash
cd lachesis-experiments/scheduling-queries/grafana-graphite
docker-compose up
```

#### K – Starting ZooKeeper and Kafka in the node that runs the kafka producer

*This node must be setup with the process outlined above and have the same directory structure as the node that runs the experiment.*

```bash
cd lachesis-experiments/kafka_2.13-2.7.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
# Separate terminal
bin/kafka-server-start.sh config/server.properties
```


#### M – Mutli-SPE Remote Graphite Config

This is only important if you are running the experiments in resource-constrained devices such as Odroids, in which case this experiment graphite needs to run remotely. 

Edit `lachesis-experiments/flink-1.11.2/conf/flink-conf.yaml` and replace `localhost` with the remote graphite host in L70. (Remember to revert if you run another experiment afterwards!)

#### GR – Starting Graphite in a Remote Node

*This node must be setup with the process outlined above and have the same directory structure as the node that runs the experiment.*

Same as GL but run in the graphite_host machine instead.

#### FD – Flink Distributed

Make sure the Flink `workers` file is correctly configured in all nodes and then start the experiment from the node that is running the JobManager.

#### SD – Storm Distributed

Make sure the Flink `workers` file is correctly configured (the scripts for Storm read from the same file). 

In the node that runs nimbus do:
```bash
cd lachesis-experiments/kafka_2.13-2.7.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
# Separate terminal
cd lachesis-experiments/distributed-apache-storm-1.2.3/bin
./storm nimbus
```

Start all Storm Supervisors in all the participating nodes (in contrast to Flink, Storm does not do this automatically):
```bash
cd lachesis-experiments/distributed-apache-storm-1.2.3/bin
./storm supervisor
```

### Troubleshooting

#### Storm JAVA_HOME

If you get errors about `JAVA_HOME` in Storm experiments, change the directory in `scripts/storm_do_run.sh` L70 
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-armhf
```
so that it points to your Java install.


#### Python Dependency Installation Issues

The following is specific to Ubuntu. If you have issues with installing/compiling the required python dependencies on ARM devices such as Odroids, you can install the following before retrying:

```bash
sudo apt-get install python3.7 
sudo apt-get install libtiff5-dev libjpeg8-dev libopenjp2-7-dev zlib1g-dev \
    libfreetype6-dev liblcms2-dev libwebp-dev tcl8.6-dev tk8.6-dev python3-tk \
    libharfbuzz-dev libfribidi-dev libxcb1-dev libyaml-dev gcc gfortran python-dev libopenblas-dev liblapack-dev cython
```

If you get errors about YAML CLoader, check [here](https://github.com/yaml/pyyaml/issues/108#issuecomment-370459912).

It is also possible to plot in a separate device, just copy the experiment results and then call `reproduce/plot.py` with the configuration found in the respective `reproduce` script.

#### Multi-Part Hostnames and Performance Plots

- **This is only relevant for the Storm experiments, if the nodes running the experiments have multi-part hostnames**, e.g., `a.b.c`. 
- After the experiments are done, the scripts fetch some metrics from graphite from further plotting. 
- However, Storm experiments format the metric keys based on the hostname, with the key format changing depending on the number of parts in the hostname (e.g., `a.b.c`). 
- Lachesis works fine in either case but the above scripts are designed to work with one-part hostnames, e.g., odroid-1, because this is what we used in our evaluation. This means that some metrics necessary for plotting the graphs might be missing if the hostnames of your nodes have more than one part. 
- It is possible to fix this manually by changing the `csv_queries` in the experiment template called by the respective `reproduce` script. For example, to convert the graphite query that fetches the throughput from one-part (e.g., `odroid-1`) to four-part hostnames (e.g., `a.b.org.odroid-1`) in `scripts/templates/StormLinearRoadKafka.yaml`, you would need to change the following:

```yaml
# Before
csv_queries:
    throughput: aliasByNode(Storm.*.*.*.source.*.emit-count.default.value, 4, 5)
# After
csv_queries:
    throughput: aliasByNode(Storm.*.*.*.*.*.*.source.*.emit-count.default.value, 7, 8)
```

We are looking for ways to reliably automate this in the future and will update the repository accordingly. 
