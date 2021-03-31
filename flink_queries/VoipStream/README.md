# Compile and run VoipStream

## Compile
$ cd scheduling_queries/flink_queries/VoipStream
$ mvn clean package

## Run
$ flink run --class VoipStream.VoipStream target/VoipStream-1.0.jar --rate <generation_rate> --time <running_time_sec> --conf <cofiguration_file.json>

Execute the VoipStream benchmark passing a generation rate, a running time duration in seconds, and a configuration file with other options.

Nota bene: remember to start the flink cluster before running the command above.

## Dataset Generation
Run the following command:
java -cp target/VoipStream-1.0.jar VoipStream.DatasetGenerator 1000000 10000000 > voip_stream.txt
