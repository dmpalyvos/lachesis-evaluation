# Compile and run VoipStream

## Compile
$ cd scheduling_queries/storm_queries/VoipStream
$ mvn clean package

## Run
$ storm jar target/VoipStream-1.0-SNAPSHOT.jar -Xmx2g VoipStream.VoipStream --rate <generation_rate> --time <running_time_sec> --conf <cofiguration_file.json>

Execute the VoipStream benchmark passing a generation rate, a running time duration in seconds, and a configuration file with other options.

## Dataset Generation
Run the following command:
java -cp target/VoipStream-1.0.jar VoipStream.DatasetGenerator 1000000 10000000 > voip_stream.txt
