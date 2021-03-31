# Compile and run LinearRoad

## Compile
$ cd scheduling_queries/flink_queries/LinearRoad
$ mvn clean package

## Run
$ flink run --class LinearRoad.LinearRoad target/LinearRoad-1.0.jar --rate <generation_rate> --time <running_time_sec> --conf <cofiguration_file.json>

Execute the LinearRoad benchmark passing a generation rate, a running time duration in seconds, and a configuration file with other options.

Nota bene: remember to start the flink cluster before running the command above.

## Dataset Generation
We use the tool available at [LinearGenerator]: https://github.com/walmartlabs/LinearGenerator
To compile the tool once downloaded:
	javac LinearGenerator/src/com/walmart/linearroad/generator/*
To run the generation:
	java -cp LinearGenerator/src/ com.walmart.linearroad.generator.LinearGen -o linear_road.txt -x 2 -m 1
