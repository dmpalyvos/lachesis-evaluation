# Compile and run LinearRoad

## Compile
$ cd scheduling_queries/storm_queries/LinearRoad
$ mvn clean package

## Run
$ storm jar target/LinearRoad-1.0-SNAPSHOT.jar -Xmx2g LinearRoad.LinearRoad --rate <generation_rate> --time <running_time_sec> --conf <cofiguration_file.json>

Execute the LinearRoad benchmark passing a generation rate, a running time duration in seconds, and a configuration file with other options.

## Dataset Generation
We use the tool available at [LinearGenerator]: https://github.com/walmartlabs/LinearGenerator
To compile the tool once downloaded:
	javac LinearGenerator/src/com/walmart/linearroad/generator/*
To run the generation:
	java -cp LinearGenerator/src/ com.walmart.linearroad.generator.LinearGen -o linear_road.txt -x 2 -m 1
