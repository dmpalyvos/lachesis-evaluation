package LinearRoad;

import java.util.Properties;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import common.AvgVehicleSpeedTuple;
import common.CountTuple;
import common.PositionReport;
import common.TollNotification;
import org.apache.logging.log4j.util.Strings;
import util.Configuration;
import org.slf4j.Logger;
import util.Log;

public class LinearRoad {
    private static final Logger LOG = Log.get(LinearRoad.class);

    // Main
    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.fromArgs(args);
    	long gen_rate = configuration.getGenRate();
    	long run_time = configuration.getRunTime();
        String datasetPath = configuration.getTree().get("dataset").textValue();
        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();
        String timedSource = configuration.getTree().get("timed_source").textValue();

      // initialize the environment
      StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
      if (configuration.getBufferTimeout() > 0) {
        streamExecutionEnvironment.setBufferTimeout(configuration.getBufferTimeout());
      }
      SingleOutputStreamOperator<Tuple3<Long, String, Long>> source = null;
      // print app info
        if (Strings.isBlank(timedSource)) {
	        LOG.info("Executing LinearRoad with parameters:\n" +
	                 "  * rate: " + gen_rate + " tuples/second\n" +
	                 "  * execution time: " + run_time +  " seconds\n" +
	                 "  * topology: complex with 9 operators");
          source = streamExecutionEnvironment
              .addSource(new LineReaderSource(run_time, gen_rate, datasetPath)).name("source")
              .setParallelism(Topology.getParallelismHint(configuration, "source"));
        }
    	else if ("local".equals(timedSource)) {
	        LOG.info("Executing LinearRoad with parameters:\n" +
	                 "  * rate: " + gen_rate + " tuples/second with external generation\n" +
	                 "  * execution time: " + run_time +  " seconds\n" +
	                 "  * topology: complex with 9 operators");
          source = streamExecutionEnvironment
              .addSource(new LineReaderTimerSource(run_time, gen_rate, datasetPath)).name("source")
              .setParallelism(Topology.getParallelismHint(configuration, "source"));
        }
        else if ("kafka".equals(timedSource)) {
          LOG.info("Executing LinearRoad with parameters:\n" +
              "  * rate: " + gen_rate + " tuples/second with kafka generation\n" +
              "  * execution time: " + run_time +  " seconds\n" +
              "  * topology: complex with 9 operators");
          String kafkaTopic = configuration.getTree().get("kafka_topic").textValue();
          Properties properties = new Properties();
          properties.put("bootstrap.servers", configuration.getKafkaHost());
          source = streamExecutionEnvironment
              .addSource(new KafkaSource(kafkaTopic, properties)).name("source")
              .setParallelism(Topology.getParallelismHint(configuration, "source"));
        }
        else {
          throw new IllegalArgumentException(String.format("Unknown timed_source: %s", timedSource));
        }


        // prepare the topology
        SingleOutputStreamOperator<Tuple3<Long, PositionReport, Long>> dispatcher = source
                .flatMap(new Dispatcher(configuration.getStatisticsFolder())).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        SingleOutputStreamOperator<Tuple3<Long, AvgVehicleSpeedTuple, Long>> averageSpeed = dispatcher
                .flatMap(new AverageSpeed()).name("average_speed")
                .setParallelism(Topology.getParallelismHint(configuration, "average_speed"));

        if (!aggressiveChaining) {
            averageSpeed.startNewChain();
        }

        SingleOutputStreamOperator<Tuple3<Long, TollNotification, Long>> speedBranch = averageSpeed
                .flatMap(new LastAverageSpeed()).name("last_average_speed")
                .setParallelism(Topology.getParallelismHint(configuration, "last_average_speed"))
                .flatMap(new TollNotificationLas()).name("toll_notification_las")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_las"));

        ///

        SingleOutputStreamOperator<Tuple3<Long, CountTuple, Long>> countVehicles = dispatcher
                .flatMap(new CountVehicles()).name("count_vehicles")
                .setParallelism(Topology.getParallelismHint(configuration, "count_vehicles"));

        if (!aggressiveChaining) {
            countVehicles.startNewChain();
        }

        SingleOutputStreamOperator<Tuple3<Long, TollNotification, Long>> countBranch = countVehicles
                .flatMap(new TollNotificationCv()).name("toll_notification_cv")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_cv"));

        ///

        SingleOutputStreamOperator<Tuple3<Long, TollNotification, Long>> positionBranch = dispatcher
                .flatMap(new TollNotificationPos()).name("toll_notification_pos")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_pos"));

        if (!aggressiveChaining) {
            positionBranch.startNewChain();
        }

        ///

        positionBranch.union(speedBranch, countBranch)
                .addSink(new DrainSink<>(configuration.getStatisticsFolder(),
                    configuration.sampleLatency())).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration, LinearRoad.class.getSimpleName());
    }
}
