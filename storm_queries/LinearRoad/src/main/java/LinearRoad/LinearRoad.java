package LinearRoad;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.util.Strings;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.SimpleRecordTranslator;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import util.Configuration;
import util.Log;
import org.slf4j.Logger;
import java.io.IOException;

public class LinearRoad {

  private static final Logger LOG = Log.get(LinearRoad.class);

  // Main
  public static void main(String[] args) throws IOException {
    Configuration configuration = Configuration.fromArgs(args);
    long gen_rate = configuration.getGenRate();
    long run_time = configuration.getRunTime();
    String datasetPath = configuration.getTree().get("dataset").textValue();
    String timedSource = configuration.getTree().get("timed_source").textValue();

    // prepare the topology
    TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);

    if (Strings.isBlank(timedSource)) {
      LOG.info("Executing LinearRoad with parameters:\n" +
          "  * rate: " + gen_rate + " tuples/second\n" +
          "  * execution time: " + run_time + " seconds\n" +
          "  * topology: complex with 9 operators");
      topologyBuilder.setSpout("source", new LineReaderSpout(gen_rate, datasetPath));
    } else if ("local".equals(timedSource)) {
      LOG.info("Executing LinearRoad with parameters:\n" +
          "  * rate: " + gen_rate + " tuples/second with external generation\n" +
          "  * execution time: " + run_time + " seconds\n" +
          "  * topology: complex with 9 operators");
      topologyBuilder.setSpout("source", new LineReaderTimerSpout(gen_rate, datasetPath));
    } else if ("kafka".equals(timedSource)) {
      LOG.info("Executing LinearRoad with parameters:\n" +
          "  * rate: " + gen_rate + " tuples/second with kafka generation\n" +
          "  * execution time: " + run_time + " seconds\n" +
          "  * topology: complex with 9 operators");
      String kafkaTopic = configuration.getTree().get("kafka_topic").textValue();
      RecordTranslator<String, String> translator = new SimpleRecordTranslator<>(
          new KafkaRecordTranslator(), new Fields("timestamp", "timestamp_ext", "line"));
      KafkaSpoutConfig<String, String> config = new KafkaSpoutConfig.Builder<String, String>(
          configuration.getKafkaHost(), kafkaTopic).setRecordTranslator(translator)
          .setProcessingGuarantee(ProcessingGuarantee.NO_GUARANTEE).build();
      config.getKafkaProps().put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      config.getKafkaProps().put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      config.getKafkaProps().put(ConsumerConfig.GROUP_ID_CONFIG, "DEFAULT");

      topologyBuilder.setSpout("source", new KafkaSpout<>(config));
    } else {
      throw new IllegalArgumentException(String.format("Unknown timed_source: %s", timedSource));
    }

    topologyBuilder.setBolt("dispatcher", new Dispatcher()).shuffleGrouping("source");

    topologyBuilder.setBolt("average_speed", new AverageSpeed()).shuffleGrouping("dispatcher");
    topologyBuilder.setBolt("last_average_speed", new LastAverageSpeed())
        .shuffleGrouping("average_speed");
    topologyBuilder.setBolt("toll_notification_las", new TollNotificationLas())
        .shuffleGrouping("last_average_speed");

    topologyBuilder.setBolt("count_vehicles", new CountVehicles()).shuffleGrouping("dispatcher");
    topologyBuilder.setBolt("toll_notification_cv", new TollNotificationCv())
        .shuffleGrouping("count_vehicles");

    topologyBuilder.setBolt("toll_notification_pos", new TollNotificationPos())
        .shuffleGrouping("dispatcher");

    topologyBuilder.setBolt("sink", new DrainSink())
        .shuffleGrouping("toll_notification_las")
        .shuffleGrouping("toll_notification_cv")
        .shuffleGrouping("toll_notification_pos");

    // start!
    Topology.submit(run_time, topologyBuilder, configuration);
  }
}
