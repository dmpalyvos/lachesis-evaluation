package VoipStream;

import org.apache.commons.lang3.Validate;
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
import org.slf4j.Logger;
import util.Log;
import java.io.IOException;

public class VoipStream {
    private static final Logger LOG = Log.get(VoipStream.class);

    // Main
    public static void main(String[] args) throws IOException {
      Configuration configuration = Configuration.fromArgs(args);
      String variant = configuration.getTree().get("variant").textValue();
      long gen_rate = configuration.getGenRate();
      long run_time = configuration.getRunTime();
      String datasetPath = configuration.getTree().get("dataset").textValue();
      String timedSource = configuration.getTree().get("timed_source").textValue();

      // prepare the topology
      TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);

      if (Strings.isBlank(timedSource)) {
        LOG.info("Executing VoipStream with parameters:\n" +
            "  * rate: " + gen_rate + " tuples/second\n" +
            "  * execution time: " + run_time + " seconds\n" +
            "  * topology: complex with 15 operators");
        topologyBuilder.setSpout("source", new LineReaderSpout(gen_rate, datasetPath));
      } else if ("local".equals(timedSource)) {
        LOG.info("Executing VoipStream with parameters:\n" +
            "  * rate: " + gen_rate + " tuples/second with external generation\n" +
            "  * execution time: " + run_time + " seconds\n" +
            "  * topology: complex with 15 operators");
        topologyBuilder.setSpout("source", new LineReaderTimerSpout(gen_rate, datasetPath));
      } else if ("kafka".equals(timedSource)) {
        LOG.info("Executing VoipStream with parameters:\n" +
            "  * rate: " + gen_rate + " tuples/second with kafka generation\n" +
            "  * execution time: " + run_time + " seconds\n" +
            "  * topology: complex with 15 operators");
        String kafkaTopic = configuration.getTree().get("kafka_topic").textValue();
        RecordTranslator<String, String> translator = new SimpleRecordTranslator<>(
            new KafkaRecordTranslator(), new Fields("timestamp", "timestamp_ext", "line"));
        KafkaSpoutConfig<String, String> config = new KafkaSpoutConfig.Builder<String, String>(
            configuration.getKafkaHost(), kafkaTopic).setRecordTranslator(translator)
            .setProcessingGuarantee(ProcessingGuarantee.NO_GUARANTEE).build();
        config.getKafkaProps()
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.getKafkaProps()
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.getKafkaProps().put(ConsumerConfig.GROUP_ID_CONFIG, "DEFAULT");

        topologyBuilder.setSpout("source", new KafkaSpout<>(config));
      } else {
        throw new IllegalArgumentException(String.format("Unknown timed_source: %s", timedSource));
      }

      // run the variant
        switch (variant) {
            case "default":
                System.out.println(variant);
                runDefaultVariant(topologyBuilder, configuration);
                break;
            case "reordering":
                System.out.println(variant);
                runReorderingVariant(topologyBuilder, configuration);
                break;
            case "forwarding":
                System.out.println(variant);
                runForwardingVariant(topologyBuilder, configuration);
                break;
            default:
                System.err.println("Unknown variant: " + variant);
                System.exit(1);
        }
    }

    static void runDefaultVariant(TopologyBuilder topologyBuilder,
        Configuration configuration) {

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("dispatcher");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("ecr", new ECR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("encr", new ENCR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL())
                .fieldsGrouping("ecr", new Fields("calling_number"))
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR())
                .fieldsGrouping("rcr", new Fields("calling_number"))
                .fieldsGrouping("ecr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink())
                .shuffleGrouping("score");

        // start!
        Topology.submit(configuration.getRunTime(), topologyBuilder, configuration);
    }

    static void runReorderingVariant(TopologyBuilder topologyBuilder, Configuration configuration) {

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));
        topologyBuilder.setBolt("ecr", new ECR_Reordering())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("ecr");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("encr", new ENCR_Reordering())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL())
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("ecr");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR_Reordering())
                .fieldsGrouping("rcr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink())
                .shuffleGrouping("score");

        // start!
        Topology.submit(configuration.getRunTime(), topologyBuilder, configuration);
    }

    static void runForwardingVariant(TopologyBuilder topologyBuilder,
        Configuration configuration) {

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("dispatcher");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("ecr", new ECR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("encr", new ENCR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL_Forwarding())
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR_Forwarding())
                .fieldsGrouping("rcr", new Fields("calling_number"))
                .fieldsGrouping("ecr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink())
                .shuffleGrouping("score");

        // start!
        Topology.submit(configuration.getRunTime(), topologyBuilder, configuration);
    }
}
