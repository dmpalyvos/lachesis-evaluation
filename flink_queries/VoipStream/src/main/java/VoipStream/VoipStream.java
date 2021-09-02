package VoipStream;

import java.util.Properties;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import common.CallDetailRecord;
import org.apache.logging.log4j.util.Strings;
import util.Configuration;
import java.io.IOException;
import org.slf4j.Logger;
import util.Log;

public class VoipStream {
    private static final Logger LOG = Log.get(VoipStream.class);

    // Main
    public static void main(String[] args) throws IOException {
        Configuration configuration = Configuration.fromArgs(args);
        String variant = configuration.getTree().get("variant").textValue();

        // initialize the environment
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        if (configuration.getBufferTimeout() > 0) {
            streamExecutionEnvironment.setBufferTimeout(configuration.getBufferTimeout());
        }
        long gen_rate = configuration.getGenRate();
        long run_time = configuration.getRunTime();
        String datasetPath = configuration.getTree().get("dataset").textValue();
        String timedSource = configuration.getTree().get("timed_source").textValue();

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> source = null;
        // print app info
        if (Strings.isBlank(timedSource)) {
            LOG.info("Executing VoipStream with parameters:\n" +
                "  * rate: " + gen_rate + " tuples/second\n" +
                "  * execution time: " + run_time +  " seconds\n" +
                "  * topology: complex with 15 operators");
            source = streamExecutionEnvironment
                .addSource(new LineReaderSource(run_time, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"));
        }
        else if ("local".equals(timedSource)) {
            LOG.info("Executing VoipStream with parameters:\n" +
                "  * rate: " + gen_rate + " tuples/second with external generation\n" +
                "  * execution time: " + run_time +  " seconds\n" +
                "  * topology: complex with 15 operators");
            source = streamExecutionEnvironment
                .addSource(new LineReaderTimerSource(run_time, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"));
        }
        else if ("kafka".equals(timedSource)) {
            LOG.info("Executing VoipStream with parameters:\n" +
                "  * rate: " + gen_rate + " tuples/second with kafka generation\n" +
                "  * execution time: " + run_time +  " seconds\n" +
                "  * topology: complex with 15 operators");
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

        // run the variant
        switch (variant) {
            case "default":
                runDefaultVariant(streamExecutionEnvironment, configuration, source);
                break;
            case "reordering":
                runReorderingVariant(streamExecutionEnvironment, configuration, source);
                break;
            case "forwarding":
                runForwardingVariant(streamExecutionEnvironment, configuration, source);
                break;
            default:
                System.err.println("Unknown variant");
                System.exit(1);
        }
    }

    private static void runDefaultVariant(StreamExecutionEnvironment streamExecutionEnvironment,
        Configuration configuration,
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> source) {

        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> dispatcher = source
                .map(new Parser(configuration.getStatisticsFolder())).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> rcr = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> ecr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> fofir = rcr.union(ecr)
                .flatMap(new FoFiR()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> encr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> url = ecr.union(encr)
                .flatMap(new URL()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple6<Long, String, Long, Double, CallDetailRecord, Long>> score = acd.union(fofir, url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(configuration.getStatisticsFolder(),
                    configuration.sampleLatency())).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration, VoipStream.class.getSimpleName());
    }

    private static void runReorderingVariant(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration,
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> source) {

        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> dispatcher = source
                .map(new Parser(configuration.getStatisticsFolder())).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"))
                .keyBy(1) // calling number
                .map(new ECR_reordering()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"));

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> fofir = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2) // calling number
                .flatMap(new FoFiR_reordering()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> url = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR_reordering()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2) // calling number
                .flatMap(new URL()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple6<Long, String, Long, Double, CallDetailRecord, Long>> score = acd.union(fofir, url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(configuration.getStatisticsFolder(), configuration.sampleLatency())).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration, VoipStream.class.getSimpleName());
    }

    private static void runForwardingVariant(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration,
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> source) {

        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> dispatcher = source
                .map(new Parser(configuration.getStatisticsFolder())).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> rcr = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> ecr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> fofir = rcr.union(ecr)
                .flatMap(new FoFiR_forwarding()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>, Tuple> encr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>, Tuple> url = fofir.union(encr)
                .flatMap(new URL_forwarding()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple6<Long, String, Long, Double, CallDetailRecord, Long>> score = acd.union(url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(configuration.getStatisticsFolder(),
                    configuration.sampleLatency())).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration, VoipStream.class.getSimpleName());
    }
}
