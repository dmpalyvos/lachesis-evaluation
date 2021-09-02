package LinearRoad;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import util.Configuration;
import util.Log;
import org.apache.storm.utils.Utils;
import util.Stats;

public class Topology {

  private static final Logger LOG = Log.get(Topology.class);

  public static final String TOPOLOGY_NAME = "LinearRoad";

  private final static long POLLING_TIME_MS = 1000;
  private final static int BUFFER_SIZE = 32768; // XXX explicit default Storm value

  public static void submit(long runTime, TopologyBuilder topologyBuilder,
      Configuration configuration) {
    // build the topology
    StormTopology topology = topologyBuilder.createTopology();

    // set the buffer size to avoid excessive buffering at the spout
    Config config = new Config();
    config.put(Stats.STATS_FOLDER_KEY, configuration.getStatisticsFolder());
    config.put(Stats.SAMPLE_LATENCY_KEY, configuration.sampleLatency());
    if (configuration.workers() > 0) {
      config.setNumWorkers(configuration.workers());
      submitCluster(topology, config);
    } else {
      // submit it to storm
      submitLocal(runTime, topology, config);
    }

    // XXX force exit because the JVM may hang waiting for a dangling
    // reference...
    System.exit(0);
  }

  private static void submitLocal(long runTime, StormTopology topology, Config config) {
    try {
      LOG.info("Submitting topology...");
      // submit the topology to a local cluster
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(TOPOLOGY_NAME, config, topology);

      Utils.sleep(runTime * 1000); // kill after runTime seconds

      LOG.info("...killing topology...");
      cluster.killTopology(TOPOLOGY_NAME);

      LOG.info("...shutting down cluster...");
      cluster.shutdown();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  private static void submitCluster(StormTopology topology, Config config) {
    try {
      StormSubmitter.submitTopology(TOPOLOGY_NAME, config, topology);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
