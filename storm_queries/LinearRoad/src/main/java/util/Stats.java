package util;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.apache.storm.task.TopologyContext;

public class Stats {

  public static final String SAMPLE_LATENCY_KEY = "SAMPLE_LATENCY";
  private static final String STATISTIC_EXTENSION = "csv";
  private static final String SPE_NAME = "Storm";
  public static String STATS_FOLDER_KEY = "CUSTOM_STATS_FOLDER";
  public static final String SOURCE_THROUGHPUT_FILE = "throughput-raw";
  public static final String SINK_THROUGHPUT_FILE = "sink-throughput-raw";
  public static final String LATENCY_FILE = "latency-raw";
  public static final String END_LATENCY_FILE = "end-latency-raw";

  public static final int LATENCY_SAMPLE_EVERY = 100;
  public static final String LATENCY_SAMPLED_FILE = "latency-sampled";
  public static final String END_LATENCY_SAMPLED_FILE = "end-latency-sampled";

  public static String statisticsFile(Map stormConf, TopologyContext topologyContext,
      String filename) {
    return statisticsFile(topologyContext.getThisComponentId(), topologyContext.getThisTaskIndex(),
        (String) stormConf.get(STATS_FOLDER_KEY), filename);
  }

  public static String statisticsFile(String operator, Object taskIndex, String statisticsFolder,
      String filename) {
    return new StringBuilder(statisticsFolder).append(File.separator)
        .append(filename).append("_")
        .append(SPE_NAME).append(".")
        .append(jvmName()).append(".")
        .append(operator).append(".")
        .append(taskIndex).append(".").append(STATISTIC_EXTENSION)
        .toString();
  }

  private static String jvmName() {
    return ManagementFactory.getRuntimeMXBean().getName();
  }
}
