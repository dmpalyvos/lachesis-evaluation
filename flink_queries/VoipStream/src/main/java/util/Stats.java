package util;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;

public class Stats {

  private static final String STATISTIC_EXTENSION = "csv";
  private static final String SPE_NAME = "Flink";
  public static final String SOURCE_THROUGHPUT_FILE = "throughput-raw";
  public static final String SINK_THROUGHPUT_FILE = "sink-throughput-raw";
  public static final String LATENCY_FILE = "latency-raw";
  public static final String END_LATENCY_FILE = "end-latency-raw";

  public static String statisticsFile(String folder, RuntimeContext context,
      String filename) {
    return statisticsFile(context.getTaskName(), context.getIndexOfThisSubtask(), folder, filename);
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
