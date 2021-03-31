package util;

import java.io.File;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Configuration {

  public static final String USAGE = "Parameters: --rate <generation_rate> --time <running_time_sec> --conf <configuration_json_file> --statisticsFolder <statistics_folder> [--bufferTimeout <buffer_timeout_ms>] [--kafkaHost <kafka_host>]";
  private long gen_rate;
  private long run_time;
  private JsonNode configuration;
  private long bufferTimeout;
  private String kafkaHost;
  private String statisticsFolder;

  public Configuration(long _gen_rate, long _run_time, String _path, long _bufferTimeout, String _kafkaHost, String _statisticsFolder)
      throws IOException {
    gen_rate = _gen_rate;
    run_time = _run_time;
    bufferTimeout = _bufferTimeout;
    kafkaHost = _kafkaHost;
    this.statisticsFolder = _statisticsFolder;
    ObjectMapper mapper = new ObjectMapper();
    configuration = mapper.readTree(new File(_path));
  }

  public static Configuration fromArgs(String[] args) throws IOException {
    if (args.length != 6 && args.length != 8 && args.length != 10 && args.length != 12) {
      System.err.println(USAGE);
      System.exit(1);
    }
    boolean isCorrect = true;
    long gen_rate = 0;
    long run_time = 0;
    String path = null;
    long bufferTimeout = -1;
    String kafkaHost = null;
    String statisticsFolder = null;
    // parse command line arguments
    for (int i = 0; i < args.length; i += 2) {
      if (args[i].equals("--rate")) {
        gen_rate = Long.parseLong(args[i + 1]);
      } else if (args[i].equals("--time")) {
        run_time = Long.parseLong(args[i + 1]);
      } else if (args[i].equals("--conf")) {
        path = args[i + 1];
      } else if (args[i].equals("--bufferTimeout")) {
        bufferTimeout = Long.valueOf(args[i + 1]);
      } else if (args[i].equals("--kafkaHost")) {
        kafkaHost = args[i + 1];
      } else if (args[i].equals("--statisticsFolder")) {
        statisticsFolder = args[i + 1];
      } else {
        isCorrect = false;
      }
    }
    if (!isCorrect) {
      System.err.println("Incorrect input parameters");
      System.err.println(USAGE);
      System.exit(1);
    }
    return new Configuration(gen_rate, run_time, path, bufferTimeout, kafkaHost, statisticsFolder);
  }

  public long getGenRate() {
    return gen_rate;
  }

  public long getRunTime() {
    return run_time;
  }

  public JsonNode getTree() {
    return configuration;
  }

  public long getBufferTimeout() {
    return bufferTimeout;
  }

  public String getKafkaHost() {
    return kafkaHost;
  }

  public String getStatisticsFolder() {
    return statisticsFolder;
  }
}
