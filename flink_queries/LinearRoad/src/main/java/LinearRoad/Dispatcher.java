package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.PositionReport;
import util.CountStat;
import util.Log;
import util.PeriodicThroughputPrinter;
import util.Stats;

public class Dispatcher extends
    RichFlatMapFunction<Tuple3<Long, String, Long>, Tuple3<Long, PositionReport, Long>> {

  private static final Logger LOG = Log.get(Dispatcher.class);
  private transient CountStat throughputStat;
  private final String statisticsFolder;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.throughputStat = new CountStat(
        Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.SOURCE_THROUGHPUT_FILE));
  }

  public Dispatcher(String statisticsFolder) {
    this.statisticsFolder = statisticsFolder;
  }

  @Override
  public void flatMap(Tuple3<Long, String, Long> tuple,
      Collector<Tuple3<Long, PositionReport, Long>> collector) throws Exception {
    throughputStat.increase(1);
    // fetch values from the tuple
    long timestamp = tuple.f0;
    long timestamp_ext = tuple.f2;
    String line = tuple.f1;
    LOG.debug("Dispatcher: {}", line);

    // parse the string
    String[] token = line.split(",");
    short type = Short.parseShort(token[0]);
    if (type == 0) { // TODO constant
      // build and emit the position report
      PositionReport positionReport = new PositionReport();
      positionReport.type = type;
      positionReport.time = Integer.parseInt(token[1]);
      positionReport.vid = Integer.parseInt(token[2]);
      positionReport.speed = Integer.parseInt(token[3]);
      positionReport.xway = Integer.parseInt(token[4]);
      positionReport.lane = Short.parseShort(token[5]);
      positionReport.direction = Short.parseShort(token[6]);
      positionReport.segment = Short.parseShort(token[7]);
      positionReport.position = Integer.parseInt(token[8]);
      collector.collect(new Tuple3<>(timestamp, positionReport, timestamp_ext));
    }
  }
}
