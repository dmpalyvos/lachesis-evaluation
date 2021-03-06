package VoipStream;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import util.AverageGauge;
import util.AvgStat;
import util.CountStat;
import util.Log;
import util.MetricGroup;
import util.Sampler;
import util.SamplingTimestampedRecorder;
import util.Stats;

public class DrainSink<T extends Tuple> extends RichSinkFunction<T> {

  private static final Logger LOG = Log.get(DrainSink.class);
  private final String statisticsFolder;
  private final boolean sampleLatency;
  private Sampler latency;
  private transient AverageGauge latencyGauge;
  private transient AverageGauge latencyGaugeExt;

  private transient AvgStat latencyStat;
  private transient AvgStat endLatencyStat;
  private transient CountStat sinkThroughputStat;

  private transient SamplingTimestampedRecorder latencySampler;
  private transient SamplingTimestampedRecorder endLatencySampler;


  public DrainSink(String statisticsFolder, boolean sampleLatency) {
    this.statisticsFolder = statisticsFolder;
    this.sampleLatency = sampleLatency;
  }

  @Override
  public void open(Configuration parameters) {
    //latency = new Sampler(samplingRate);
    latencyGauge = getRuntimeContext().getMetricGroup()
        .gauge("latency", new AverageGauge());
    latencyGaugeExt = getRuntimeContext().getMetricGroup()
        .gauge("latency-ext", new AverageGauge());
    this.latencyStat = new AvgStat(Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.LATENCY_FILE));
    this.endLatencyStat = new AvgStat(Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.END_LATENCY_FILE));
    this.sinkThroughputStat = new CountStat(Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.SINK_THROUGHPUT_FILE));

    if (sampleLatency) {
      this.latencySampler = new SamplingTimestampedRecorder(
          Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.LATENCY_SAMPLED_FILE),
          Stats.LATENCY_SAMPLE_EVERY);
      this.endLatencySampler = new SamplingTimestampedRecorder(
          Stats.statisticsFile(statisticsFolder, getRuntimeContext(),
              Stats.END_LATENCY_SAMPLED_FILE),
          Stats.LATENCY_SAMPLE_EVERY);
    }
  }

  @Override
  public void invoke(Tuple tuple, Context context) {
    LOG.debug("tuple in: {}", tuple);
    sinkThroughputStat.increase(1);
    assert tuple.getArity() > 1;

    long timestamp = tuple.getField(0);
    long timestamp_ext = tuple.getField(tuple.getArity()-1);
    long now = System.currentTimeMillis();
    //latency.add((now - timestamp) / 1e3, now); // microseconds
    long latency = now - timestamp;
    long latency_ext = now - timestamp_ext;
    latencyGauge.add(latency);
    latencyStat.add(latency);
    latencyGaugeExt.add(latency_ext);
    endLatencyStat.add(latency_ext);

    if (sampleLatency) {
      latencySampler.add(latency);
      endLatencySampler.add(latency_ext);
    }
  }

  @Override
  public void close() {
    MetricGroup.add("latency", latency);
  }
}
