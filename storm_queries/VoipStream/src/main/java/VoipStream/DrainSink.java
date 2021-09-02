package VoipStream;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import util.AvgStat;
import util.CountStat;
import util.Log;
import util.SamplingTimestampedRecorder;
import util.Stats;

public class DrainSink extends BaseRichBolt {

  private static final Logger LOG = Log.get(DrainSink.class);
  private transient ReducedMetric reducedMetric;
  private transient ReducedMetric reducedMetricExt;
  private transient AvgStat latencyStat;
  private transient AvgStat endLatencyStat;
  private transient CountStat sinkThroughputStat;

  private transient SamplingTimestampedRecorder latencySampler;
  private transient SamplingTimestampedRecorder endLatencySampler;

  private OutputCollector outputCollector;
  private boolean sampleLatency;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.latencyStat = new AvgStat(Stats.statisticsFile(map, topologyContext, Stats.LATENCY_FILE));
    this.endLatencyStat = new AvgStat(
        Stats.statisticsFile(map, topologyContext, Stats.END_LATENCY_FILE));
    this.sinkThroughputStat = new CountStat(
        Stats.statisticsFile(map, topologyContext, Stats.SINK_THROUGHPUT_FILE));
    // initialize
    this.outputCollector = outputCollector;
    Long builtinPeriod = (Long) map.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS);
    reducedMetric = new ReducedMetric(new MeanReducer());
    reducedMetricExt = new ReducedMetric(new MeanReducer());
    topologyContext.registerMetric("total-latency", reducedMetric, builtinPeriod.intValue());
    topologyContext.registerMetric("total-latency-ext", reducedMetricExt, builtinPeriod.intValue());

    this.sampleLatency = (boolean) map.get(Stats.SAMPLE_LATENCY_KEY);
    if (sampleLatency) {
      this.latencySampler = new SamplingTimestampedRecorder(
          Stats.statisticsFile(map, topologyContext, Stats.LATENCY_SAMPLED_FILE),
          Stats.LATENCY_SAMPLE_EVERY);
      this.endLatencySampler = new SamplingTimestampedRecorder(
          Stats.statisticsFile(map, topologyContext, Stats.END_LATENCY_SAMPLED_FILE),
          Stats.LATENCY_SAMPLE_EVERY);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

  @Override
  public void execute(Tuple tuple) {
    LOG.debug("tuple in: {}", tuple);

    long timestamp = (long) tuple.getValueByField("timestamp");
    long timestamp_ext = (long) tuple.getValueByField("timestamp_ext");
    long now = System.currentTimeMillis();
    long lat = now - timestamp;
    long lat_ext = now - timestamp_ext;
    reducedMetric.update(lat);
    latencyStat.add(lat);
    reducedMetricExt.update(lat_ext);
    endLatencyStat.add(lat_ext);
    sinkThroughputStat.increase(1);

    if (sampleLatency) {
      latencySampler.add(lat);
      endLatencySampler.add(lat_ext);
    }

    //outputCollector.ack(tuple);
  }

  @Override
  public void cleanup() {
  }
}
