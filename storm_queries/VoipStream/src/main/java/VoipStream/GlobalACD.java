package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import common.VariableEWMA;
import util.Log;

import java.util.Map;

class GlobalACD extends BaseRichBolt {
    private static final Logger LOG = Log.get(GlobalACD.class);

    private OutputCollector outputCollector;

    private VariableEWMA avgCallDuration;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;

        double decayFactor = Constants.ACD_DECAY_FACTOR;
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "timestamp_ext", "answer_timestamp", "average"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        long timestamp_ext = (long) tuple.getValueByField("timestamp_ext");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        avgCallDuration.add(cdr.callDuration);

        Values out = new Values(ScorerMap.GlobalACD, timestamp, timestamp_ext, cdr.answerTimestamp, avgCallDuration.getAverage());
        outputCollector.emit(out);
        LOG.debug("tuple out: {}", out);

        //outputCollector.ack(tuple);
    }
}
