package VoipStream;

import org.apache.flink.api.java.tuple.Tuple7;
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
import util.Log;

import java.util.Map;

public class FoFiR_Forwarding extends BaseRichBolt {
    private static final Logger LOG = Log.get(FoFiR.class);

    private OutputCollector outputCollector;

    private ScorerMap scorerMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
        scorerMap = new ScorerMap(new int[]{ScorerMap.RCR, ScorerMap.ECR});
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "timestamp_ext", "calling_number", "answer_timestamp", "rate", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        long timestamp_ext = (long) tuple.getValueByField("timestamp_ext");
        int source = (int) tuple.getValueByField("source");

        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        String number = (String) tuple.getValueByField("calling_number");
        long answerTimestamp = (long) tuple.getValueByField("answer_timestamp");
        double rate = (double) tuple.getValueByField("rate");

        // forward the tuples from ECR but continue the computation
        if (source == ScorerMap.ECR) {
            Values out = new Values(source, timestamp, timestamp_ext, number, answerTimestamp, rate, cdr);
            outputCollector.emit(out);
            LOG.debug("tuple out: {}", out);
        }

        String key = String.format("%s:%d", number, answerTimestamp);

        Map<String, ScorerMap.Entry> map = scorerMap.getMap();
        if (map.containsKey(key)) {
            ScorerMap.Entry entry = map.get(key);
            entry.set(source, rate);

            if (entry.isFull()) {
                // calculate the score for the ratio
                double ratio = (entry.get(ScorerMap.ECR) / entry.get(ScorerMap.RCR));
                double score = ScorerMap.score(Constants.FOFIR_THRESHOLD_MIN, Constants.FOFIR_THRESHOLD_MAX, ratio);

                Values out = new Values(ScorerMap.FoFiR, timestamp, timestamp_ext, number, answerTimestamp, score, cdr);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);

                map.remove(key);
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, rate);
            map.put(key, entry);
        }

        //outputCollector.ack(tuple);
    }
}
