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
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

import java.util.Map;

public class ENCR_Reordering extends BaseRichBolt {
    private static final Logger LOG = Log.get(ENCR.class);

    private OutputCollector outputCollector;

    private ODTDBloomFilter filter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;

        int numElements = Constants.ENCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ENCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ENCR_BUCKETS_PER_WORD;
        double beta = Constants.ENCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "timestamp_ext", "calling_number", "answer_timestamp", "rate", "cdr", "ecr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        long timestamp_ext = (long) tuple.getValueByField("timestamp_ext");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");

        boolean newCallee = tuple.getBooleanByField("new_callee");

        if (cdr.callEstablished) {
            String caller = tuple.getStringByField("calling_number");

            // forward the tuple fro ECR
            double ecr = (double) tuple.getValueByField("ecr");
            if (ecr >= 0) {
                Values out = new Values(ScorerMap.ECR, timestamp, timestamp_ext, caller, cdr.answerTimestamp, ecr, cdr, -1.0);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);
            }

            if (newCallee) {
                filter.add(caller, 1, cdr.answerTimestamp);
                double rate = filter.estimateCount(caller, cdr.answerTimestamp);

                Values out = new Values(ScorerMap.ENCR, timestamp, timestamp_ext, caller, cdr.answerTimestamp, rate, cdr, -1.0);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);
            }
        }

        //outputCollector.ack(tuple);
    }
}
