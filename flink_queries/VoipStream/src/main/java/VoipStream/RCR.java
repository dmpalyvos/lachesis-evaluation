package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

public class RCR extends RichFlatMapFunction<
        Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> {
    private static final Logger LOG = Log.get(RCR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.RCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.RCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.RCR_BUCKETS_PER_WORD;
        double beta = Constants.RCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple, Collector<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        long timestamp_ext = tuple.f8;
        CallDetailRecord cdr = tuple.f6;

        if (cdr.callEstablished) {
            String key = tuple.f0;

            // default stream
            if (key.equals(cdr.callingNumber)) {
                String callee = cdr.calledNumber;
                filter.add(callee, 1, cdr.answerTimestamp);
            }
            // backup stream
            else {
                String caller = cdr.callingNumber;
                double rcr = filter.estimateCount(caller, cdr.answerTimestamp);

                Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long> out = new Tuple8<>(ScorerMap.RCR, timestamp, caller, cdr.answerTimestamp, rcr, cdr, tuple.f7, timestamp_ext);
                collector.collect(out);
                LOG.debug("tuple out: {}", out);
            }
        }
    }
}
