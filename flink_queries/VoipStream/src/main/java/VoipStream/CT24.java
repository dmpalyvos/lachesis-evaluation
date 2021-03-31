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

public class CT24 extends RichFlatMapFunction<
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> {
    private static final Logger LOG = Log.get(CT24.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.CT24_NUM_ELEMENTS;
        int bucketsPerElement = Constants.CT24_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.CT24_BUCKETS_PER_WORD;
        double beta = Constants.CT24_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple, Collector<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f7;
        CallDetailRecord cdr = tuple.f5;

        boolean newCallee = tuple.f4;
        if (cdr.callEstablished && newCallee) {
            String caller = tuple.f1;
            filter.add(caller, cdr.callDuration, cdr.answerTimestamp);
            double callTime = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long> out = new Tuple7<>(ScorerMap.CT24, timestamp, caller, cdr.answerTimestamp, callTime, cdr, timestamp_ext);
            collector.collect(out);
            LOG.debug("tuple out: {}", out);
        }
    }
}
