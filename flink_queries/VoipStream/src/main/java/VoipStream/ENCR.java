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

public class ENCR extends RichFlatMapFunction<
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> {
    private static final Logger LOG = Log.get(ENCR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.ENCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ENCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ENCR_BUCKETS_PER_WORD;
        double beta = Constants.ENCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple, Collector<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f7;
        CallDetailRecord cdr = tuple.f5;

        boolean newCallee = tuple.f4;

        if (cdr.callEstablished && newCallee) {
            String caller = tuple.f1;
            filter.add(caller, 1, cdr.answerTimestamp);
            double rate = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long> out = new Tuple8<>(ScorerMap.ENCR, timestamp, caller, cdr.answerTimestamp, rate, cdr, tuple.f6, timestamp_ext);
            collector.collect(out);
            LOG.debug("tuple out: {}", out);
        }
    }
}
