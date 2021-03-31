package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

public class ECR_reordering extends RichMapFunction<
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> {
    private static final Logger LOG = Log.get(ECR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.ECR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ECR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ECR_BUCKETS_PER_WORD;
        double beta = Constants.ECR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> map(Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f7;
        CallDetailRecord cdr = tuple.f5;

        if (cdr.callEstablished) {
            String caller = cdr.callingNumber;
            // add numbers to filters
            filter.add(caller, 1, cdr.answerTimestamp);
            double ecr = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> out = new Tuple8<>(timestamp, tuple.f1, tuple.f2, tuple.f3, tuple.f4, cdr, ecr, timestamp_ext);
            LOG.debug("tuple out: {}", out);
            return out;
        } else {
            Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> out = new Tuple8<>(timestamp, tuple.f1, tuple.f2, tuple.f3, tuple.f4, cdr, -1.0, timestamp_ext);
            LOG.debug("tuple out: {}", out);
            return out;
        }
    }
}
