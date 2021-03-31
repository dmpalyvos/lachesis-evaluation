package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

public class PreRCR extends RichFlatMapFunction<
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> {
    private static final Logger LOG = Log.get(PreRCR.class);

    @Override
    public void flatMap(Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple, Collector<Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f7;

        // fetch values from the tuple
        String callingNumber = tuple.f1;
        String calledNumber = tuple.f2;
        long answerTimestamp = tuple.f3;
        boolean newCallee = tuple.f4;
        CallDetailRecord cdr = tuple.f5;

        // emits the tuples twice, the key is calling then called number
        Tuple9<String, Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> output = new Tuple9<>(null, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, tuple.f6, timestamp_ext);

        output.f0 = callingNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);


        output.f0 = calledNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);
    }
}
