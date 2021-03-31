package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import common.VariableEWMA;
import util.Log;

// XXX include some common fields to CT24 and ECR24 since Flink tuples must be strongly typed
public class GlobalACD extends RichMapFunction<
        Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> {
    private static final Logger LOG = Log.get(GlobalACD.class);

    private VariableEWMA avgCallDuration;

    @Override
    public void open(Configuration parameters) {

        double decayFactor = Constants.ACD_DECAY_FACTOR;
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long> map(Tuple8<Long, String, String, Long, Boolean, CallDetailRecord, Double, Long> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f7;
        CallDetailRecord cdr = tuple.f5;
        avgCallDuration.add(cdr.callDuration);

        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long> out = new Tuple7<>(ScorerMap.GlobalACD, timestamp, cdr.callingNumber, cdr.answerTimestamp, avgCallDuration.getAverage(), cdr, timestamp_ext);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
