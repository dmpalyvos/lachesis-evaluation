package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import util.Log;

import java.util.Map;

public class FoFiR_forwarding extends RichFlatMapFunction<
        Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>,
        Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> {
    private static final Logger LOG = Log.get(FoFiR.class);

    private ScorerMap scorerMap;

    @Override
    public void open(Configuration parameters) {

        scorerMap = new ScorerMap(new int[]{ScorerMap.RCR, ScorerMap.ECR});
    }

    @Override
    public void flatMap(Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long> tuple, Collector<Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        long timestamp_ext = tuple.f7;
        int source = tuple.f0;

        CallDetailRecord cdr = tuple.f5;
        String number = tuple.f2;
        long answerTimestamp = tuple.f3;
        double rate = tuple.f4;

        // forward the tuples from ECR but continue the computation
        if (source == ScorerMap.ECR) {
            Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long> out = new Tuple8<>(source, timestamp, tuple.f2, tuple.f3, tuple.f4, tuple.f5, -1.0, timestamp_ext);
            collector.collect(out);
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

                Tuple8<Integer, Long, String, Long, Double, CallDetailRecord, Double, Long> out = new Tuple8<>(ScorerMap.FoFiR, timestamp, number, answerTimestamp, score, cdr, -1.0, timestamp_ext);
                collector.collect(out);
                LOG.debug("tuple out: {}", out);

                map.remove(key);
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, rate);
            map.put(key, entry);
        }
    }
}
