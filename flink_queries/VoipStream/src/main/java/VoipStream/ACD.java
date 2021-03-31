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

public class ACD extends RichFlatMapFunction<
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> {
    private static final Logger LOG = Log.get(ACD.class);

    private double avg;
    private ScorerMap scorerMap;

    @Override
    public void open(Configuration parameters) {

        scorerMap = new ScorerMap(new int[]{ScorerMap.CT24, ScorerMap.ECR24});
    }

    @Override
    public void flatMap(Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long> tuple, Collector<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long>> collector) throws Exception {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        long timestamp_ext = tuple.f6;
        int source = tuple.f0;

        if (source == ScorerMap.GlobalACD) {
            avg = tuple.f4;
        } else {
            CallDetailRecord cdr = tuple.f5;
            String number = tuple.f2;
            long answerTimestamp = tuple.f3;
            double rate = tuple.f4;

            String key = String.format("%s:%d", number, answerTimestamp);

            Map<String, ScorerMap.Entry> map = scorerMap.getMap();
            if (map.containsKey(key)) {
                ScorerMap.Entry entry = map.get(key);
                entry.set(source, rate);

                if (entry.isFull()) {
                    // calculate the score for the ratio
                    double ratio = (entry.get(ScorerMap.CT24) / entry.get(ScorerMap.ECR24)) / avg;
                    double score = ScorerMap.score(Constants.ACD_THRESHOLD_MIN, Constants.ACD_THRESHOLD_MAX, ratio);

                    Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Long> out = new Tuple7<>(ScorerMap.ACD, timestamp, number, answerTimestamp, score, cdr, timestamp_ext);
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
}
