package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CountTuple;
import common.SegmentIdentifier;
import common.TollNotification;
import util.Log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TollNotificationCv extends RichFlatMapFunction<Tuple3<Long, CountTuple, Long>, Tuple3<Long, TollNotification, Long>> {
    private static final Logger LOG = Log.get(TollNotificationCv.class);

    //////////
    /**
     * Buffer for accidents.
     */
    private Set<SegmentIdentifier> currentMinuteAccidents = new HashSet<>();
    /**
     * Buffer for accidents.
     */
    private Set<SegmentIdentifier> previousMinuteAccidents = new HashSet<>();
    /**
     * Buffer for car counts.
     */
    private Map<SegmentIdentifier, Integer> currentMinuteCounts = new HashMap<>();
    /**
     * Buffer for car counts.
     */
    private Map<SegmentIdentifier, Integer> previousMinuteCounts = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<SegmentIdentifier, Integer> currentMinuteLavs = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<SegmentIdentifier, Integer> previousMinuteLavs = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;
    //////////

    @Override
    public void flatMap(Tuple3<Long, CountTuple, Long> tuple, Collector<Tuple3<Long, TollNotification, Long>> collector) throws Exception {
        LOG.debug("TollNotificationCv");

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f2;

        collector.collect(new Tuple3<>(timestamp, new TollNotification(), timestamp_ext));//as an indication.

        CountTuple inputCountTuple = tuple.f1;

        this.checkMinute(inputCountTuple.minuteNumber);

        if (inputCountTuple.isProgressTuple()) {
            return;
        }

        SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
        segmentIdentifier.xway = inputCountTuple.xway;
        segmentIdentifier.segment = inputCountTuple.segment;
        segmentIdentifier.direction = inputCountTuple.direction;
        this.currentMinuteCounts.put(segmentIdentifier, inputCountTuple.count);
    }

    private void checkMinute(short minute) {
        //due to the tuple may be send in reverse-order, it may happen that some tuples are processed too late.
//        assert (minute >= this.currentMinute);

        if (minute < this.currentMinute) {
            //restart..
            currentMinute = minute;
        }
        if (minute > this.currentMinute) {
            this.currentMinute = minute;
            this.previousMinuteAccidents = this.currentMinuteAccidents;
            this.currentMinuteAccidents = new HashSet<>();
            this.previousMinuteCounts = this.currentMinuteCounts;
            this.currentMinuteCounts = new HashMap<>();
            this.previousMinuteLavs = this.currentMinuteLavs;
            this.currentMinuteLavs = new HashMap<>();
        }
    }
}
