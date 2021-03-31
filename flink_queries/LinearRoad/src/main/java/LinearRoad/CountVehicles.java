package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CarCount;
import common.CountTuple;
import common.PositionReport;
import common.SegmentIdentifier;
import util.Log;

import java.util.HashMap;
import java.util.Map;

class CountVehicles extends RichFlatMapFunction<Tuple3<Long, PositionReport, Long>, Tuple3<Long, CountTuple, Long>> {
    private static final Logger LOG = Log.get(CountVehicles.class);

    //////////
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each segment to its count value.
     */
    private final Map<SegmentIdentifier, CarCount> countsMap = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = -1;
    //////////

    @Override
    public void flatMap(Tuple3<Long, PositionReport, Long> tuple, Collector<Tuple3<Long, CountTuple, Long>> collector) throws Exception {
        LOG.debug("CountVehicles");

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f2;


        PositionReport inputPositionReport = tuple.f1;

        short minute = inputPositionReport.getMinuteNumber();
        segment.xway = inputPositionReport.xway;
        segment.segment = inputPositionReport.segment;
        segment.direction = inputPositionReport.direction;

        boolean emitted = false;

        for (Map.Entry<SegmentIdentifier, CarCount> entry : this.countsMap.entrySet()) {
            SegmentIdentifier segId = entry.getKey();


            int count = entry.getValue().count;
            if (count > 50) {

                emitted = true;

                CountTuple countTuple = new CountTuple();
                countTuple.minuteNumber = currentMinute;
                countTuple.xway = segId.xway;
                countTuple.segment = segId.segment;
                countTuple.direction = segId.direction;
                countTuple.count = count;
                collector.collect(new Tuple3<>(timestamp, countTuple, timestamp_ext));
            }
        }
        if (!emitted) {
            CountTuple countTuple = new CountTuple();
            countTuple.minuteNumber = currentMinute;
            collector.collect(new Tuple3<>(timestamp, countTuple, timestamp_ext));
        }
        this.countsMap.clear();
        this.currentMinute = minute;

        CarCount segCnt = this.countsMap.get(this.segment);
        if (segCnt == null) {
            segCnt = new CarCount();
            this.countsMap.put((SegmentIdentifier) this.segment.clone(), segCnt);
        } else {
            ++segCnt.count;
        }
    }
}
