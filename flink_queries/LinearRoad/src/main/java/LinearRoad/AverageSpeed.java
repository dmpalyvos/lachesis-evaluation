package LinearRoad;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.AvgValue;
import common.AvgVehicleSpeedTuple;
import common.PositionReport;
import common.SegmentIdentifier;
import util.Log;

import java.util.HashMap;
import java.util.Map;

class AverageSpeed extends RichFlatMapFunction<Tuple3<Long, PositionReport, Long>, Tuple3<Long, AvgVehicleSpeedTuple, Long>> {
    private static final Logger LOG = Log.get(AverageSpeed.class);

    //////////
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified
     * segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;
    //////////

    @Override
    public void flatMap(Tuple3<Long, PositionReport, Long> tuple, Collector<Tuple3<Long, AvgVehicleSpeedTuple, Long>> collector) throws Exception {
        LOG.debug("AverageSpeed");

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f2;

        PositionReport inputPositionReport = tuple.f1;

        Integer vid = inputPositionReport.vid;
        short minute = inputPositionReport.getMinuteNumber();
        int speed = inputPositionReport.speed;
        this.segment.xway = inputPositionReport.xway;
        this.segment.segment = inputPositionReport.segment;
        this.segment.direction = inputPositionReport.direction;

        for (Map.Entry<Integer, Pair<AvgValue, SegmentIdentifier>> entry : this.avgSpeedsMap.entrySet()) {
            Pair<AvgValue, SegmentIdentifier> value = entry.getValue();
            SegmentIdentifier segId = value.getRight();

            AvgVehicleSpeedTuple avgVehicleSpeedTuple = new AvgVehicleSpeedTuple();
            avgVehicleSpeedTuple.vid = entry.getKey();
            avgVehicleSpeedTuple.minute = currentMinute;
            avgVehicleSpeedTuple.xway = segId.xway;
            avgVehicleSpeedTuple.segment = segId.segment;
            avgVehicleSpeedTuple.direction = segId.direction;
            avgVehicleSpeedTuple.avgSpeed = value.getLeft().getAverage();
            collector.collect(new Tuple3<>(timestamp, avgVehicleSpeedTuple, timestamp_ext));
        }

        this.avgSpeedsMap.clear();
        this.currentMinute = minute;

        Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
        if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {
            SegmentIdentifier segId = vehicleEntry.getRight();

            AvgVehicleSpeedTuple avgVehicleSpeedTuple = new AvgVehicleSpeedTuple();
            avgVehicleSpeedTuple.vid = vid;
            avgVehicleSpeedTuple.minute = currentMinute;
            avgVehicleSpeedTuple.xway = segId.xway;
            avgVehicleSpeedTuple.segment = segId.segment;
            avgVehicleSpeedTuple.direction = segId.direction;
            avgVehicleSpeedTuple.avgSpeed = vehicleEntry.getLeft().getAverage();
            collector.collect(new Tuple3<>(timestamp, avgVehicleSpeedTuple, timestamp_ext));

            vehicleEntry = null;
        }

        if (vehicleEntry == null) {
            vehicleEntry = new MutablePair<AvgValue, SegmentIdentifier>(new AvgValue(speed), (SegmentIdentifier) this.segment.clone());
            this.avgSpeedsMap.put(vid, vehicleEntry);
        } else {
            vehicleEntry.getLeft().updateAverage(speed);
        }
    }
}
