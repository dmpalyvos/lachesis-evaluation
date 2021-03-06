package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.Constants;
import common.PositionReport;
import common.SegmentIdentifier;
import common.TollNotification;
import util.Log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TollNotificationPos extends RichFlatMapFunction<Tuple3<Long, PositionReport, Long>, Tuple3<Long, TollNotification, Long>> {
    private static final Logger LOG = Log.get(TollNotificationPos.class);

    //////////
    /**
     * Contains all vehicle IDs and segment of the last {@link PositionReport} to allow skipping already sent
     * notifications (there's only one notification per segment per vehicle).
     */
    private final Map<Integer, Short> allCars = new HashMap<>();
    /**
     * Contains the last toll notification for each vehicle to assess the toll when the vehicle leaves a segment.
     */
    private final Map<Integer, TollNotification> lastTollNotification = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentToCheck = new SegmentIdentifier();
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
    public void flatMap(Tuple3<Long, PositionReport, Long> tuple, Collector<Tuple3<Long, TollNotification, Long>> collector) throws Exception {
        LOG.debug("TollNotificationPos");

        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f2;

        collector.collect(new Tuple3<>(timestamp, new TollNotification(), timestamp_ext));//as an indication.

        PositionReport inputPositionReport = tuple.f1;

        this.checkMinute(inputPositionReport.getMinuteNumber());

        if (inputPositionReport.isOnExitLane()) {
            final TollNotification lastNotification = this.lastTollNotification.remove(inputPositionReport.vid);

            assert lastNotification == null || (lastNotification.pos.xway != null);
            return;
        }

        final Short currentSegment = inputPositionReport.segment;
        final Integer vid = inputPositionReport.vid;
        final Short previousSegment = this.allCars.put(vid, currentSegment);
        if (previousSegment != null && currentSegment.shortValue() == previousSegment.shortValue()) {
            return;
        }

        int toll = 0;
        SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
        segmentIdentifier.xway = inputPositionReport.xway;
        segmentIdentifier.segment = inputPositionReport.segment;
        segmentIdentifier.direction = inputPositionReport.direction;
        Integer lav = this.previousMinuteLavs.get(segmentIdentifier);

        final int lavValue;
        if (lav != null) {
            lavValue = lav;
        } else {
            lav = 0;
            lavValue = 0;
        }

        if (lavValue < 50) {
            SegmentIdentifier segmentIdentifier2 = new SegmentIdentifier();
            segmentIdentifier2.xway = inputPositionReport.xway;
            segmentIdentifier2.segment = inputPositionReport.segment;
            segmentIdentifier2.direction = inputPositionReport.direction;
            final Integer count = this.previousMinuteCounts.get(segmentIdentifier2);
            int carCount = 0;
            if (count != null) {
                carCount = count;
            }

            if (carCount > 50) {
                // downstream is either larger or smaller of current segment
                final Short direction = inputPositionReport.direction;
                final short dir = direction;
                // EASTBOUND == 0 => diff := 1
                // WESTBOUNT == 1 => diff := -1
                final short diff = (short) -(dir - 1 + ((dir + 1) / 2));
                assert (dir == Constants.EASTBOUND ? diff == 1 : diff == -1);

                final Integer xway = inputPositionReport.xway;
                final short curSeg = currentSegment;

                this.segmentToCheck.xway = xway;
                this.segmentToCheck.direction = direction;

                int i;
                for (i = 0; i <= 4; ++i) {
                    final short nextSegment = (short) (curSeg + (diff * i));
                    assert (dir == Constants.EASTBOUND ? nextSegment >= curSeg : nextSegment <= curSeg);

                    this.segmentToCheck.segment = nextSegment;

                    if (this.previousMinuteAccidents.contains(this.segmentToCheck)) {
                        break;
                    }
                }

                if (i == 5) { // only true if no accident was found and "break" was not executed
                    final int var = carCount - 50;
                    toll = 2 * var * var;
                }
            }
        }
        // TODO get accurate emit time...
        final TollNotification tollNotification = new TollNotification();
        tollNotification.vid = vid;
        tollNotification.speed = lav;
        tollNotification.toll = toll;
        tollNotification.pos = (PositionReport) inputPositionReport.clone();

        final TollNotification lastNotification;
        if (toll != 0) {
            lastNotification = this.lastTollNotification.put(vid, tollNotification);
        } else {
            lastNotification = this.lastTollNotification.remove(vid);
        }
        assert lastNotification == null || (lastNotification.pos.xway != null);
        assert (tollNotification.pos.xway != null);
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
