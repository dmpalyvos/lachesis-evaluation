package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.CountStat;
import util.Log;
import util.Stats;

public class Parser extends RichMapFunction<
        Tuple3<Long, String, Long>,
        Tuple6<Long, String, String, Long, CallDetailRecord, Long>> {

    private static final Logger LOG = Log.get(Parser.class);
    private transient CountStat throughputStat;
    private final String statisticsFolder;

    public Parser(String statisticsFolder) {
        this.statisticsFolder = statisticsFolder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.throughputStat = new CountStat(
            Stats.statisticsFile(statisticsFolder, getRuntimeContext(), Stats.SOURCE_THROUGHPUT_FILE));    }

    @Override
    public Tuple6<Long, String, String, Long, CallDetailRecord, Long> map(Tuple3<Long, String, Long> tuple) {
        LOG.debug("tuple in: {}", tuple);
        throughputStat.increase(1);
        // fetch values from the tuple
        long timestamp = tuple.f0;
        long timestamp_ext = tuple.f2;
        String line = tuple.f1;

        // parse the line
        CallDetailRecord cdr = new CallDetailRecord(line);

        Tuple6<Long, String, String, Long, CallDetailRecord, Long> out = new Tuple6<>(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, cdr, timestamp_ext);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
