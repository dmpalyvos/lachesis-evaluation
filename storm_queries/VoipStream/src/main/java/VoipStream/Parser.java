package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.AvgStat;
import util.CountStat;
import util.Log;

import java.util.Map;
import util.Stats;

class Parser extends BaseRichBolt {
    private static final Logger LOG = Log.get(Parser.class);

    private OutputCollector outputCollector;
    private transient CountStat throughputStat;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.throughputStat = new CountStat(Stats.statisticsFile(map, topologyContext, Stats.SOURCE_THROUGHPUT_FILE));
        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "timestamp_ext", "calling_number", "called_number", "answer_timestamp", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);
        throughputStat.increase(1);

        // fetch values from the tuple
        long timestamp = (long) tuple.getValueByField("timestamp");
        long timestamp_ext = (long) tuple.getValueByField("timestamp_ext");
        String line = (String) tuple.getValueByField("line");

        // parse the line
        CallDetailRecord cdr = new CallDetailRecord(line);

        Values out = new Values(timestamp, timestamp_ext, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, cdr);
        outputCollector.emit(out);
        LOG.debug("tuple out: {}", out);

        //outputCollector.ack(tuple);
    }
}
