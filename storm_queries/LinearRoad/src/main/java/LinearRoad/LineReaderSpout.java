package LinearRoad;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineReaderSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(LineReaderSpout.class);

    private SpoutOutputCollector spoutOutputCollector;

    private String path;
    private long gen_rate;
    private List<String> data;

    private boolean waitForShutdown;
    private int index;
    private int offset;
    private int stride;
    private long counter;
    private Sampler throughput;
    private long epoch, lastTupleTs;

    public LineReaderSpout(long _gen_rate, String path) {
        this.gen_rate = _gen_rate;
        this.path = path;
        waitForShutdown = false;
        epoch = 0;
        lastTupleTs = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "timestamp_ext", "line"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // initialize
        this.spoutOutputCollector = spoutOutputCollector;
        data = new ArrayList<>();
        offset = topologyContext.getThisTaskIndex();
        stride = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        index = offset;
        counter = 0;
        throughput = new Sampler();

        // read the whole dataset
        try {
            readAll();
        } catch (IOException e) {
            spoutOutputCollector.reportError(e);
        }
    }

    @Override
    public void nextTuple() {
        long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
        active_delay(delay_nsec);

        // fetch the next item
        if (index >= data.size()) {
            index = offset;
        }
        String line = data.get(index);

        // send the tuple
        long timestamp = System.currentTimeMillis();
        Values out = new Values(timestamp, timestamp, line);
        spoutOutputCollector.emit(out);
        LOG.debug("tuple out: {}", out);
        lastTupleTs = timestamp;
        index += stride;
        counter++;
        // update the epoch the first time
        if (counter == 1) {
            epoch = System.nanoTime();
        }
    }

    @Override
    public void close() {

        double rate = counter / ((lastTupleTs - epoch) / 1e9); // per second
        throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    private void readAll() throws IOException {
        // read the whole file line by line
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                data.add(line);
            }
        }
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }
}
