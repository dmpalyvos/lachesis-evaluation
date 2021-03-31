package LinearRoad;

import util.Log;
import java.util.Map;
import java.util.Timer;
import org.slf4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;
import java.util.concurrent.BlockingQueue;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.metric.api.MeanReducer;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

// class LineReaderTimerSpout
public class LineReaderTimerSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(LineReaderTimerSpout.class);
    private long counter;
    private long gen_rate;
    private String path;
    private long epoch, lastTime;
	private Timer sourceTimer;
	private BlockingQueue<Values> inputQueue;
	private SpoutOutputCollector spoutOutputCollector;
	private final int period = 100; // timer triggers every period milliseconds

	// Constructor
    public LineReaderTimerSpout(long _gen_rate, String _path) {
        this.counter = 0;
        this.gen_rate = _gen_rate;
        this.path = _path;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "timestamp_ext", "line"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    	int offset = topologyContext.getThisTaskIndex();
    	int stride = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
    	int index = offset;
    	long batchSize = this.gen_rate / (1000 / period);
    	this.inputQueue = new LinkedBlockingQueue<Values>();
        this.spoutOutputCollector = spoutOutputCollector;
        // generate the timer
        sourceTimer = new Timer("SourceTimer", true);
        SourceTimerTask sourceTimerTask = new SourceTimerTask(this, path, batchSize, index, offset, stride, topologyContext, map);
        sourceTimer.scheduleAtFixedRate(sourceTimerTask, 0, period);
        Long builtinPeriod = (Long) map.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS);
        topologyContext.registerMetric("external-queue-size", () -> inputQueue.size(), builtinPeriod.intValue());
    }

    @Override
    public void nextTuple() {
    	Values event = (this.inputQueue).poll(); // nextTuple should not block!
    	if (event == null) {
    		return; // this happens if the inputQueue is empty
    	}
    	event.add(0, System.currentTimeMillis());
        // send the tuple
        spoutOutputCollector.emit(event);
        counter++;
        LOG.debug("tuple out: {}", event);
        // get the current time
        lastTime = System.nanoTime();
        // get the epoch time
        if (counter == 1) {
            epoch = System.nanoTime();
        }
    }

    @Override
    public void close() {
    	sourceTimer.cancel(); // cancel the timer
        double rate = counter / ((lastTime - epoch) / 1e9); // per second
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    // add a tuple to the internal queue
    public void addTuple(Values event) {
		try {
			(this.inputQueue).put(event);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}
