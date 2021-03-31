package LinearRoad;

import java.util.Map;
import java.util.List;
import java.io.FileReader;
import java.util.TimerTask;
import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import org.apache.storm.Config;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;

// class SourceTimerTask
public class SourceTimerTask extends TimerTask {

	private static final double ONE_SECOND_NANOS = 1e9;
	private LineReaderTimerSpout spout; // spout connected to this SourceTimerTask
	private String path; // pathname of the dataset file to be read
	private List<String> data; // dataset in memory
	private long batchSize; // number of inputs to be generated per activation of the task
    private int index;
    private int offset;
    private int stride;
    private transient ReducedMetric reducedMetric;
    private TopologyContext topologyContext;
    private Map map;
    private long generated;
    private long lastSample;

	// Constructor
	SourceTimerTask(LineReaderTimerSpout _spout, String _path, long _batchSize, int _index, int _offset, int _stride, TopologyContext _topologyContext, Map _map) {
		this.spout = _spout;
		this.path = _path;
		this.data = new ArrayList<>();
		this.batchSize = _batchSize;
		this.index = _index;
		this.offset = _offset;
		this.stride = _stride;
		this.generated = 0;
        // read the whole dataset
        try {
            readAll();
        }
        catch (IOException e) {}
        reducedMetric = new ReducedMetric(new MeanReducer());
        topologyContext = _topologyContext;
        map = _map;
        Long builtinPeriod = (Long) map.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS);
        topologyContext.registerMetric("external-rate", reducedMetric, builtinPeriod.intValue());
	}

	@Override
	public void run() {
		if (generated == 0) {
			lastSample = System.nanoTime();
		}
		long elapsed = System.nanoTime() - lastSample;
		if (elapsed >= ONE_SECOND_NANOS) {
			double rate = generated / ((elapsed) / ONE_SECOND_NANOS); // per second
			reducedMetric.update((int) rate);
			generated = 0;
			lastSample = System.nanoTime();
		}
		// Generate the batch
		for (int i=0; i<batchSize; i++) {
	        // fetch the next item
	        if (index >= data.size()) {
	            index = offset;
	        }
	        String line = data.get(index); // get the next tuple
	        long timestamp = System.currentTimeMillis(); // put the timestamp
	        Values event = new Values(timestamp, line);
	        spout.addTuple(event); // add the tuple to the queue used by the spout
	        generated++;
	        index += stride;
		}
	}

	// Read all the dataset in memory
    private void readAll() throws IOException {
        // read the whole file line by line
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                data.add(line);
            }
        }
    }
}
