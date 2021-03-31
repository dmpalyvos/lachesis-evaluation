package VoipStream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import org.apache.flink.api.java.tuple.Tuple2;
import util.AverageGauge;

// class SourceTimerTask
public class SourceTimerTask extends TimerTask {

	private static final double ONE_SECOND_NANOS = 1e9;
	private LineReaderTimerSource source; // source connected to this SourceTimerTask
	private String path; // pathname of the dataset file to be read
	private List<String> data; // dataset in memory
	private long batchSize; // number of inputs to be generated per activation of the task
    private int index;
    private int offset;
    private int stride;
    private transient AverageGauge externalRateGauge;
    private long generated;
    private long lastSample;

	// Constructor
	SourceTimerTask(LineReaderTimerSource _source, String _path, long _batchSize, int _index, int _offset, int _stride) {
		this.source = _source;
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
        externalRateGauge = _source.getRuntimeContext().getMetricGroup().gauge("external-rate", new AverageGauge());
	}

	@Override
	public void run() {
		if (generated == 0) {
			lastSample = System.nanoTime();
		}
		long elapsed = System.nanoTime() - lastSample;
		if (elapsed >= ONE_SECOND_NANOS) {
			double rate = generated / ((elapsed) / ONE_SECOND_NANOS); // per second
			externalRateGauge.add((int) rate);
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
	        long timestamp = System.currentTimeMillis();
	        Tuple2<Long, String> event = new Tuple2<Long, String>(timestamp, line);
	        source.addTuple(event); // add the tuple to the queue used by the spout
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
