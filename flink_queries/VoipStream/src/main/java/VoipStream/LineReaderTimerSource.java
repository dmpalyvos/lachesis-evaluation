package VoipStream;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import util.Log;

// class LineReaderTimerSource
public class LineReaderTimerSource extends RichParallelSourceFunction<Tuple3<Long, String, Long>> {
    private static final Logger LOG = Log.get(LineReaderTimerSource.class);
    private long counter;
    private long gen_rate;
    private String path;
    private long runTimeSec;
    private long epoch;
	private Timer sourceTimer;
	private BlockingQueue<Tuple2<Long, String>> inputQueue;
	private final int period = 100; // timer triggers every period milliseconds
	private transient Gauge<Integer> queueSizeGauge;
  private volatile boolean enabled = true;

	// Constructor
    public LineReaderTimerSource(long runTimeSec, long _gen_rate, String _path) {
    	this.counter = 0;
        this.gen_rate = _gen_rate;
        this.path = _path;
		this.runTimeSec = runTimeSec;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
    	int offset = getRuntimeContext().getIndexOfThisSubtask();
    	int stride = getRuntimeContext().getNumberOfParallelSubtasks();
    	int index = offset;
    	long batchSize = this.gen_rate / (1000 / period);
    	this.inputQueue = new LinkedBlockingQueue<Tuple2<Long, String>>();
        // generate the timer
        sourceTimer = new Timer("SourceTimer", true);
        SourceTimerTask sourceTimerTask = new SourceTimerTask(this, path, batchSize, index, offset, stride);
        sourceTimer.scheduleAtFixedRate(sourceTimerTask, 0, period);
        queueSizeGauge = getRuntimeContext().getMetricGroup().gauge("external-queue-size", () -> inputQueue.size());
    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Long>> sourceContext) throws InterruptedException {
        long epoch = System.currentTimeMillis();
        long timestamp = epoch;
        while (timestamp - epoch < TimeUnit.SECONDS.toMillis(runTimeSec)) {
	    	Tuple2<Long, String> event = null;
	    	do {
          		if (!enabled) {
            		return;
          		}
          		event = (this.inputQueue).poll(); // nextTuple should not block!
	    	}
	    	while(event == null);
	    	Tuple3<Long, String, Long> out = new Tuple3<>(System.currentTimeMillis(), event.getField(1), event.getField(0));
	    	sourceContext.collect(out);
	    	counter++;
	    	timestamp = System.currentTimeMillis();
        }
    }

    @Override
    public void cancel() {
      this.enabled = false;
    }

    @Override
    public void close() {
      this.enabled = false;
    	sourceTimer.cancel(); // cancel the timer
    }

    // add a tuple to the internal queue
    public void addTuple(Tuple2<Long, String> event) {
		try {
			(this.inputQueue).put(event);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}
