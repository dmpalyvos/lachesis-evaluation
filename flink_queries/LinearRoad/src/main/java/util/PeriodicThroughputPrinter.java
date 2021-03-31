package util;

import LinearRoad.LinearRoad;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class PeriodicThroughputPrinter implements Serializable {

  private static final Logger LOG = Log.get(LinearRoad.class);
  private long calls;
  private long startTs = -1;
  private long previousPrintTs;
  private final int taskIndex;
  private final long printPeriodMs;

  public PeriodicThroughputPrinter(int taskIndex, long printPeriodMs) {
    this.taskIndex = taskIndex;
    this.printPeriodMs = printPeriodMs;
  }

  public PeriodicThroughputPrinter(int taskIndex) {
    this(taskIndex, TimeUnit.MINUTES.toMillis(1));
  }

  public void run() {
    checkInit();
    calls++;
    if (timeToPrint()) {
      LOG.info("[{}] Throughput = {}", taskIndex, averageThroughput());
      previousPrintTs = System.currentTimeMillis();
    }
  }

  private int averageThroughput() {
    long interval = secondsElapsedSinceStart();
    return interval > 0 ?
        Math.round(calls / interval) : 0;
  }

  private void checkInit() {
    if (startTs < 0) {
      startTs = System.currentTimeMillis();
    }
  }

  private long secondsElapsedSinceStart() {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTs);
  }

  private boolean timeToPrint() {
    return System.currentTimeMillis() - previousPrintTs > printPeriodMs;
  }

}
