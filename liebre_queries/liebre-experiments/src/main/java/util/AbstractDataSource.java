package util;

import component.source.SourceFunction;
import common.metrics.Metric;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import net.openhft.affinity.Affinity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import query.LiebreContext;
import synthetic.CloneableSourceFunction;

public abstract class AbstractDataSource<T> implements DataSource<T> {

  private static final Logger LOG = LogManager.getLogger();
  public static final int INITIAL_SLEEP_MILLIS = 15000;
  protected final SourceFunction<T> function;
  protected final List<Metric> metrics = new ArrayList<>();
  protected final String id;
  protected final BitSet affinity;

  public AbstractDataSource(
      SourceFunction<T> function, String id, BitSet affinity) {
    this.function = function;
    this.id = id;
    this.affinity = affinity;
    metrics.add(LiebreContext.operatorMetrics().newCountPerSecondMetric(id, "sourceRate"));
  }

  @Override
  public abstract Queue<T> initQueue(int index);

  @Override
  public void run() {
    Affinity.setAffinity(affinity);
    for (Metric metric : metrics) {
      metric.enable();
    }
    // Wait until SPE has started
    try {
      Thread.sleep(INITIAL_SLEEP_MILLIS);
    } catch (InterruptedException e) {
      return;
    }
    if (function instanceof CloneableSourceFunction) {
      ((CloneableSourceFunction) function).setParent(this);
    }
    function.enable();
    LOG.info("Started writing data (affinity = {})", Affinity.getAffinity());
    while (!Thread.currentThread().isInterrupted()) {
      T tuple = function.get();
      if (tuple == null) {
        continue;
      }
      emit(tuple);
    }
    function.disable();
    for (Metric metric : metrics) {
      metric.disable();
    }
    LOG.info("Stopped writing data");
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public final String streamName(int index) {
    return String.format("%s-%d-DATASOURCE_%s-%d-%s", getId(), index, getId(), index, RemoteSource.NAME);
  }

  @Override
  public int getIndex() {
    return -1;
  }
}
