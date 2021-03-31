package synthetic;

import common.tuple.RichTuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;
import util.DataSource;

/**
 * Hack-ish implementation of a constant-rate {@link synthetic.CloneableSourceFunction} that relies
 * on a {@link java.util.TimerTask} for higher resilience
 */
public class TimerBasedConstantRateSource<T extends RichTuple> extends TimerTask implements
    CloneableSourceFunction<T> {

  private static final int DEFAULT_PERIOD = 100;
  private final int batchSize;
  private final int period;
  private final int rate;
  private final Supplier<T> supplier;
  private final Timer timer = new Timer();
  private DataSource<T> dataSource;

  public TimerBasedConstantRateSource(Supplier<T> supplier, int rate) {
    this(supplier, rate, DEFAULT_PERIOD);
  }

  public TimerBasedConstantRateSource(Supplier<T> supplier, int rate, int period) {
    this.rate = rate;
    this.period = period;
    this.batchSize = rate / (1000 / period);
    this.supplier = supplier;
  }

  @Override
  public void enable() {
    timer.scheduleAtFixedRate(this, 0, this.period);
  }

  @Override
  public void setParent(DataSource<T> datasource) {
    this.dataSource = datasource;
  }

  @Override
  public void disable() {
    timer.cancel();
  }

  @Override
  public void run() {
    for (int i = 0; i < batchSize; i++) {
      dataSource.emit(supplier.get());
    }
  }

  @Override
  public CloneableSourceFunction<T> copy() {
    return new TimerBasedConstantRateSource<T>(supplier, rate, period);
  }

  @Override
  public T get() {
    return null;
  }
}
