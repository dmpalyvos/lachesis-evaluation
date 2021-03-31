package synthetic;

import common.tuple.RichTuple;
import java.util.function.Supplier;
import util.LiveSettings;

public class LiveBurstyRateSource<T extends RichTuple> extends BurstyRateSource<T> {

  private final LiveSettings settings;

  public LiveBurstyRateSource(Supplier<T> supplier, LiveSettings settings) {
    super(supplier);
    this.settings = settings;
  }

  @Override
  protected long baseRate() {
    return settings.baseRate;
  }

  @Override
  protected long burstRate() {
    return settings.burstRate;
  }

  @Override
  protected long burstDurationMillis() {
    return settings.burstDurationMillis;
  }

  @Override
  protected long burstPeriodMillis() {
    return settings.burstPeriodMillis;
  }

  @Override
  public CloneableSourceFunction<T> copy() {
    return new LiveBurstyRateSource<>(supplier, settings);
  }
}
