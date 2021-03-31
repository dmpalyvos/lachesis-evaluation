package synthetic;

import common.tuple.RichTuple;
import java.util.function.Supplier;
import util.LiveSettings;

public class LiveConstantRateSource<T extends RichTuple> extends ConstantRateSource<T> {

  private final LiveSettings settings;

  public LiveConstantRateSource(Supplier<T> supplier, LiveSettings settings) {
    super(supplier);
    this.settings = settings;
  }

  @Override
  protected long rate() {
    return settings.rate;
  }

  @Override
  public CloneableSourceFunction<T> copy() {
    return new LiveConstantRateSource<>(supplier, settings);
  }
}
