package synthetic;

import common.tuple.RichTuple;
import java.util.function.Supplier;

public class BasicConstantRateSource<T extends RichTuple> extends ConstantRateSource<T>{

  private final int rate;

  public BasicConstantRateSource(Supplier<T> supplier, final int rate) {
    super(supplier);
    this.rate = rate;
  }

  @Override
  protected long rate() {
    return rate;
  }

  @Override
  public CloneableSourceFunction<T> copy() {
    return new BasicConstantRateSource<>(supplier, rate);
  }
}
