package synthetic;

import common.util.Util;

public class SyntheticStressOperatorBlocking<T extends SyntheticTuple> extends
    SyntheticStressOperator<T> {

  private final int blockTimeMs;
  private final double blockProbability;

  public SyntheticStressOperatorBlocking(int cost, double operatorSelectivity, double blockProbability, int blockTimeMs) {
    super(cost, operatorSelectivity);
    this.blockProbability = blockProbability;
    this.blockTimeMs = blockTimeMs;
  }

  @Override
  public boolean test(T t) {
    if (r.nextDouble() <= blockProbability) {
      Util.sleep(r.nextInt(blockTimeMs));
    }
    return super.test(t);
  }
}
