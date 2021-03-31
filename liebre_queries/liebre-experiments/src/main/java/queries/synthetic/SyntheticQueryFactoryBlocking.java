package queries.synthetic;

import synthetic.SyntheticStressOperator;
import synthetic.SyntheticStressOperatorBlocking;
import synthetic.SyntheticTuple;
import util.DataSource;

public class SyntheticQueryFactoryBlocking<T extends SyntheticTuple> extends
    SyntheticQueryFactory<T> {

  public static final int BLOCK_TIME_MS = 200;
  public static final double BLOCK_PROBABILITY = 0.001;
  private static final double BLOCKING_OPERATOR_FREQUENCY = 0.1;

  public SyntheticQueryFactoryBlocking(SyntheticQuerySettings settings, String name,
      DataSource<T> dataSource, int priority) {
    super(settings, name, dataSource, priority);
  }

  @Override
  protected SyntheticStressOperator<T> newOperator(int cost, double selectivity) {
    if (random.nextDouble() < BLOCKING_OPERATOR_FREQUENCY) {
      return new SyntheticStressOperatorBlocking<>(cost, selectivity, BLOCK_PROBABILITY,
          BLOCK_TIME_MS);
    }
    return super.newOperator(cost, selectivity);
  }
}
