package queries.synthetic;

import synthetic.SyntheticTuple;
import util.DataSource;

public class SyntheticQueryFactoryNonRandom<T extends SyntheticTuple> extends SyntheticQueryFactory<T> {


  public SyntheticQueryFactoryNonRandom(SyntheticQuerySettings settings, String name,
      DataSource<T> dataSource, int priority) {
    super(settings, name, dataSource, priority);
  }

  @Override
  protected double selectivity() {
    return minSelectivity();
  }

  @Override
  protected int cost() {
    return settings.cost();
  }
  
}
