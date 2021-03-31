package queries.synthetic;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import synthetic.SyntheticTuple;
import util.DataSource;

public class ImprovedSyntheticQueryFactory<T extends SyntheticTuple> extends
    SyntheticQueryFactory<T> {

  private ExponentialDistribution exponentialDistribution;

  public ImprovedSyntheticQueryFactory(SyntheticQuerySettings settings, String name,
      DataSource<T> dataSource, int priority) {
    super(settings, name, dataSource, priority);
    exponentialDistribution = new ExponentialDistribution(
        new JDKRandomGenerator(Math.toIntExact(settings.randomSeed())), 1);
  }

  @Override
  protected int cost() {
    // Choose exponent up to maxCostFactor using exponential sampling
    int exponent = Math.toIntExact(Math.round(exponentialDistribution.sample()));
    exponent = Math.min(exponent, settings.varianceFactor());
    int costFactor = Math.toIntExact(Math.round(Math.pow(2, exponent)));
    return settings.cost() * costFactor;
  }

}