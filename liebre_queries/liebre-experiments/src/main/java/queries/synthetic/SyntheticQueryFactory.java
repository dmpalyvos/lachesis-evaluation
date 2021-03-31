/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package queries.synthetic;

import static stream.AbstractStream.METRIC_ARRIVAL_TIME;

import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import io.palyvos.haren.FeatureHelper;
import io.palyvos.haren.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.jetbrains.annotations.NotNull;
import query.LiebreContext;
import query.Query;
import synthetic.SyntheticStressOperator;
import synthetic.SyntheticStressSink;
import synthetic.SyntheticTuple;
import util.DataSource;
import util.DataSourceFactory;
import util.LatencyLoggingSink;

public class SyntheticQueryFactory<T extends SyntheticTuple>
    extends DataSourceFactory<T, SyntheticQuerySettings> {

  protected final Random random;
  protected final int priority;

  public SyntheticQueryFactory(
      SyntheticQuerySettings settings, String name, DataSource<T> dataSource, int priority) {
    super(name, settings, dataSource);
    this.priority = priority;
    this.random = new Random(settings.randomSeed());
  }

  @Override
  protected void addQueryInstance(Query q) {
    final List<Task> addedTasks = new ArrayList<>();
    Source<T> source = q.addSource(newSource());
    source.setPriority(priority);
    List<Operator<T, T>> operators = new ArrayList<>();
    final int queryCost = cost();
    final double querySelectivity = selectivity();
    LOG.info("{} {} cost = {}, selectivity = {}", queryName, queryIndex, queryCost, querySelectivity);
    for (int i = 0; i < settings.chainLength(); i++) {
      operators.add(
          q.addFilterOperator(
              name(String.format("OP-%d", i)),
              newOperator(queryCost, querySelectivity)));
    }
    Sink<T> sink =
        q.addSink(
            new LatencyLoggingSink<>(
                name("SINK-0"),
                new SyntheticStressSink<>(operatorCost(queryCost)),
                settings.statisticsFolder(),
                settings.autoFlush()));
    for (int i = 0; i <= operators.size(); i++) {
      StreamProducer<T> producer = i > 0 ? operators.get(i - 1) : source;
      StreamConsumer<T> consumer = i < operators.size() ? operators.get(i) : sink;
      q.connect(producer, consumer);
    }
    addedTasks.add(source);
    addedTasks.add(sink);
    addedTasks.addAll(operators);
    addedTasks.forEach(task -> LiebreContext
        .streamMetrics()
        .newGaugeMetric(((Component) task).getId(), METRIC_ARRIVAL_TIME, () -> {
          double arrivalTime = ((Component) task).getHeadArrivalTime();
          return ((arrivalTime > 0) && (arrivalTime < FeatureHelper.NO_ARRIVAL_TIME)) ? (
              System.currentTimeMillis() - Math.round(arrivalTime)) : 0;
        }));
    onTasksAdded(addedTasks);
  }

  protected SyntheticStressOperator<T> newOperator(int cost, double selectivity) {
    return new SyntheticStressOperator<>(
        operatorCost(cost),
        operatorSelectivity(selectivity));
  }

  protected int operatorCost(int queryCost) {
    return withVariance(queryCost, settings.operatorVariance());
  }

  protected double operatorSelectivity(double querySelectivity) {
    return withVariance(querySelectivity, settings.operatorVariance());
  }

  protected void onTasksAdded(List<Task> addedTasks) {
  }

  protected int cost() {
    return Math.toIntExact(
        settings.cost() * Math.round(Math.pow(2, nextInt(0, settings.varianceFactor()))));
  }

  protected double selectivity() {
    return nextDouble(minSelectivity(), 1);
  }

  protected int withVariance(int baseValue, int variancePercent) {
    int variance = (baseValue * variancePercent) / 100;
    return baseValue + nextInt(-variance, variance);
  }

  protected double withVariance(double baseValue, int variancePercent) {
    double variance = (baseValue * variancePercent) / 100;
    return baseValue + nextDouble(-variance, variance);
  }

  protected double minSelectivity() {
    return Math.exp(Math.log(settings.selectivity()) / settings.chainLength());
  }

  protected final int nextInt(int from, int to) {
    return from + random.nextInt(to - from + 1);
  }

  protected final double nextDouble(double from, double to) {
    return from + random.nextDouble() * (to - from);
  }
}
