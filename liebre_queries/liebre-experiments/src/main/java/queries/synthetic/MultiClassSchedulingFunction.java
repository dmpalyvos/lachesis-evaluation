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

import static queries.synthetic.queries.MultiClass.HIGH_PRIORITY;
import static queries.synthetic.queries.MultiClass.LOW_PRIORITY;

import io.palyvos.haren.Feature;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import scheduling.IntraThreadSchedulingFunctions;

public class MultiClassSchedulingFunction implements VectorIntraThreadSchedulingFunction {

  private static final int N_DIMENSIONS = 3;
  private static final int USER_PRIORITY_INDEX = 0;
  private static final int HIGH_PRIO_INDEX = 1;
  private static final int LOW_PRIO_INDEX = 2;

  private final SingleIntraThreadSchedulingFunction USER_PRIORITY_FUNCTION =
      IntraThreadSchedulingFunctions.userPriority();
  private final SingleIntraThreadSchedulingFunction lowPriorityFunction;
  private final SingleIntraThreadSchedulingFunction highPriorityFunction;
  private final boolean[] isReverseOrder;
  private final Feature[] requiredFeatures;
  private boolean caching;
  private long[] lastSlowUpdate;
  private long slowUpdatePeriod = 250;

  public MultiClassSchedulingFunction(
      SingleIntraThreadSchedulingFunction highPriorityFunction,
      SingleIntraThreadSchedulingFunction lowPriorityFunction) {
    this.lowPriorityFunction = lowPriorityFunction;
    this.highPriorityFunction = highPriorityFunction;
    Set<Feature> functionFeatures = new HashSet<>();
    functionFeatures.addAll(Arrays.asList(USER_PRIORITY_FUNCTION.requiredFeatures()));
    functionFeatures.addAll(Arrays.asList(lowPriorityFunction.requiredFeatures()));
    functionFeatures.addAll(Arrays.asList(highPriorityFunction.requiredFeatures()));
    this.requiredFeatures = functionFeatures.toArray(new Feature[0]);
    this.isReverseOrder = new boolean[3];
    this.isReverseOrder[USER_PRIORITY_INDEX] = USER_PRIORITY_FUNCTION.isReverseOrder();
    this.isReverseOrder[LOW_PRIO_INDEX] = lowPriorityFunction.isReverseOrder();
    this.isReverseOrder[HIGH_PRIO_INDEX] = highPriorityFunction.isReverseOrder();
  }

  public MultiClassSchedulingFunction() {
    this(
        IntraThreadSchedulingFunctions.averageArrivalTime(),
        IntraThreadSchedulingFunctions.globalRate());
  }

  @Override
  public void apply(Task task, TaskIndexer indexer, double[][] features, double[] output) {
    final double priority = USER_PRIORITY_FUNCTION.apply(task, indexer, features);
    output[USER_PRIORITY_INDEX] = priority;
    if (priority == HIGH_PRIORITY) {
      applyHighPriority(task, features, output, indexer);
    } else if (priority == LOW_PRIORITY) {
      applyLowPriority(task, features, output, indexer);
    } else {
      throw new IllegalStateException("Unknown Priority!");
    }
  }

  private void applyHighPriority(
      Task task, double[][] features, double[] output, TaskIndexer indexer) {
    output[LOW_PRIO_INDEX] = 0;
    output[HIGH_PRIO_INDEX] = highPriorityFunction.apply(task, indexer, features);
  }

  private void applyLowPriority(
      Task task, double[][] features, double[] output, TaskIndexer indexer) {
    if (timeForSlowUpdate(task, indexer)) {
      output[LOW_PRIO_INDEX] = lowPriorityFunction.apply(task, indexer, features);
    }
    output[HIGH_PRIO_INDEX] = 0;
  }

  private boolean timeForSlowUpdate(Task task, TaskIndexer indexer) {
    final long ts = System.currentTimeMillis();
    final boolean isTime = (ts - lastSlowUpdate[indexer.schedulerIndex(task)]) > slowUpdatePeriod;
    if (isTime) {
      lastSlowUpdate[indexer.schedulerIndex(task)] = ts;
    }
    return isTime;
  }

  @Override
  public int dimensions() {
    return N_DIMENSIONS;
  }

  @Override
  public Feature[] requiredFeatures() {
    return requiredFeatures;
  }

  @Override
  public VectorIntraThreadSchedulingFunction enableCaching(int nTasks) {
    USER_PRIORITY_FUNCTION.enableCaching(nTasks);
    highPriorityFunction.enableCaching(nTasks);
    lowPriorityFunction.enableCaching(nTasks);
    this.lastSlowUpdate = new long[nTasks];
    this.caching = true;
    return this;
  }

  @Override
  public void clearCache() {
    USER_PRIORITY_FUNCTION.clearCache();
    highPriorityFunction.clearCache();
    lowPriorityFunction.clearCache();
  }

  @Override
  public boolean cachingEnabled() {
    return caching;
  }

  @Override
  public String name() {
    return "MULTI_CLASS";
  }

  @Override
  public boolean isReverseOrder(int i) {
    return isReverseOrder[i];
  }
}
