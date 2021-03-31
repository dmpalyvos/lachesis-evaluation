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

package scheduling;

import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionImpl;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

public class HighestRateFunction extends VectorIntraThreadSchedulingFunctionImpl {

  public static final int ARRIVAL_TIME_INDEX = 0;
  private long costUpdatePeriodMillis = 500;
  private long[] lastCostUpdate;

  public HighestRateFunction() {
    super(
        IntraThreadSchedulingFunctions.sourceAverageArrivalTime(),
        IntraThreadSchedulingFunctions.globalRate());
  }

  public void setCostUpdatePeriod(long period, TimeUnit timeUnit) {
    this.costUpdatePeriodMillis = timeUnit.toMillis(period);
  }

  @Override
  public void apply(Task task, TaskIndexer indexer, double[][] features, double[] output) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    long ts = System.currentTimeMillis();
    boolean updateCost = (ts - lastCostUpdate[indexer.schedulerIndex(task)]) > costUpdatePeriodMillis;
    if (updateCost) {
      super.apply(task, indexer, features, output);
      lastCostUpdate[indexer.schedulerIndex(task)] = ts;
      return;
    }
    // Otherwise update only arrival time
    output[ARRIVAL_TIME_INDEX] = functions[ARRIVAL_TIME_INDEX].apply(task, indexer, features);
  }

  @Override
  public VectorIntraThreadSchedulingFunction enableCaching(int nTasks) {
    this.lastCostUpdate = new long[nTasks];
    return super.enableCaching(nTasks);
  }

  @Override
  public String name() {
    return "HIGHEST_RATE";
  }
}
