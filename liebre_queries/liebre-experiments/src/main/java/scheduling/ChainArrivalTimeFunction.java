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

import io.palyvos.haren.FeatureHelper;
import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import io.palyvos.haren.function.AbstractIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionImpl;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

/** The full chain scheduling policy, e.g. chain + arrival time, with chain slowly changing. */
public class ChainArrivalTimeFunction extends VectorIntraThreadSchedulingFunctionImpl {

  public static final int ARRIVAL_TIME_INDEX = 2;
  private static SingleIntraThreadSchedulingFunction sourcesLast =
      new AbstractIntraThreadSchedulingFunction("SOURCES_LAST", Features.COMPONENT_TYPE) {
        @Override
        public double apply(Task task, TaskIndexer indexer, double[][] features) {
          if (Features.COMPONENT_TYPE.get(task, indexer, features) == FeatureHelper.CTYPE_SOURCE) {
            return 0;
          } else {
            return 1;
          }
        }
      };
  private long chainUpdatePeriodMillis = 250;
  private long[] lastChainUpdate;

  public ChainArrivalTimeFunction() {
    super(
        sourcesLast,
        IntraThreadSchedulingFunctions.chain(),
        IntraThreadSchedulingFunctions.headArrivalTime());
  }

  public void setCostUpdatePeriod(long period, TimeUnit timeUnit) {
    this.chainUpdatePeriodMillis = timeUnit.toMillis(period);
  }

  @Override
  public void apply(Task task, TaskIndexer indexer, double[][] features, double[] output) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    long ts = System.currentTimeMillis();
    boolean updateCost = (ts - lastChainUpdate[indexer.schedulerIndex(task)]) > chainUpdatePeriodMillis;
    if (updateCost) {
      super.apply(task, indexer, features, output);
      lastChainUpdate[indexer.schedulerIndex(task)] = ts;
      return;
    }
    // Otherwise update only arrival time
    output[ARRIVAL_TIME_INDEX] = functions[ARRIVAL_TIME_INDEX].apply(task, indexer, features);
  }

  @Override
  public VectorIntraThreadSchedulingFunction enableCaching(int nTasks) {
    this.lastChainUpdate = new long[nTasks];
    return super.enableCaching(nTasks);
  }

  @Override
  public String name() {
    return "CHAIN_ARRIVAL_TIME";
  }
}
