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

package util.logging;

import common.tuple.RichTuple;
import component.operator.in1.filter.FilterFunction;
import common.metrics.Metric;
import query.LiebreContext;

public class LatencyLogger<T extends RichTuple> implements FilterFunction<T> {

  private final Metric latencyMetric;

  public LatencyLogger(String id, String folder, boolean autoFlush) {
    latencyMetric = LiebreContext.userMetrics().newSamplingHistogramMetric(id, "latency");
  }

  @Override
  public void enable() {
    latencyMetric.enable();
  }

  @Override
  public void disable() {
    latencyMetric.disable();
  }

  @Override
  public boolean test(T t) {
    latencyMetric.record(System.currentTimeMillis() - t.getStimulus());
    return true;
  }
}
