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

package util;

import common.tuple.RichTuple;
import component.source.BaseSource;
import component.source.SourceFunction;
import common.metrics.Metric;
import query.LiebreContext;
import synthetic.SyntheticTuple;

public class ThroughputLoggingSource<T extends RichTuple> extends BaseSource<T> {
  private final Metric throughputMetric;

  public ThroughputLoggingSource(
      String id, ExperimentSettings settings, SourceFunction<T> function) {
    super(id, function);
    throughputMetric = LiebreContext.userMetrics().newCountPerSecondMetric(id, "throughput-raw");
  }

  @Override
  public T getNextTuple() {
    T tuple = super.getNextTuple();
    if (tuple != null) {
      tuple.setStimulus(System.currentTimeMillis());
      throughputMetric.record(1);
    }
    return tuple;
  }

  @Override
  public void enable() {
    throughputMetric.enable();
    super.enable();
  }

  @Override
  public void disable() {
    super.disable();
    throughputMetric.disable();
  }
}
