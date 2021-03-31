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
import component.sink.AbstractSink;
import component.sink.SinkFunction;
import common.metrics.Metric;
import query.LiebreContext;

public class LatencyLoggingSink<T extends RichTuple> extends AbstractSink<T> {

  private final Metric latencyMetric;
  private final Metric endLatencyMetric;
  private final SinkFunction<T> function;
  private final Metric rateMetric;

  public LatencyLoggingSink(String id, SinkFunction<T> function, String statisticsFolder,
      boolean autoFlush) {
    super(id);
    this.latencyMetric = LiebreContext.userMetrics().newSamplingHistogramMetric(id, "latency-raw");
    this.endLatencyMetric = LiebreContext.userMetrics().newSamplingHistogramMetric(id, "end-latency-raw");
    this.rateMetric = LiebreContext.userMetrics().newCountPerSecondMetric(id, "sink-throughput-raw");
    this.function = function;
  }

  public LatencyLoggingSink(String id, String statisticsFolder, boolean autoFlush) {
   this(id, null, statisticsFolder, autoFlush);
  }

  @Override
  public void processTuple(T tuple) {
    if (function != null) {
      function.accept(tuple);
    }
    final long now = System.currentTimeMillis();
    latencyMetric.record(now - tuple.getStimulus());
    endLatencyMetric.record(now - tuple.getTimestamp());
    rateMetric.record(1);
  }

  @Override
  public void enable() {
    super.enable();
    latencyMetric.enable();
    endLatencyMetric.enable();
    rateMetric.enable();
  }

  @Override
  public void disable() {
    endLatencyMetric.disable();
    latencyMetric.disable();
    rateMetric.disable();
    super.disable();
  }
}
