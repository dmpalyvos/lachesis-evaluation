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
import scheduling.haren.HarenFeatureTranslator;
import component.source.SourceFunction;
import common.util.backoff.Backoff;
import common.metrics.Metric;
import java.util.Queue;
import query.LiebreContext;

public class RemoteSource<T extends RichTuple> implements SourceFunction<T> {

  public static final String NAME = "SOURCE-0";
  public static final double ALPHA = 0.5;
  private final Queue<T> input;
  private final Metric metric;
  private final Backoff backoff;
  private volatile boolean enabled;
  private volatile double averageArrivalTime = -1;

  public RemoteSource(Queue<T> input, Backoff backoff, String streamName, String statisticsFolder) {
    this.input = input;
    this.backoff = backoff;
    this.metric = LiebreContext.operatorMetrics().newStreamMetric(streamName, "OUT");
  }

  @Override
  public void enable() {
    this.metric.enable();
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.metric.disable();
    this.enabled = false;
  }

  @Override
  public T get() {
    // Finish early if disabled
    // even if the remote queue still has data
    if (!isEnabled()) {
      return null;
    }
    T tuple = input.poll();
    if (tuple == null) {
      backoff.backoff();
      return null;
    }
    metric.record(1);
    backoff.relax();
    return tuple;
  }

  @Override
  public double getHeadArrivalTime() {
    return HarenFeatureTranslator.NO_ARRIVAL_TIME;
  }

  @Override
  public double getAverageArrivalTime() {
    return HarenFeatureTranslator.NO_ARRIVAL_TIME;
  }

}
