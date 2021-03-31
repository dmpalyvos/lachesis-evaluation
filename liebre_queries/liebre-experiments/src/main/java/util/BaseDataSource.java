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

import common.metrics.Metric;
import component.source.SourceFunction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import query.LiebreContext;

public class BaseDataSource<T> extends AbstractDataSource<T> {

  public static final String RATE_METRIC_KEY = "inRate";
  public static final String EXTERNAL_QUEUE_SIZE = "EXTERNAL_QUEUE_SIZE";
  private final List<Queue<T>> queues = new ArrayList<>();

  public BaseDataSource(String id, SourceFunction<T> function, int nQueues,
      BitSet affinity) {
    super(function, id, affinity);
    for (int i = 0; i < nQueues; i++) {
      Queue<T> e = new ConcurrentLinkedQueue<>();
      queues.add(e);
      Metric rateMetric = LiebreContext.operatorMetrics()
          .newCountPerSecondMetric(streamName(i), RATE_METRIC_KEY);
      metrics.add(rateMetric);
      Metric queueSizeMetric = LiebreContext.streamMetrics().newGaugeMetric(streamName(i),
          EXTERNAL_QUEUE_SIZE,
          () -> Long.valueOf(e.size()));
      queueSizeMetric.enable();
      rateMetric.enable();
    }
  }

  @Override
  public Queue<T> initQueue(int index) {
    return queues.get(index);
  }

  @Override
  public void enableQueue(int index) {
    // All queues in this DataSource are enabled by default
  }

  @Override
  public void disableQueue(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void emit(T tuple) {
    for (int i = 0; i < queues.size(); i++) {
      queues.get(i).offer(tuple);
      metrics.get(i).record(1);
    }
  }

}
