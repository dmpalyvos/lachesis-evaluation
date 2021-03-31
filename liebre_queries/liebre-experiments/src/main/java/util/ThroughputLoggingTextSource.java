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

import component.source.TextFileSourceFunction;
import common.metrics.Metric;
import query.LiebreContext;

public class ThroughputLoggingTextSource extends TextFileSourceFunction {

  private final Metric throughputMetric;

  public ThroughputLoggingTextSource(String id, ExperimentSettings settings) {
    super(settings.inputFile(id));
    throughputMetric = LiebreContext.userMetrics().newCountPerSecondMetric(id, "rate");
  }

  @Override
  public String get() {
    throughputMetric.record(1);
    return super.get();
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
