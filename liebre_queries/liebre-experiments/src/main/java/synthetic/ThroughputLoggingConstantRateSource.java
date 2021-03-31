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

package synthetic;

import component.source.SourceFunction;
import common.util.RateLimiter;
import common.metrics.Metric;
import query.LiebreContext;

public class ThroughputLoggingConstantRateSource implements SourceFunction<SyntheticTuple> {

  private final Metric rateMetric;
  private final long rate;
  private final RateLimiter limiter;

  public ThroughputLoggingConstantRateSource(String id, String folder) {
    this(id, folder, -1);
  }

  public ThroughputLoggingConstantRateSource(String id, String folder, int rate) {
    this.rateMetric = LiebreContext.userMetrics().newCountPerSecondMetric(id, "rate");
    this.limiter = new RateLimiter();
    this.rate = rate;

  }

  @Override
  public void enable() {
    rateMetric.enable();
  }

  @Override
  public void disable() {
    rateMetric.disable();
  }

  @Override
  public SyntheticTuple get() {
    limiter.limit(rate);
    rateMetric.record(1L);
    return new SyntheticTuple();
  }

}
