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

import common.util.RateLimiter;
import component.source.SourceFunction;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IncreasingRateSource<T extends SyntheticTuple> implements SourceFunction<T> {

  private final Thread rateIncreaseThread;
  private volatile int rate;
  private final RateLimiter limiter = new RateLimiter();
  private final Supplier<T> supplier;

  public IncreasingRateSource(Supplier<T> supplier, int initialRate, int step,
      int stepFrequencyMillis) {
    this.supplier = supplier;
    this.rate = initialRate;
    this.rateIncreaseThread = new Thread(new RateIncreaseAction(step, stepFrequencyMillis, this));
  }

  @Override
  public void enable() {
    rateIncreaseThread.start();
  }

  @Override
  public void disable() {
   rateIncreaseThread.interrupt();
    try {
      rateIncreaseThread.join();
    } catch (InterruptedException e) {
    }
  }

  @Override
  public T get() {
    limiter.limit(rate);
    return supplier.get();
  }

  private static class RateIncreaseAction implements Runnable {

    private static final Logger LOG = LogManager.getLogger();
    private final int step;
    private final int stepFrequencyMillis;
    private final IncreasingRateSource source;

    public RateIncreaseAction(int step, int stepFrequencyMillis,
        IncreasingRateSource source) {
      this.step = step;
      this.stepFrequencyMillis = stepFrequencyMillis;
      this.source = source;
    }


    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(stepFrequencyMillis);
        } catch (InterruptedException e) {
          return;
        }
        source.rate += step;
        LOG.info("increased source rate to {}", source.rate);
      }
    }
  }

}
