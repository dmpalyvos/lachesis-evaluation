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

package queries.synthetic.queries;

import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import queries.synthetic.SyntheticQueryFactoryNonRandom;
import queries.synthetic.SyntheticQuerySettings;
import synthetic.HeavySyntheticTuple;
import synthetic.LiveConstantRateSource;
import util.AbstractQueryFactory;
import util.BaseDataSource;
import util.DataSource;
import util.Execution;
import util.LiveSettings;

public class IncreasingRate {

  private static final Logger LOG = LogManager.getLogger();

  public static final int BASE_PRIORITY = 1;
  public static final String BASE_NAME = "LOW";

  public static void main(String[] args) {

    SyntheticQuerySettings settings = SyntheticQuerySettings.newInstance(args);
    Execution.init(settings);
    DataSource<HeavySyntheticTuple> dataSource =
        new BaseDataSource<>(
            BASE_NAME,
            new LiveConstantRateSource<>(HeavySyntheticTuple::new, Execution.liveSettings()),
            settings.nqueries(),
            settings.sourceAffinity());

    final List<AbstractQueryFactory> factories =
        Arrays.asList(
            new SyntheticQueryFactoryNonRandom<>(settings, BASE_NAME, dataSource, BASE_PRIORITY));
    Execution.liveSettings().rate = settings.rate();
    Thread increaserThread = new Thread(
        new RateIncreaser(settings.rateStep(), settings.ratePeriod(), Execution.liveSettings()));
    increaserThread
        .start();
    Execution.execute(settings, factories, settings.nqueries());
    increaserThread.interrupt();
  }

  private static class RateIncreaser implements Runnable {

    private final LiveSettings liveSettings;
    private final long stepPeriod;
    private final int step;

    public RateIncreaser(int step, long stepPeriod, LiveSettings liveSettings) {
      this.liveSettings = liveSettings;
      this.stepPeriod = stepPeriod;
      this.step = step;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(stepPeriod);
        } catch (InterruptedException e) {
          return;
        }
        LOG.info("Increasing source rate to {}", liveSettings.rate + step);
        liveSettings.rate += step;
      }
    }
  }
}
