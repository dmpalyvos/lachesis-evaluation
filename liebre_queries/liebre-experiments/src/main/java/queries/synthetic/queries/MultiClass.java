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
import queries.synthetic.SyntheticQueryFactory;
import queries.synthetic.SyntheticQuerySettings;
import synthetic.LiveBurstyRateSource;
import synthetic.LiveConstantRateSource;
import synthetic.HeavySyntheticTuple;
import synthetic.SyntheticTuple;
import util.AbstractQueryFactory;
import util.DataSource;
import util.BaseDataSource;
import util.Execution;

public class MultiClass {

  public static final int LOW_PRIORITY = 1;
  public static final int HIGH_PRIORITY = 3;
  public static final String HIGH_NAME = "HIGH";
  public static final String LOW_NAME = "LOW";
  private static final Logger LOG = LogManager.getLogger();

  public static void main(String[] args) {

    SyntheticQuerySettings settings = SyntheticQuerySettings.newInstance(args);
    Execution.init(settings);

    DataSource<SyntheticTuple> highSource = new BaseDataSource<>(HIGH_NAME,
        new LiveBurstyRateSource<>(HeavySyntheticTuple::new, Execution.liveSettings()),
        settings.highPriority(), settings.sourceAffinity());
    DataSource<SyntheticTuple> lowSource = new BaseDataSource<>(LOW_NAME,
        new LiveConstantRateSource<>(HeavySyntheticTuple::new, Execution.liveSettings()),
        settings.lowPriority(), settings.sourceAffinity());

    final List<AbstractQueryFactory> factories = Arrays.asList(
        new SyntheticQueryFactory<>(settings, HIGH_NAME, highSource, HIGH_PRIORITY),
        new SyntheticQueryFactory<>(settings, LOW_NAME, lowSource, LOW_PRIORITY));
    Execution.execute(settings, factories, settings.highPriority(), settings.lowPriority());
  }

}
