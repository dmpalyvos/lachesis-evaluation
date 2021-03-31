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
import queries.synthetic.ElasticSyntheticQueryFactory;
import queries.synthetic.SyntheticQuerySettings;
import synthetic.HeavySyntheticTuple;
import synthetic.LiveBurstyRateSource;
import synthetic.LiveConstantRateSource;
import synthetic.SyntheticTuple;
import util.AbstractQueryFactory;
import util.DataSource;
import util.ElasticDataSource;
import util.Execution;

public class ElasticMultiClass {

  public static final int LOW_PRIORITY = 1;
  public static final int HIGH_PRIORITY = 3;
  public static final String HIGH_NAME = "HIGH";
  public static final String LOW_NAME = "LOW";

  public static void main(String[] args) {

    SyntheticQuerySettings settings = SyntheticQuerySettings.newInstance(args);
    Execution.init(settings);
    DataSource<SyntheticTuple> highSource =
        new ElasticDataSource<>(
            HIGH_NAME,
            new LiveBurstyRateSource<>(HeavySyntheticTuple::new, Execution.liveSettings()),
            settings.sourceAffinity());
    DataSource<SyntheticTuple> lowSource =
        new ElasticDataSource<>(
            LOW_NAME,
            new LiveConstantRateSource<>(HeavySyntheticTuple::new, Execution.liveSettings()),
            settings.sourceAffinity());

    final List<AbstractQueryFactory> factories =
        Arrays.asList(
            new ElasticSyntheticQueryFactory<>(settings, HIGH_NAME, highSource, HIGH_PRIORITY),
            new ElasticSyntheticQueryFactory<>(settings, LOW_NAME, lowSource, LOW_PRIORITY));
    Execution.useLiveSettingsUpdater();
    Execution.execute(settings, factories, settings.highPriority(), settings.lowPriority());
  }
}
