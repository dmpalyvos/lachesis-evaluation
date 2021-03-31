/* * Copyright (C) 2017-2019
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
import queries.synthetic.ImprovedSyntheticQueryFactory;
import queries.synthetic.SyntheticQueryFactory;
import queries.synthetic.SyntheticQuerySettings;
import synthetic.HeavySyntheticTuple;
import util.DataSource;
import util.BaseDataSource;
import util.Execution;
import util.KafkaSourceFunction;

public class Chain {

  public static final String NAME = Chain.class.getSimpleName();

  public static void main(String[] args) {
    SyntheticQuerySettings settings = SyntheticQuerySettings.newInstance(args);
    Execution.init(settings);
    DataSource<HeavySyntheticTuple> dataSource = new BaseDataSource<>(NAME,
        new KafkaSourceFunction<>(HeavySyntheticTuple::new, settings),
        settings.nqueries(), settings.sourceAffinity());
    Execution.execute(settings, Arrays
        .asList(new SyntheticQueryFactory<>(settings, NAME, dataSource, 0)),
        settings.nqueries());
  }

}
