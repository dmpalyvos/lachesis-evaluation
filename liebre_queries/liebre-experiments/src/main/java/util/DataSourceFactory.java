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
import common.util.backoff.Backoff;
import common.util.backoff.ExponentialBackoff;
import common.util.backoff.InactiveBackoff;
import component.source.Source;
import component.source.SourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import synthetic.CloneableSourceFunction;

public abstract class DataSourceFactory<T extends RichTuple, S extends ExperimentSettings>
    extends AbstractQueryFactory {

  protected static final Logger LOG = LogManager.getLogger();
  protected final S settings;
  protected final Thread dataSourceThread;
  protected final DataSource<T> dataSource;

  public DataSourceFactory(String queryName, S settings, DataSource<T> dataSource) {
    super(queryName);
    this.settings = settings;
    this.dataSourceThread = new Thread(dataSource);
    this.dataSource = dataSource;
    dataSourceThread.setName(String.format("DataSource-%s", queryName));
  }

  public DataSourceFactory(String queryName, S settings, SourceFunction<T> sourceFunction) {
    this(
        queryName,
        settings,
        new BaseDataSource<>(
            queryName,
            sourceFunction,
            settings.parallelism(),
            settings.sourceAffinity()));
  }

  @Override
  public void onBeforeQueryExecution() {
    dataSourceThread.start();
  }

  @Override
  public void onAfterQueryExecution() {
    dataSourceThread.interrupt();
    try {
      dataSourceThread.join();
    } catch (InterruptedException e) {
      LOG.warn("DataSource join interrupted!");
    }
  }

  protected final Source<T> newSource() {
    Source<T> source =
        new ThroughputLoggingSource<>(
            name(RemoteSource.NAME),
            settings,
            new RemoteSource<>(
                dataSource.initQueue(queryIndex),
                sourceBackoff(),
                dataSource.streamName(queryIndex),
                settings.statisticsFolder()));
    dataSource.enableQueue(queryIndex);
    return source;
  }

  protected Backoff sourceBackoff() {
    return InactiveBackoff.INSTANCE;
  }
}
