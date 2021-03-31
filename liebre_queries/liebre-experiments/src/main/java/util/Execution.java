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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.PickledGraphite;
import common.metrics.FileMetricsFactory;
import common.metrics.MetricName;
import common.metrics.Metrics;
import common.util.backoff.InactiveBackoff;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import net.openhft.affinity.Affinity;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import query.LiebreContext;
import query.Query;
import query.QueryGraphExporter;
import scheduling.basic.BasicLiebreScheduler;
import scheduling.haren.HarenLiebreSchedulerAdapter;
import stream.BackoffStreamFactory;
import stream.BlockingStreamFactory;

public class Execution {

  public static final int DEFAULT_AFFINITY = 0;
  private static final Logger LOGGER = LogManager.getLogger();
  private static final AtomicInteger activeSources = new AtomicInteger(0);
  private static final LiveSettingsUpdater LIVE_SETTINGS_UPDATER = new LiveSettingsUpdater();
  private static boolean useLiveSettingsUpdater;
  private static final Pattern SCHEDULER_METRIC_PATTERN = Pattern
      .compile("QUEUE_SIZE|ARRIVAL_TIME|OUT|IN|EXTERNAL_QUEUE_SIZE");
  public static final MetricFilter SCHEDULER_METRIC_FILTER = (name, metric) -> SCHEDULER_METRIC_PATTERN
      .matcher(name).find();
  private static final Pattern USER_METRIC_PATTERN = Pattern
      .compile("rate|latency|endLatency|sourceRate|sinkRate");
  public static final MetricFilter USER_METRIC_FILTER = (name, metric) -> USER_METRIC_PATTERN
      .matcher(name).find();

  public static void useLiveSettingsUpdater() {
    useLiveSettingsUpdater = true;
  }

  public static final void execute(ExperimentSettings settings,
      AbstractQueryFactory queryFactory) {
    execute(settings, Arrays.asList(queryFactory));
  }

  public static LiveSettings liveSettings() {
    return LIVE_SETTINGS_UPDATER.settings();
  }

  public static final void execute(ExperimentSettings settings,
      Collection<AbstractQueryFactory> queryFactories) {
    Query query = Execution.initQuery(settings);
    generateQueries(settings, queryFactories, query);
    runWithSetupTeardown(settings, queryFactories, query);
  }

  public static final void execute(ExperimentSettings settings,
      Collection<AbstractQueryFactory> queryFactories, int... parallelisms) {
    Query query = Execution.initQuery(settings);
    generateQueries(queryFactories, query, parallelisms);
    runWithSetupTeardown(settings, queryFactories, query);
  }

  private static void generateQueries(ExperimentSettings settings,
      Collection<AbstractQueryFactory> queryFactories, Query query) {
    queryFactories.forEach(f -> f.generateQueries(query, settings.parallelism()));
  }

  private static void generateQueries(Collection<AbstractQueryFactory> queryFactories, Query query,
      int[] parallelisms) {
    int i = 0;
    for (AbstractQueryFactory factory : queryFactories) {
      factory.generateQueries(query, parallelisms[i++]);
    }
  }

  private static void runWithSetupTeardown(ExperimentSettings settings,
      Collection<AbstractQueryFactory> queryFactories, Query query) {
    queryFactories.forEach(f -> f.onBeforeQueryExecution());
    Execution.runQuery(query, settings);
    queryFactories.forEach(f -> f.onAfterQueryExecution());
  }

  public static void init(ExperimentSettings settings) {
    Affinity.setAffinity(Execution.DEFAULT_AFFINITY);
    LiebreContext.setUserMetrics(new FileMetricsFactory(settings.statisticsFolder(),
        (id, type) -> type + "_" + id));
    LiebreContext.setStreamMetrics(Metrics.dropWizard());
    LiebreContext.setOperatorMetrics(Metrics.dropWizard());
    final PickledGraphite graphite = new PickledGraphite(
        new InetSocketAddress(settings.statisticsHost(), 2004));
    final GraphiteReporter graphiteReporter =
        GraphiteReporter.forRegistry(Metrics.metricRegistry())
            .prefixedWith(String.format("liebre.%s.", settings.experimentName()))
            .build(graphite);
    graphiteReporter.start(1, TimeUnit.SECONDS);
  }

  public static final void runQuery(Query q, ExperimentSettings settings) {
    Configurator.setLevel("kafka", Level.WARN);
    QueryGraphExporter.export(q, settings.queryDAGFile());
    if (useLiveSettingsUpdater) {
      LIVE_SETTINGS_UPDATER.enable();
    }
    activeSources.set(q.sourcesNumber());
    LOGGER.info("Configuration: {}", settings);

    q.activate();
    final Thread hook = new Thread(() -> {
      LOGGER.warn("Shutting down JVM...");
      cleanup(q);
    });
    Runtime.getRuntime().addShutdownHook(hook);
    try {
      Thread.sleep(settings.durationMillis());
    } catch (InterruptedException e) {
      LOGGER.info("Query finished before timeout!");
    }
    cleanup(q);
    Runtime.getRuntime().removeShutdownHook(hook);
  }

  private static void cleanup(Query q) {
    LOGGER.info("Cleaning up...");
    q.deActivate();
    if (useLiveSettingsUpdater) {
      LIVE_SETTINGS_UPDATER.disable();
    }
  }

  public static final Query initQuery(ExperimentSettings settings) {
    LiebreContext.disableFlushing();
    Query q;
    if (settings.scheduler() instanceof BasicLiebreScheduler) {
      q = new Query(settings.scheduler(), new BlockingStreamFactory());
      q.setBackoff(InactiveBackoff.INSTANCE);
    } else if (settings.scheduler() instanceof HarenLiebreSchedulerAdapter) {
      q = new Query(settings.scheduler(), new BackoffStreamFactory());
      q.setBackoff(InactiveBackoff.INSTANCE);
      LIVE_SETTINGS_UPDATER.setHaren((HarenLiebreSchedulerAdapter) settings.scheduler());
    } else {
      throw new IllegalStateException("Unknown scheduler class!");
    }
    return q;
  }

  public static void sourceFinished() {
    LOGGER.info("Source finished");
    if (activeSources.decrementAndGet() == 0) {
      LOGGER.info("Interrupting...");
      Thread.currentThread().interrupt();
    }
  }


}
