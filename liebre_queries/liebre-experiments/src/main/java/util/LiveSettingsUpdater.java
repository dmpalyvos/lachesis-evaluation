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

import common.Active;
import common.util.Util;
import io.palyvos.haren.function.NoopIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionImpl;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import queries.synthetic.MultiClassSchedulingFunction;
import scheduling.IntraThreadSchedulingFunctions;
import scheduling.haren.HarenLiebreSchedulerAdapter;

public class LiveSettingsUpdater implements Active {

  private static final Logger LOG = LogManager.getLogger();
  private static final long FREQUENCY_MILLIS = 5000;
  private static String CONFIG_FILE = "live_config.yaml";
  private Thread executor;
  private volatile HarenLiebreSchedulerAdapter haren;
  private final LiveSettings settings = new LiveSettings();

  public void setHaren(HarenLiebreSchedulerAdapter haren) {
    this.haren = haren;
  }

  @Override
  public void enable() {
    executor = new Thread(new Poller(settings, haren));
    executor.start();
  }

  @Override
  public boolean isEnabled() {
    return !executor.isInterrupted();
  }

  @Override
  public void disable() {
    executor.interrupt();
  }

  public LiveSettings settings() {
    return settings;
  }

  private static class Poller implements Runnable {

    private final Yaml yaml = new Yaml(new Constructor(LiveSettings.class));
    private final Map<String, VectorIntraThreadSchedulingFunction> intraThreadFunctions =
        new HashMap<>();

    private final LiveSettings settings;
    private final HarenLiebreSchedulerAdapter haren;

    public Poller(LiveSettings settings, HarenLiebreSchedulerAdapter haren) {
      this.settings = settings;
      this.haren = haren;
      intraThreadFunctions.put(
          "MultiClass(High: MaxLatency, Low: AvgLatency)", new MultiClassSchedulingFunction());
      // For FCFS we add to NO-OP functions to make sure the output has the same length as the
      // MultiClass
      intraThreadFunctions.put(
          "MaxLatency",
          new VectorIntraThreadSchedulingFunctionImpl(
              IntraThreadSchedulingFunctions.averageArrivalTime(),
              NoopIntraThreadSchedulingFunction.INSTANCE,
              NoopIntraThreadSchedulingFunction.INSTANCE));
      intraThreadFunctions.put(
          "AvgLatency",
          new VectorIntraThreadSchedulingFunctionImpl(
              IntraThreadSchedulingFunctions.globalRate(),
              IntraThreadSchedulingFunctions.averageArrivalTime(),
              NoopIntraThreadSchedulingFunction.INSTANCE));
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {
          LiveSettings newSettings = yaml.load(input);
          settings.copyFieldsFrom(newSettings);
          LOG.info(settings);
          if (haren != null) {
            haren.setBatchSize(settings.batchSize);
            haren.setSchedulingPeriod(settings.schedulingPeriodMillis);
//            haren.setIntraThreadFunction(intraThreadFunctions.get(settings.intraThreadFunctionKey));
          }
        } catch (Exception exception) {
          LOG.error(exception);
        }
        Util.sleep(FREQUENCY_MILLIS);
      }
    }
  }
}
