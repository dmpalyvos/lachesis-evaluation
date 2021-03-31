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

package util.logging;

import com.codahale.metrics.Gauge;
import common.metrics.Metrics;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import net.openhft.affinity.Affinity;
import util.Execution;

public class MemoryConsumptionLogger {

  private final Thread loggerThread;
  private static final String LOGGER_NAME = "Memory-Logger";
  private static final long SAMPLE_PERIOD_MS = 1000;

  public MemoryConsumptionLogger(String filename, boolean autoFlush) {
    this.loggerThread = new Thread(new MemoryLoggerRunnable(filename, autoFlush));
    this.loggerThread.setName(LOGGER_NAME);
  }

  public void start() {
    loggerThread.start();
  }

  public void stop() {
    loggerThread.interrupt();
    try {
      loggerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static class MemoryLoggerRunnable implements Runnable {

    private static final long MB = 1024 * 1024;
    private final PrintWriter file;
    private long usedMemory;

    private class UsedMemoryGauge implements Gauge<Long> {

      @Override
      public Long getValue() {
        return usedMemory;
      }
    }

    public MemoryLoggerRunnable(String filename, boolean autoFlush) {
      try {
        file = new PrintWriter(new FileWriter(filename), autoFlush);
        Metrics.metricRegistry().gauge("memory", UsedMemoryGauge::new);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Could not open file %s for writing", filename));
      }
    }

    @Override
    public void run() {
      Affinity.setAffinity(Execution.DEFAULT_AFFINITY);
      while (!Thread.currentThread().isInterrupted()) {
        Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory() / MB;
        long totalMemory = runtime.totalMemory() / MB;
        usedMemory = totalMemory - freeMemory;
        file.println(
            String.format("%s,%d,%d", LOGGER_NAME, System.currentTimeMillis() / 1000, usedMemory));
        try {
          Thread.sleep(SAMPLE_PERIOD_MS);
        } catch (InterruptedException e) {
          break;
        }
      }
      file.flush();
      file.close();
    }
  }
}
