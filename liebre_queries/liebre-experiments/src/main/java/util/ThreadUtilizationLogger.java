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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;

//FIXME: Warning, this only works correctly for a very small number of threads!
public class ThreadUtilizationLogger implements Active {

  private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger();

  private static final long SAMPLE_FREQUENCY_MS = 1000;
  private static final String LOGGER_THREAD_NAME = "Utilization-Logger";
  public static final String OUTPUT_THREAD_NAME = "Execution-Total-Utilization";
  private final Thread loggerThread;

  public ThreadUtilizationLogger(String utilizationFile, boolean autoFlush) {
    loggerThread = new Thread(new Logger(utilizationFile, autoFlush));
    loggerThread.setName(LOGGER_THREAD_NAME);
  }

  @Override
  public void enable() {
    loggerThread.start();
  }

  @Override
  public boolean isEnabled() {
    return loggerThread.isAlive();
  }

  @Override
  public void disable() {
    loggerThread.interrupt();
    try {
      loggerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static class Logger implements Runnable {

    private final PrintWriter utilizationFile;

    public Logger(String filepath, boolean autoFlush) {
      try {
        utilizationFile = new PrintWriter(new FileWriter(filepath), autoFlush);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Could not open file %s for writing", filepath));
      }
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        writeThreadUtilizations(currentSecond);
        try {
          Thread.sleep(SAMPLE_FREQUENCY_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      utilizationFile.flush();
      utilizationFile.close();
    }

    private void writeThreadUtilizations(long timestamp) {
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      long totalTime = 0;
      for (long threadId : threadMXBean.getAllThreadIds()) {
        try {
          String name = threadMXBean.getThreadInfo(threadId).getThreadName();
          long cpuTime = threadMXBean.getThreadCpuTime(threadId);
          totalTime += cpuTime;
        }
        catch(Exception exception){
          LOG.warn("Failed to write utilization for thread #{}", threadId);
        }
      }
      writeLine(OUTPUT_THREAD_NAME, System.currentTimeMillis(), totalTime);
    }

    private void writeLine(String threadName, long timestamp, long value) {
      utilizationFile.println(String.format("%s,%d,%d", threadName, timestamp, value));
    }
  }


}
