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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import common.Active;
import component.source.SourceFunction;
import common.metrics.Metric;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import query.LiebreContext;
import synthetic.CloneableSourceFunction;
import zmq.ZMQ;

public class TCPSender<T extends Serializable> implements Active {

  public static final String NAME = "DataSource";
  private static final Logger LOG = LogManager.getLogger();
  private final CloneableSourceFunction<T> function;
  private final int index;
  private final int receivers;
  private final List<Thread> threads = new ArrayList<>();
  private final List<TCPSource<T>> sources = new ArrayList<>();
  private final String statisticsFolder;
  private final String queryName;

  public TCPSender(
      CloneableSourceFunction<T> function,
      int index,
      int receivers,
      String queryName,
      String statisticsFolder) {
    this.function = function;
    this.index = index;
    this.receivers = receivers;
    this.statisticsFolder = statisticsFolder;
    this.queryName = queryName;
  }

  public static int getPortNumber(int dataSourceIndex, int sourceIndex) {
    return 10000 + (1000 * dataSourceIndex + sourceIndex);
  }

  public static String getConnection(int dataSourceIndex, int sourceIndex) {
    return String.format("tcp://localhost:%d", getPortNumber(dataSourceIndex, sourceIndex));
  }

  public static String getStreamName(String queryName, int queryIndex) {
    return String.format(
        "%s-%d-%s_%s-%d-%s",
        queryName, queryIndex, TCPSender.NAME, queryName, queryIndex, TCPReceiver.NAME);
  }

  @Override
  public void enable() {
    for (int i = 0; i < receivers; i++) {
      TCPSource<T> source = new TCPSource<>(index, i, function.copy(), queryName, statisticsFolder);
      sources.add(source);
      Thread thread = new Thread(source);
      thread.setName(String.format("DataSource-%d-SourceThread-%d", index, i));
      threads.add(thread);
      thread.start();
    }
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void disable() {
    for (TCPSource<?> source : sources) {
      source.disable();
    }
    for (Thread thread : threads) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted when joining {}", thread);
      }
    }
  }

  private static class TCPSource<T extends Serializable> implements Runnable, Active {

    private static final int TIMEOUT = 10000;
    private static final int OUTPUT_BUFFER_INITIAL_CAPACITY = 4096;
    private final String connection;
    private final SourceFunction<T> function;
    private final Metric writes;
    private ZContext context;
    private Socket sender;
    private volatile boolean enabled;
    private final Kryo kryo = KryoFactory.getKryo();

    public TCPSource(
        int dataSourceIndex,
        int sourceIndex,
        SourceFunction<T> function,
        String queryName,
        String statisticsFolder) {
      this.connection = getConnection(dataSourceIndex, sourceIndex);
      this.writes =
          LiebreContext.userMetrics()
              .newStreamMetric(getStreamName(queryName, sourceIndex), "IN");
      this.function = function;
    }

    @Override
    public void enable() {
      context = new ZContext();
      sender = context.createSocket(ZMQ.ZMQ_PUSH);
      sender.setSendTimeOut(TIMEOUT);
      sender.bind(connection);
      LOG.debug("Initiated connection {}", connection);
      writes.enable();
      this.enabled = true;
    }

    @Override
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public void disable() {
      this.enabled = false;
      writes.disable();
      try {
        context.close();
        LOG.debug("Closed connection {}", connection);
      } catch (Exception exception) {
        LOG.debug("Failed to close connection {}: {}", connection, exception.getMessage());
      }
    }

    @Override
    public void run() {
      enable();
      while (isEnabled()) {
        try {
          Output output = new Output(OUTPUT_BUFFER_INITIAL_CAPACITY, -1);
          kryo.writeClassAndObject(output, function.get());
          sender.send(output.getBuffer());
          writes.record(1);
        } catch (Exception exception) {
          LOG.debug("Failed to send message through connection {}", connection);
          LOG.debug(exception);
        }
      }
    }
  }
}
