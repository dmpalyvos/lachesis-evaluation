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
import com.esotericsoftware.kryo.io.Input;
import common.tuple.RichTuple;
import scheduling.haren.HarenFeatureTranslator;
import component.source.SourceFunction;
import common.util.backoff.Backoff;
import common.metrics.Metric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import query.LiebreContext;
import zmq.ZMQ;

public class TCPReceiver<T extends RichTuple> implements SourceFunction<T> {

  public static final String NAME = "Source";
  public static final double ALPHA = 0.5;
  private static final int TIMEOUT = 10000;
  private final Logger LOG = LogManager.getLogger();
  private final String connection;
  private final Backoff backoff;
  private final Metric reads;
  private ZContext context;
  private Socket receiver;
  private volatile boolean enabled;
  private final Kryo kryo = KryoFactory.getKryo();
  private volatile double averageArrivalTime = -1;

  public TCPReceiver(
      int dataSourceIndex,
      int queryIndex,
      Backoff backoff,
      String queryName,
      String statisticsFolder) {
    this.backoff = backoff;
    this.connection = TCPSender.getConnection(dataSourceIndex, queryIndex);
    this.reads =
        LiebreContext.userMetrics()
            .newStreamMetric(TCPSender.getStreamName(queryName, queryIndex), "OUT");
  }

  @Override
  public void enable() {
    context = new ZContext();
    receiver = context.createSocket(ZMQ.ZMQ_PULL);
    receiver.connect(connection);
    receiver.setReceiveTimeOut(TIMEOUT);
    LOG.debug("Initialized connection {}", connection);
    reads.enable();
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    reads.disable();
    try {
      receiver.close();
      context.close();
    } catch (Exception exception) {
      LOG.debug("Failed to close connection {}", connection);
      return;
    }
    LOG.debug("Closed connection {}", connection);
  }

  @Override
  public T get() {
    // Finish early if disabled
    // even if the remote queue still has data
    if (!isEnabled()) {
      return null;
    }
    if (context.isClosed()) {
      return null;
    }
    try {
      byte[] message = receiver.recv(ZMQ.ZMQ_DONTWAIT);
      if (message == null) {
        backoff.backoff();
        return null;
      }
      T tuple = (T) kryo.readClassAndObject(new Input(message));
      backoff.relax();
      updateAverageArrivalTime(tuple);
      reads.record(1);
      return tuple;
    } catch (Exception exception) {
      LOG.debug("Failed to receive data from connection {}", connection);
      LOG.debug("Details: ", exception);
      return null;
    }
  }

  @Override
  public double getHeadArrivalTime() {
    return getAverageArrivalTime();
  }

  @Override
  public double getAverageArrivalTime() {
    return averageArrivalTime > 0 ? averageArrivalTime : HarenFeatureTranslator.NO_ARRIVAL_TIME;
  }

  private void updateAverageArrivalTime(T tuple) {
    averageArrivalTime =
        averageArrivalTime < 0
            ? tuple.getStimulus()
            : (ALPHA * tuple.getStimulus()) + ((1 - ALPHA) * averageArrivalTime);
  }
}
