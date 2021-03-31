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

package scheduling;

import static io.palyvos.haren.FeatureHelper.CTYPE_SOURCE;

import io.palyvos.haren.FeatureHelper;
import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import io.palyvos.haren.function.AbstractIntraThreadSchedulingFunction;
import io.palyvos.haren.function.CachingIntraThreadSchedulingFunction;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ChainIntraThreadSchedulingFunction extends AbstractIntraThreadSchedulingFunction {

  public static final int NOT_INITIALIZED = -1;
  private static final Logger LOG = LogManager.getLogger();
  private static final SingleIntraThreadSchedulingFunction TOTAL_SELECTIVITY =
      new CachingIntraThreadSchedulingFunction(
          "TOTAL_SELECTIVITY", Features.SELECTIVITY, Features.COMPONENT_TYPE) {
        @Override
        protected double applyWithCachingSupport(
            Task task, TaskIndexer indexer, double[][] features) {
          Task upstream = getUpstream(task);
          double selectivity = getSelectivity(task, indexer, features);
          if (upstream == null) {
            return selectivity;
          } else {
            return selectivity * apply(upstream, indexer, features);
          }
        }
      };
  private static final SingleIntraThreadSchedulingFunction TIME =
      new CachingIntraThreadSchedulingFunction("TOTAL_TIME", Features.COST, Features.SELECTIVITY) {

        @Override
        protected double applyWithCachingSupport(
            Task task, TaskIndexer indexer, double[][] features) {
          Task upstream = getUpstream(task);
          if (upstream == null) {
            return Features.COST.get(task, indexer, features);
          } else {
            return Features.COST.get(task, indexer, features)
                    * TOTAL_SELECTIVITY.apply(upstream, indexer, features)
                + apply(upstream, indexer, features);
          }
        }

      };
  public static final double SINK_SELECTIVITY = 0;
  private double[] sdop;
  private boolean warm = false;

  public ChainIntraThreadSchedulingFunction() {
    super("CHAIN", TOTAL_SELECTIVITY, TIME);
  }

  private static Task getDownstream(Task task) {
    List<? extends Task> downstream = task.getDownstream();
    Validate.isTrue(downstream.size() <= 1, "This implementation works only for chains!");
    return downstream.size() == 0 ? null : downstream.get(0);
  }

  private static Task getUpstream(Task task) {
    List<? extends Task> upstream = task.getUpstream();
    Validate.isTrue(upstream.size() <= 1, "This implementation works only for chains!");
    return upstream.size() == 0 ? null : upstream.get(0);
  }

  private static double getSelectivity(Task task, TaskIndexer indexer, double[][] features) {
    boolean isSink =
        Features.COMPONENT_TYPE.get(task, indexer, features) == FeatureHelper.CTYPE_SINK;
    return isSink ? SINK_SELECTIVITY : Features.SELECTIVITY.get(task, indexer, features);
  }

  @Override
  public double apply(Task task, TaskIndexer indexer, double[][] features) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    if (sdop[indexer.schedulerIndex(task)] < 0) {
      Task source = getSource(task, indexer, features);
      calculateLowerEnvelope(source, features, indexer);
    }
    return sdop[indexer.schedulerIndex(task)];
  }

  private void calculateLowerEnvelope(Task task, double[][] features, TaskIndexer indexer) {
    if (task == null) {
      return;
    }
    List<Task> lowerEnvelopeCandidates = new ArrayList<>();
    lowerEnvelopeCandidates.add(task);
    Task downstream = task;
    Task selected = null;
    double maxDerivative = NOT_INITIALIZED;
    while ((downstream = getDownstream(downstream)) != null) {
      double derivative = getDerivative(task, downstream, features, indexer);
      if (derivative > maxDerivative) {
        maxDerivative = derivative;
        selected = downstream;
      }
      lowerEnvelopeCandidates.add(downstream);
    }
    // Create the envelope
    if (selected != null) {
      if (LOG.getLevel() == Level.TRACE) {
        printDetails(features, lowerEnvelopeCandidates, selected, maxDerivative, indexer);
      }
      for (Task candidate : lowerEnvelopeCandidates) {
        sdop[indexer.schedulerIndex(candidate)] = maxDerivative;
        if (indexer.schedulerIndex(candidate) == selected.getIndex()) {
          break;
        }
      }
      calculateLowerEnvelope(selected, features, indexer);
    }
  }

  private void printDetails(
      double[][] features,
      List<Task> lowerEnvelopeCandidates,
      Task selected,
      double maxDerivative,
      TaskIndexer indexer) {
    System.out.println("\n---------------------");
    System.out.println(lowerEnvelopeCandidates);
    System.out.format(
        "cost = [%s]%n",
        lowerEnvelopeCandidates.stream()
            .map(t -> Double.toString(Features.COST.get(t, indexer, features)))
            .collect(Collectors.joining(",")));
    System.out.format(
        "selectivity = [%s]%n",
        lowerEnvelopeCandidates.stream()
            .map(t -> Double.toString(getSelectivity(t, indexer, features)))
            .collect(Collectors.joining(",")));
    System.out.println("> " + selected);
    System.out.println("> " + maxDerivative);
  }

  private double getDerivative(Task left, Task right, double[][] features, TaskIndexer indexer) {
    double selectivityChange =
        TOTAL_SELECTIVITY.apply(right, indexer, features)
            - TOTAL_SELECTIVITY.apply(left, indexer, features);
    double timeChange = TIME.apply(right, indexer, features) - TIME.apply(left, indexer, features);
    return -selectivityChange / timeChange;
  }

  private Task getSource(Task task, TaskIndexer indexer, double[][] features) {
    if (Features.COMPONENT_TYPE.get(task, indexer, features) == CTYPE_SOURCE) {
      return task;
    }
    return getSource(getUpstream(task), indexer, features);
  }

  @Override
  public SingleIntraThreadSchedulingFunction enableCaching(int nTasks) {
    super.enableCaching(nTasks);
    sdop = new double[nTasks];
    for (int i = 0; i < nTasks; i++) {
      sdop[i] = NOT_INITIALIZED;
    }
    return this;
  }

  @Override
  public void clearCache() {
    super.clearCache();
    for (int i = 0; i < sdop.length; i++) {
      sdop[i] = NOT_INITIALIZED;
    }
  }

}
