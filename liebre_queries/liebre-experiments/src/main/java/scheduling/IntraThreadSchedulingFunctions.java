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

import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import io.palyvos.haren.function.AbstractIntraThreadSchedulingFunction;
import io.palyvos.haren.function.CachingIntraThreadSchedulingFunction;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;

public class IntraThreadSchedulingFunctions {

  private static final SingleIntraThreadSchedulingFunction TUPLE_PROCESSING_TIME = new CachingIntraThreadSchedulingFunction(
      "TUPLE_PROCESSING_TIME", Features.COST) {
    @Override
    public double applyWithCachingSupport(Task task, TaskIndexer indexer, double[][] features) {
      double totalProcessingTime = Features.COST.get(task, indexer, features);
      for (Task downstream : task.getDownstream()) {
        totalProcessingTime += apply(downstream, indexer, features);
      }
      return totalProcessingTime;
    }

    @Override
    public boolean isReverseOrder() {
      return true;
    }
  };

  private static final SingleIntraThreadSchedulingFunction GLOBAL_SELECTIVITY = new CachingIntraThreadSchedulingFunction(
      "GLOBAL_SELECTIVITY", Features.SELECTIVITY) {
    @Override
    public double applyWithCachingSupport(Task task, TaskIndexer indexer, double[][] features) {
      double globalSelectivity = Features.SELECTIVITY.get(task, indexer, features);
      for (Task downstream : task.getDownstream()) {
        globalSelectivity *= apply(downstream, indexer, features);
      }
      return globalSelectivity;
    }
  };

  private static final SingleIntraThreadSchedulingFunction HEAD_ARRIVAL_TIME = new AbstractIntraThreadSchedulingFunction(
      "HEAD_ARRIVAL_TIME", Features.HEAD_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, TaskIndexer indexer, double[][] features) {
      return Features.HEAD_ARRIVAL_TIME.get(task, indexer, features);
    }

    @Override
    public boolean isReverseOrder() {
      return true;
    }
  };

  private static final SingleIntraThreadSchedulingFunction AVERAGE_ARRIVAL_TIME = new AbstractIntraThreadSchedulingFunction(
      "AVERAGE_ARRIVAL_TIME", Features.AVERAGE_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, TaskIndexer indexer, double[][] features) {
      return Features.AVERAGE_ARRIVAL_TIME.get(task, indexer, features);
    }

    @Override
    public boolean isReverseOrder() {
      return true;
    }

  };

  private static final SingleIntraThreadSchedulingFunction SOURCE_AVERAGE_ARRIVAL_TIME =
      new CachingIntraThreadSchedulingFunction("SOURCE_AVERAGE_ARRIVAL_TIME", Features.AVERAGE_ARRIVAL_TIME,
          Features.COMPONENT_TYPE) {
        @Override
        public double applyWithCachingSupport(Task task, TaskIndexer indexer, double[][] features) {
          if (Features.COMPONENT_TYPE.get(task, indexer, features) == CTYPE_SOURCE) {
            return AVERAGE_ARRIVAL_TIME.apply(task, indexer, features);
          }
          double arrivalTime = Double.MAX_VALUE;
          for (Task upstream : task.getUpstream()) {
            arrivalTime = Math.min(arrivalTime, apply(upstream, indexer, features));
          }
          return arrivalTime;
        }
      };

  private static final SingleIntraThreadSchedulingFunction GLOBAL_AVERAGE_COST =
      new CachingIntraThreadSchedulingFunction("GLOBAL_AVERAGE_COST", Features.COST, Features.SELECTIVITY,
          Features.COMPONENT_TYPE) {

        @Override
        public double applyWithCachingSupport(Task task, TaskIndexer indexer, double[][] features) {
          double globalAverageCost = Features.COST.get(task, indexer, features);
          double selectivity = Features.SELECTIVITY.get(task, indexer, features);
          for (Task downstream : task.getDownstream()) {
            globalAverageCost += selectivity * apply(downstream, indexer, features);
          }
          return globalAverageCost;
        }

        @Override
        public boolean isReverseOrder() {
          return true;
        }
      };
  private static final SingleIntraThreadSchedulingFunction GLOBAL_RATE =
      new AbstractIntraThreadSchedulingFunction("GLOBAL_RATE", GLOBAL_SELECTIVITY, GLOBAL_AVERAGE_COST) {
        @Override
        public double apply(Task task, TaskIndexer indexer, double[][] features) {
          return GLOBAL_SELECTIVITY.apply(task, indexer, features) / GLOBAL_AVERAGE_COST
              .apply(task, indexer, features);
        }

      };
  private static final SingleIntraThreadSchedulingFunction GLOBAL_NORMALIZED_RATE =
      new AbstractIntraThreadSchedulingFunction("GLOBAL_NORMALIZED_RATE", GLOBAL_SELECTIVITY,
          GLOBAL_AVERAGE_COST, TUPLE_PROCESSING_TIME) {
        @Override
        public double apply(Task task, TaskIndexer indexer, double[][] features) {
          return GLOBAL_SELECTIVITY.apply(task, indexer, features) / (GLOBAL_AVERAGE_COST.apply(task,
              indexer, features)
              * TUPLE_PROCESSING_TIME.apply(task, indexer, features));
        }
      };

  private static final SingleIntraThreadSchedulingFunction USER_PRIORITY =
      new AbstractIntraThreadSchedulingFunction("USER_PRIORITY", Features.USER_PRIORITY) {
        @Override
        public double apply(Task task, TaskIndexer indexer, double[][] features) {
          return Features.USER_PRIORITY.get(task, indexer, features);
        }
      };

  private static final SingleIntraThreadSchedulingFunction INPUT_QUEUE_SIZE = new AbstractIntraThreadSchedulingFunction(
      "INPUT_QUEUE_SIZE", Features.INPUT_QUEUE_SIZE) {
    @Override
    public double apply(Task task, TaskIndexer indexer, double[][] features) {
      return Features.INPUT_QUEUE_SIZE.get(task, indexer, features);
    }
  };

  private static final SingleIntraThreadSchedulingFunction OUTPUT_QUEUE_SIZE = new AbstractIntraThreadSchedulingFunction(
      "OUTPUT_QUEUE_SIZE", Features.OUTPUT_QUEUE_SIZE) {
    @Override
    public double apply(Task task, TaskIndexer indexer, double[][] features) {
      return Features.OUTPUT_QUEUE_SIZE.get(task, indexer, features);
    }

    @Override
    public boolean isReverseOrder() {
      return true;
    }
  };


  private IntraThreadSchedulingFunctions() {

  }

  public static SingleIntraThreadSchedulingFunction averageArrivalTime() {
    return AVERAGE_ARRIVAL_TIME;
  }

  public static SingleIntraThreadSchedulingFunction headArrivalTime() {
    return HEAD_ARRIVAL_TIME;
  }

  public static SingleIntraThreadSchedulingFunction globalRate() {
    return GLOBAL_RATE;
  }

  public static SingleIntraThreadSchedulingFunction globalNormalizedRate() {
    return GLOBAL_NORMALIZED_RATE;
  }

  public static SingleIntraThreadSchedulingFunction tupleProcessingTime() {
    return TUPLE_PROCESSING_TIME;
  }

  public static SingleIntraThreadSchedulingFunction userPriority() {
    return USER_PRIORITY;
  }

  public static SingleIntraThreadSchedulingFunction inputQueueSize() {
    return INPUT_QUEUE_SIZE;
  }

  public static SingleIntraThreadSchedulingFunction outputQueueSize() {
    return OUTPUT_QUEUE_SIZE;
  }

  public static SingleIntraThreadSchedulingFunction chain() {
    return new ChainIntraThreadSchedulingFunction();
  }

  public static SingleIntraThreadSchedulingFunction sourceAverageArrivalTime() {
    return SOURCE_AVERAGE_ARRIVAL_TIME;
  }



}
