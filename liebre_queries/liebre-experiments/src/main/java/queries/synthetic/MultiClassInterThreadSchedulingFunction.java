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

package queries.synthetic;

import io.palyvos.haren.Features;
import io.palyvos.haren.function.AbstractInterThreadSchedulingFunction;
import io.palyvos.haren.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import queries.synthetic.queries.MultiClass;

public class MultiClassInterThreadSchedulingFunction extends
    AbstractInterThreadSchedulingFunction {

  private final Random random = new Random(1);

  public MultiClassInterThreadSchedulingFunction() {
    super("MULTI_CLASS", Features.COST, Features.RATE, Features.USER_PRIORITY);
  }

  @Override
  public List<List<Task>> getAssignment(int nThreads) {
    List<List<Task>> deployment = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      deployment.add(new ArrayList<>());
    }
    for (Task task : tasks) {
      final double priority = Features.USER_PRIORITY.get(task, indexer, features);
      if (priority == MultiClass.LOW_PRIORITY) {
        deployment.get(random.nextInt(nThreads));
      } else {
        final double load = Features.COST.get(task, indexer, features) * Features.RATE.get(task,
            indexer, features);
        if (load < 0.2) {
          deployment.get(random.nextInt(nThreads));
        }
        distributeLoad(deployment, task, priority, nThreads);
      }
    }
    return deployment;
  }

  private void distributeLoad(List<List<Task>> deployment, Task task, double priority,
      int nThreads) {
    throw new UnsupportedOperationException();
  }


  private final int nextInt(int from, int to) {
    return from + random.nextInt(to - from + 1);
  }
}
