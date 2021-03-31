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

import common.Active;
import io.palyvos.haren.Task;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import queries.synthetic.queries.ElasticMultiClass;
import query.Query;
import synthetic.SyntheticTuple;
import util.DataSource;

public class ElasticSyntheticQueryFactory<T extends SyntheticTuple>
    extends SyntheticQueryFactory<T> {

  private final ElasticityTester elasticityTester = new ElasticityTester();
  private final Map<Integer, List<Task>> tasks = new HashMap<>();
  private final Map<Integer, List<Task>> disabledTasks = new HashMap<>();
  private volatile boolean executing;

  public ElasticSyntheticQueryFactory(
      SyntheticQuerySettings settings, String name, DataSource<T> dataSource, int priority) {
    super(settings, name, dataSource, priority);
  }

  @Override
  public void generateQueries(Query q, int repetitions) {
    super.generateQueries(q, repetitions);
    final String id = priority == ElasticMultiClass.HIGH_PRIORITY ? "HIGH" : "LOW";
    elasticityTester.run(
        settings,
        q,
        id,
        (Consumer<Query>) (Query x) -> elasticAddQueryInstance(x),
        (Consumer<Query>) (x) -> elasticRemoveQueryInstance(x));
  }

  protected void elasticAddQueryInstance(Query q) {
    executing = true;
    if (disabledTasks.isEmpty()) {
      super.addQueryInstance(q);
      queryIndex++;
    } else {
      Map.Entry<Integer, List<Task>> entry = removeRandomMapEntry(disabledTasks);
      int index = entry.getKey();
      List<Task> query = entry.getValue();
      settings.scheduler().addTasks(query);
      dataSource.enableQueue(index);
      LOG.info("Enabled query {}", index);
    }
  }

  private void elasticRemoveQueryInstance(Query q) {
    executing = true;
    Map.Entry<Integer, List<Task>> entry = removeRandomMapEntry(tasks);
    int index = entry.getKey();
    List<Task> query = entry.getValue();
    query.stream().forEach(t -> ((Active) t).disable());
    settings.scheduler().removeTasks(query);
    dataSource.disableQueue(index);
    disabledTasks.put(index, query);
    LOG.info("Disabled query {}", index);
  }

  @Override
  protected void onTasksAdded(List<Task> addedTasks) {
    super.onTasksAdded(addedTasks);
    LOG.info("Adding tasks {}", addedTasks);
    this.tasks.put(queryIndex, addedTasks);
    if (executing) {
      // If the app is executing, the query will not add the tasks itself
      // to the scheduler, so we do it manually
      settings.scheduler().addTasks(addedTasks);
    }
  }

  private <K, V> Map.Entry<K, V> removeRandomMapEntry(Map<K, V> map) {
    int randomIndex = ThreadLocalRandom.current().nextInt(map.size());
    int i = 0;
    Iterator<Entry<K, V>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Entry<K, V> entry = it.next();
      if (i++ == randomIndex) {
        it.remove();
        return entry;
      }
    }
    // Should never reach this point
    throw new IllegalStateException("Reached end of map!");
  }
}
