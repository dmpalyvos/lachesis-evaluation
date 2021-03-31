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

import query.Query;

public abstract class AbstractQueryFactory {

  protected final String queryName;
  protected volatile int queryIndex = 0;

  public AbstractQueryFactory(String queryName) {
    this.queryName = queryName;
  }

  public void generateQueries(Query q, int repetitions) {
    for (int i = 0; i < repetitions; i++) {
      addQueryInstance(q);
      queryIndex++;
    }
  }

  public void onBeforeQueryExecution() {

  }

  public void onAfterQueryExecution() {

  }

  public String name(String componentName) {
    return name(componentName, queryName, queryIndex);
  }

  public static String name(String componentName, String customQueryName, int customQueryIndex) {
    return String.format("%s-%d-%s", customQueryName, customQueryIndex, componentName);
  }

  protected abstract void addQueryInstance(Query q);

}
