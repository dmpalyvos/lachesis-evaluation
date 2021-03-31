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

public class LiveSettings {

  public volatile long schedulingPeriodMillis;
  public volatile int batchSize;
  public volatile int rate;
  public volatile int baseRate;
  public volatile int burstRate;
  public volatile long burstDurationMillis;
  public volatile long burstPeriodMillis;
  public volatile String intraThreadFunctionKey;

  public void copyFieldsFrom(LiveSettings other) {
    this.schedulingPeriodMillis = other.schedulingPeriodMillis;
    this.batchSize = other.batchSize;
    this.rate = other.rate;
    this.baseRate = other.baseRate;
    this.burstRate = other.burstRate;
    this.burstDurationMillis = other.burstDurationMillis;
    this.burstPeriodMillis = other.burstPeriodMillis;
    this.intraThreadFunctionKey = other.intraThreadFunctionKey;
  }

  @Override
  public String toString() {
    return "LiveSettings{" +
        "schedulingPeriodMillis=" + schedulingPeriodMillis +
        ", batchSize=" + batchSize +
        ", rate=" + rate +
        ", baseRate=" + baseRate +
        ", burstRate=" + burstRate +
        ", burstDurationMillis=" + burstDurationMillis +
        ", burstPeriodMillis=" + burstPeriodMillis +
        ", intraThreadSchedulingFunction='" + intraThreadFunctionKey + '\'' +
        '}';
  }

}
