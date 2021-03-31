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

package synthetic;

import common.tuple.RichTuple;
import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import scheduling.haren.HarenFeatureTranslator;

/**
 * Dummy tuple which only has a timestamp.
 *
 * @author palivosd
 */
public class SyntheticTuple implements RichTuple, Serializable {

  long ts;
  long stimulus = -1;
  public double f1 = Math.PI;
  public int f2;
  public int f3;

  public SyntheticTuple() {
    this.ts = System.currentTimeMillis();
    this.f2 = ThreadLocalRandom.current().nextInt(1000);
  }

  public String getKey() {
    return null;
  }

  public long getTimestamp() {
    return ts;
  }

  public void setTimestamp(long ts) {
    this.ts = ts;
  }

  @Override
  public long getStimulus() {
    return stimulus;
  }

  @Override
  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }
}
