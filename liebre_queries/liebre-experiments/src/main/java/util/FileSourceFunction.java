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

import component.source.SourceFunction;
import common.util.Util;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileSourceFunction<T> implements SourceFunction<T> {

  private static final Logger LOG = LogManager.getLogger();
  private final Function<String, T> function;
  private final BufferedReader br;
  private final String id;
  private boolean done;
  private volatile boolean enabled;

  public FileSourceFunction(String id, String filename, Function<String, T> function) {
    Validate.notNull(function, "function");
    this.function = function;
    this.id = id;
    try {
      this.br = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File not found: %s", filename));
    }
  }

  @Override
  public T get() {
    if (!done) {
      return function.apply(nextLine());
    }
    LOG.debug("Text Source {} has finished processing input. Sleeping...", id);
    // If done, prevent spinning
    Util.sleep(1000);
    return null;
  }

  private String nextLine() {
    String nextLine = null;
    try {
      nextLine = br.readLine();
    } catch (IOException e) {
      LOG.warn("Text Source failed to read", e);
    }
    done = (nextLine == null);
    return nextLine;
  }

  @Override
  public void enable() {
   this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
  }

  @Override
  public double getHeadArrivalTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getAverageArrivalTime() {
    throw new UnsupportedOperationException();
  }
}
