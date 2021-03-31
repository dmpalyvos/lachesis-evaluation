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
import common.util.RateLimiter;
import java.util.function.Supplier;

public abstract class ConstantRateSource<T extends RichTuple>
    implements CloneableSourceFunction<T> {

  protected final Supplier<T> supplier;
  private final RateLimiter limiter;

  public ConstantRateSource(Supplier<T> supplier) {
    this.limiter = new RateLimiter();
    this.supplier = supplier;
  }

  @Override
  public T get() {
    limiter.limit(rate());
    return supplier.get();
  }

  protected abstract long rate();
}
