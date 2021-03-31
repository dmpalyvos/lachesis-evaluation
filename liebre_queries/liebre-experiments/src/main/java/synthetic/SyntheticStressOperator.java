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

import component.operator.in1.filter.FilterFunction;
import java.util.Random;


public class SyntheticStressOperator<T extends SyntheticTuple> implements FilterFunction<T> {
	protected final Random r = new Random();
	private final int operatorMultiplications;
	private final double operatorSelectivity;

	public SyntheticStressOperator(int cost, double operatorSelectivity) {
		this.operatorMultiplications = cost;
		this.operatorSelectivity = operatorSelectivity;
	}

	@Override
	public boolean test(T t) {
		for (int z = 0; z < operatorMultiplications; z++) {
			t.f1 *= t.f2;
		}

		if (r.nextDouble() <= operatorSelectivity)
			return true;

		return false;
	}

}
