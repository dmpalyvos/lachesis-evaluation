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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import util.ExperimentSettings;

public class SyntheticQuerySettings extends ExperimentSettings {


  private static final double SELECTIVITY_SCALE_UNIT = 100.0;

  @Parameter(names = "--nqueries")
  private int nqueries = -1;

  @Parameter(names = "--rate")
  private int rate = -1;

  @Parameter(names = "--chainLength")
  private int chainLength = -1;

  @Parameter(names = "--varianceFactor")
  private int varianceFactor = 4;

  @Parameter(names = "--cost")
  protected int cost = -1;

  @Parameter(names = "--selectivity")
  private double selectivity = 1;

  @Parameter(names = "--operatorVariance")
  private int operatorVariance = 10;

  @Parameter(names = "--highPriority")
  private int highPriority = 1;

  @Parameter(names = "--mediumPriority")
  private int mediumPriority  = 5;

  @Parameter(names = "--lowPriority")
  private int lowPriority  = 10;

  @Parameter(names = "--rateStep")
  private int rateStep  = 100;

  @Parameter(names = "--ratePeriod")
  private int ratePeriod  = 30000;

  @Parameter(names = "--burstRate")
  private int burstRate;

  @Parameter(names = "--burstDuration")
  private int burstDuration;

  @Parameter(names = "--burstPeriod")
  private int burstPeriod;

  @Parameter(names = "--kafkaHost")
  private String kafkaHost;

  @Parameter(names = "--kafkaTopic")
  private List<String> kafkaTopics;

  public static SyntheticQuerySettings newInstance(String[] args) {
    SyntheticQuerySettings settings = new SyntheticQuerySettings();
    JCommander.newBuilder().addObject(settings).build().parse(args);
    settings.initLogLevel();
    //FIXME: Metrics
    return settings;
  }

  public int rate() {
    Validate.isTrue(rate > 0);
    return rate;
  }

  public int chainLength() {
    Validate.isTrue(chainLength > 0);
    return chainLength;
  }

  public int nqueries() {
    Validate.isTrue(nqueries > 0, "Zero nqueries!");
    return nqueries;
  }

  public int cost() {
    Validate.isTrue(cost > 0);
    return cost;
  }

  public int varianceFactor() {
    return varianceFactor;
  }

  public double selectivity() {
    return selectivity/ SELECTIVITY_SCALE_UNIT;
  }

  public int highPriority() {
    return highPriority;
  }

  public int mediumPriority() {
    return mediumPriority;
  }

  public int lowPriority() {
    return lowPriority;
  }

  public int operatorVariance() {
    return operatorVariance;
  }

  public int rateStep() {
    return rateStep;
  }

  public int ratePeriod() {
    return ratePeriod;
  }

  public int burstRate() {
    return burstRate;
  }

  public int burstDuration() {
    return burstDuration;
  }

  public int burstPeriod() {
    return burstPeriod;
  }

  public String kafkaHost() {
    return kafkaHost;
  }

  public Collection<String> kafkaTopics() {
    return kafkaTopics;
  }

  public long randomSeed() {
    Validate.isTrue(randomSeed >= 0);
    return randomSeed;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("nqueries", nqueries)
        .append("rate", rate)
        .append("chainLength", chainLength)
        .append("varianceFactor", varianceFactor)
        .append("cost", cost)
        .append("selectivity", selectivity)
        .append("operatorVariance", operatorVariance)
        .append("highPriority", highPriority)
        .append("mediumPriority", mediumPriority)
        .append("lowPriority", lowPriority)
        .append("rateStep", rateStep)
        .append("ratePeriod", ratePeriod)
        .append("burstRate", burstRate)
        .append("burstDuration", burstDuration)
        .append("burstPeriod", burstPeriod)
        .append("kafkaHost", kafkaHost)
        .append("kafkaTopics", kafkaTopics)
        .toString();
  }
}
