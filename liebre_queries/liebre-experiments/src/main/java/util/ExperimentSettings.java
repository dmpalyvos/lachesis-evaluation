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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.haren.HarenScheduler;
import io.palyvos.haren.function.InterThreadSchedulingFunction;
import io.palyvos.haren.function.IntraThreadSchedulingFunction;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.util.Strings;
import queries.synthetic.MultiClassInterThreadSchedulingFunction;
import queries.synthetic.MultiClassSchedulingFunction;
import scheduling.ChainArrivalTimeFunction;
import scheduling.HighestRateFunction;
import scheduling.InterThreadSchedulingFunctions;
import scheduling.IntraThreadSchedulingFunctions;
import scheduling.LiebreScheduler;
import scheduling.basic.BasicLiebreScheduler;
import scheduling.haren.HarenLiebreSchedulerAdapter;

public class ExperimentSettings {

  private static final Logger LOG = LogManager.getLogger();



  @Parameter(names = "--randomSeed")
  protected long randomSeed = -1;

  @Parameter(names = "--experimentName")
  private String experimentName;

  @Parameter(names = "--logLevel")
  private String logLevel;

  @Parameter(names = "--schedulingPeriodMillis")
  private int schedulingPeriodMillis = 100;

  @Parameter(names = "--batchSize")
  private int batchSize = 10;

  @Parameter(names = "--priorityFunction", converter = IntraThreadSchedulingFunctionConverter.class)
  private VectorIntraThreadSchedulingFunction priorityFunction;

  @Parameter(
      names = "--deploymentFunction",
      converter = InterThreadSchedulingFunctionConverter.class)
  private InterThreadSchedulingFunction deploymentFunction;

  @Parameter(names = "--priorityCaching")
  private boolean priorityCaching;

  @Parameter(names = "--priorityIntervalNanos")
  private long priorityIntervalNanos = -1;

  @Parameter(names = "--scheduling")
  private boolean scheduling;

  @Parameter(names = "--maxThreads")
  private int maxThreads;

  @Parameter(names = "--parallelism")
  private int parallelism = 1;

  @Parameter(names = "--autoFlush")
  private boolean autoFlush;

  @Parameter(names = "--statisticsFolder")
  private String statisticsFolder;

  @Parameter(names = "--inputFiles", converter = InputFileConverter.class)
  private Map<String, String> inputFiles;

  @Parameter(names = "--inputFolder")
  private String inputFolder;

  @Parameter(names = "--durationSeconds", required = true)
  private double durationSeconds;

  @Parameter(names = "--workerAffinity", required = true, converter = BitSetConverter.class)
  private BitSet workerAffinity;

  @Parameter(names = "--sourceAffinity", required = true, converter = BitSetConverter.class)
  private BitSet sourceAffinity;

  @Parameter(names = "--utilizations", converter = ExecutionPercentageConverter.class)
  private Map<String, Double> utilizations = Collections.emptyMap();

  @Parameter(names = "--statisticsHost")
  private String statisticsHost = "localhost";

  private LiebreScheduler scheduler;

  public static ExperimentSettings newInstance(String[] args) {
    ExperimentSettings settings = new ExperimentSettings();
    JCommander.newBuilder().addObject(settings).build().parse(args);
    settings.initLogLevel();
    return settings;
  }

  protected void initLogLevel() {
    if (Strings.isBlank(logLevel)) {
      return;
    }
    Configurator.setRootLevel(Level.getLevel(logLevel.toUpperCase()));
  }

  public VectorIntraThreadSchedulingFunction priorityFunction() {
    return priorityFunction;
  }

  public synchronized LiebreScheduler scheduler() {
    if (scheduler == null) {
      if (!scheduling) {
        scheduler = new BasicLiebreScheduler(workerAffinity);
      } else {
        Validate.isTrue(schedulingPeriodMillis > 0, "maxTimeMillis <= 0");
        Validate.isTrue(maxThreads > 0, "maxThreads <= 0");
        Validate.notNull(priorityFunction, "No priority function given!");
        Validate.notNull(deploymentFunction, "No deployment function given!");
        HarenScheduler haren =
            new HarenScheduler(
                maxThreads,
                priorityFunction,
                deploymentFunction,
                priorityCaching,
                batchSize,
                schedulingPeriodMillis,
                workerAffinity);
        scheduler = new HarenLiebreSchedulerAdapter(haren);
      }
    }
    return scheduler;
  }

  public String statisticsFolder() {
    Validate.notBlank(statisticsFolder);
    return statisticsFolder;
  }

  public String experimentName() {
    return experimentName;
  }

  public String inputFile(String sourceId) {
    Validate.isTrue(!inputFiles.isEmpty());
    Validate.notBlank(inputFolder);
    List<String> matchingKeys =
        inputFiles.keySet().stream()
            .filter(k -> sourceId.toUpperCase().contains(k.toUpperCase()))
            .collect(Collectors.toList());
    Validate.isTrue(
        matchingKeys.size() > 0, String.format("No matching input file for %s", sourceId));
    Validate.isTrue(
        matchingKeys.size() == 1, String.format("More than one input file for %s", sourceId));
    String inputFile = inputFiles.get(matchingKeys.get(0));
    return inputFolder + File.separator + inputFile;
  }

  public String memoryFile() {
    Validate.notBlank(statisticsFolder);
    return statisticsFolder + File.separator + "memory.csv";
  }

  public boolean autoFlush() {
    return autoFlush;
  }

  public long durationSeconds() {
    Validate.isTrue(durationSeconds > 0);
    return Math.round(durationSeconds);
  }

  public long durationMillis() {
    return TimeUnit.SECONDS.toMillis(durationSeconds());
  }

  public int parallelism() {
    Validate.isTrue(parallelism > 0);
    return parallelism;
  }

  public boolean scheduling() {
    return scheduling;
  }

  public BitSet sourceAffinity() {
    return sourceAffinity;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("experimentName", experimentName)
        .append("randomSeed", randomSeed)
        .append("logLevel", logLevel)
        .append("schedulingPeriodMillis", schedulingPeriodMillis)
        .append("batchSize", batchSize)
        .append("priorityFunction", priorityFunction)
        .append("deploymentFunction", deploymentFunction)
        .append("priorityCaching", priorityCaching)
        .append("priorityIntervalNanos", priorityIntervalNanos)
        .append("haren", scheduling)
        .append("maxThreads", maxThreads)
        .append("parallelism", parallelism)
        .append("autoFlush", autoFlush)
        .append("statisticsFolder", statisticsFolder)
        .append("statisticsHost", statisticsHost)
        .append("inputFiles", inputFiles)
        .append("inputFolder", inputFolder)
        .append("durationSeconds", durationSeconds)
        .append("workerAffinity", workerAffinity)
        .append("sourceAffinity", sourceAffinity)
        .append("utilizations", utilizations)
        .toString();
  }

  public String statisticsHost() {
    return statisticsHost;
  }

  public String queryDAGFile() {
    return statisticsFolder + File.separator + "query_dag.yaml";
  }

  private static class IntraThreadSchedulingFunctionConverter
      implements IStringConverter<IntraThreadSchedulingFunction> {

    private static final IntraThreadSchedulingFunction[] AVAILABLE_FUNCTIONS = {
      IntraThreadSchedulingFunctions.tupleProcessingTime(),
      IntraThreadSchedulingFunctions.globalRate(),
      IntraThreadSchedulingFunctions.globalNormalizedRate(),
      IntraThreadSchedulingFunctions.headArrivalTime(),
      IntraThreadSchedulingFunctions.averageArrivalTime(),
      IntraThreadSchedulingFunctions.userPriority(),
      IntraThreadSchedulingFunctions.chain(),
      IntraThreadSchedulingFunctions.inputQueueSize(),
      IntraThreadSchedulingFunctions.outputQueueSize(),
      new HighestRateFunction(),
      new ChainArrivalTimeFunction(),
      new MultiClassSchedulingFunction(),
    };

    @Override
    public VectorIntraThreadSchedulingFunction convert(String value) {
      final String[] functionNames = value.split(",");
      if (functionNames.length == 1) {
        try {
          return getFunction(functionNames[0], VectorIntraThreadSchedulingFunction.class);
        } catch (Exception e) {
        }
      }
      final List<SingleIntraThreadSchedulingFunction> result = new ArrayList<>();
      for (String functionName : functionNames) {
        result.add(getFunction(functionName, SingleIntraThreadSchedulingFunction.class));
      }
      return new VectorIntraThreadSchedulingFunctionImpl(
          result.toArray(new SingleIntraThreadSchedulingFunction[0]));
    }

    private <T> T getFunction(String functionName, Class<T> clazz) {
      for (IntraThreadSchedulingFunction function : AVAILABLE_FUNCTIONS) {
        if (function.name().equals(functionName)) {
          return clazz.cast(function);
        }
      }
      throw new IllegalArgumentException("Unknown function: " + functionName);
    }
  }

  private static class InterThreadSchedulingFunctionConverter
      implements IStringConverter<InterThreadSchedulingFunction> {

    private static final InterThreadSchedulingFunction[] AVAILABLE_FUNCTIONS = {
      InterThreadSchedulingFunctions.roundRobinQuery(),
      InterThreadSchedulingFunctions.adaptiveLatency(),
      InterThreadSchedulingFunctions.randomOperator(),
      new MultiClassInterThreadSchedulingFunction()
    };

    @Override
    public InterThreadSchedulingFunction convert(String value) {
      for (InterThreadSchedulingFunction function : AVAILABLE_FUNCTIONS) {
        if (function.name().equals(value)) {
          return function;
        }
      }
      throw new IllegalArgumentException("Unknown function: " + value);
    }
  }

  private static class StatsTypeConverter implements IStringConverter<List<StatisticType>> {

    @Override
    public List<StatisticType> convert(String rawTypes) {
      String[] types = rawTypes.split(",");
      List<StatisticType> converted = new ArrayList<>();
      for (String type : types) {
        converted.add(StatisticType.valueOf(type.toUpperCase()));
      }
      return converted;
    }
  }

  private static class ExecutionPercentageConverter
      implements IStringConverter<Map<String, Double>> {

    @Override
    public Map<String, Double> convert(String value) {
      String[] percentages = value.split(",");
      Map<String, Double> result = new HashMap<>();
      for (String group : percentages) {
        String[] groupParts = group.split(":");
        Validate.isTrue(groupParts.length == 2);
        result.put(groupParts[0], Double.valueOf(groupParts[1]));
      }
      return result;
    }
  }

  private static class InputFileConverter implements IStringConverter<Map<String, String>> {

    @Override
    public Map<String, String> convert(String value) {
      String[] inputFiles = value.split(",");
      Map<String, String> result = new HashMap<>();
      for (String group : inputFiles) {
        String[] groupParts = group.split(":");
        Validate.isTrue(groupParts.length == 2);
        result.put(groupParts[0], groupParts[1]);
      }
      return result;
    }
  }

  private static class BitSetConverter implements IStringConverter<BitSet> {

    @Override
    public BitSet convert(String input) {
      String[] values = input.split(",");
      final BitSet result = new BitSet();
      for (String value : values) {
        result.set(Integer.valueOf(value));
      }
      return result;
    }
  }
}
