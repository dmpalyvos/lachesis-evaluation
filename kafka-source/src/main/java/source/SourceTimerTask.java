package source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceTimerTask extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceTimerTask.class);
  private static final double ONE_SECOND_NANOS = 1e9;
  private static final String RATE_METRIC_KEY_PATTERN = "kafka.%s.external-rate.value";
  private final Config config;
  private final List<String> data = new ArrayList<>();
  private final int batchSize;
  private final KafkaProducer<String, String> producer;
  private final SimpleGraphiteReporter rateReporter;

  private final String rateMetricKey;
  private int index;
  private long generated = 0;
  private long lastSample = 0;

  // Constructor
  SourceTimerTask(Config config, KafkaProducer<String, String> producer) {
    this.config = config;
    this.batchSize = config.rate / (1000 / config.period);
    this.index = config.offset;
    this.producer = producer;
    this.rateReporter = new SimpleGraphiteReporter(config.graphiteHost, config.graphitePort);
    this.rateMetricKey = String.format(RATE_METRIC_KEY_PATTERN, config.topic);
    loadData();
  }

  private void loadData() {
    if (config.hasDummyInputFile()) {
      loadDummyDataset();
      return;
    }
    loadWholeDataset();
  }

  @Override
  public void run() {
    final long nowNanos = System.nanoTime();
    if (generated == 0) {
      this.lastSample = nowNanos;
    }
    long elapsed = nowNanos - this.lastSample;
    if (elapsed >= ONE_SECOND_NANOS) {
      double rate = (long) (generated / (elapsed / ONE_SECOND_NANOS)); // per second
      rateReporter.open();
      rateReporter
          .report(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), rateMetricKey, rate);
      rateReporter.close();
      LOG.debug("Rate = {}", rate);
      generated = 0;
      this.lastSample = nowNanos;
    }
    // Generate the batch
    for (int i = 0; i < batchSize; i++) {
      // fetch the next item
      if (index >= data.size()) {
        index = config.offset;
      }
      String line = data.get(index); // get the next tuple
      long timestamp = System.currentTimeMillis(); // put the timestamp
      String event = timestamp + "|" + line;
      producer.send(new ProducerRecord<>(config.topic, event));
      generated++;
      index += config.stride;
    }
  }

  private void loadWholeDataset() {
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(config.inputFile))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        data.add(line);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void loadDummyDataset() {
    for (int i = 0; i < 100000; i++) {
      data.add(String.valueOf(i));
    }
  }
}
