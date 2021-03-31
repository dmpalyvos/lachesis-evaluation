package util;

import common.tuple.RichTuple;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import queries.synthetic.SyntheticQuerySettings;
import synthetic.CloneableSourceFunction;
import synthetic.SyntheticTuple;

/**
 * Completely convoluted kafka consumer implementation to keep API compatibility with the current
 * infrastructure.
 */
public class KafkaSourceFunction<T extends SyntheticTuple> implements CloneableSourceFunction<T> {

  public static final Duration POLL_WAIT = Duration.of(100, ChronoUnit.MILLIS);
  private final KafkaConsumer<String, String> consumer;
  private final Supplier<T> supplier;
  private final SyntheticQuerySettings settings;
  private DataSource<T> dataSource;

  public KafkaSourceFunction(Supplier<T> supplier, SyntheticQuerySettings settings) {
    Validate.notNull(supplier, "supplier");
    Validate.notNull(settings, "Settings");
    this.supplier = supplier;
    this.settings = settings;
    Properties props = new Properties();
    props.put("group.id", UUID.randomUUID().toString());
    props.put("bootstrap.servers", String.format("%s:9092", settings.kafkaHost()));
    props.put("key.deserializer", StringDeserializer.class);
    props.put("value.deserializer", StringDeserializer.class);
    consumer = new KafkaConsumer<>(props);
  }

  @Override
  public void enable() {
    consumer.subscribe(settings.kafkaTopics());
  }

  @Override
  public void disable() {
  }

  @Override
  public CloneableSourceFunction<T> copy() {
    return new KafkaSourceFunction<>(supplier, settings);
  }

  @Override
  public void setParent(DataSource<T> datasource) {
    this.dataSource = datasource;
  }

  @Override
  public T get() {
    for (ConsumerRecord<String, String> record : consumer.poll(POLL_WAIT)) {
      T tuple = supplier.get();
      if (tuple == null) {
        continue;
      }
      tuple.setTimestamp(Long.valueOf(record.value().split("\\|")[0]));
      dataSource.emit(tuple);
    }
    return null;
  }
}
