package VoipStream;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.Func;

public class KafkaRecordTranslator implements
    Func<ConsumerRecord<String, String>, List<Object>> {


  @Override
  public List<Object> apply(ConsumerRecord<String, String> record) {
    String[] parts = record.value().split("\\|");
    return Arrays.asList(System.currentTimeMillis(), Long.valueOf(parts[0]), parts[1]);
  }
}
