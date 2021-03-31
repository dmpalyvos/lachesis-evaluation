package source;

import java.util.Properties;
import java.util.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaFileSource {

  public static void main(String[] args) {
    Config config = Config.parse(args);
    Timer sourceTimer = new Timer("SourceTimer");
    Properties kafkaProperties = new Properties();
    kafkaProperties.put("bootstrap.servers", "localhost:9092");
    kafkaProperties.put("key.serializer", StringSerializer.class);
    kafkaProperties.put("value.serializer", StringSerializer.class);
    KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
    sourceTimer.scheduleAtFixedRate(new SourceTimerTask(config, producer), 0, config.period);
  }




}
