package LinearRoad;

import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.CountStat;

public class KafkaSource extends FlinkKafkaConsumer<Tuple3<Long, String, Long>> {

  static class SourceTupleSchema implements DeserializationSchema<Tuple3<Long, String, Long>> {

    private final SimpleStringSchema delegate = new SimpleStringSchema();

    @Override
    public Tuple3<Long, String, Long> deserialize(byte[] message) throws IOException {
      final String value = delegate.deserialize(message);
      String[] parts = value.split("\\|");
      return Tuple3.of(System.currentTimeMillis(), parts[1], Long.valueOf(parts[0]));
    }

    @Override
    public boolean isEndOfStream(Tuple3<Long, String, Long> nextElement) {
      return false;
    }

    @Override
    public TypeInformation<Tuple3<Long, String, Long>> getProducedType() {
      return new TypeHint<Tuple3<Long, String, Long>>() {
      }.getTypeInfo();
    }
  }

  public KafkaSource(String topic, Properties props) {
    super(topic, new SourceTupleSchema(), props);
  }

}
