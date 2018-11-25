package kioto.custom;

import kioto.Constants;
import kioto.HealthCheck;
import kioto.serde.HealthCheckDeserializer;
import kioto.serde.HealthCheckSerializer;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

// $ kafka-topics --zookeeper localhost:2181 --create --topic uptimes --replication-factor 1 --partitions 4

public final class CustomStreamsProcessor {

  private final String brokers;

  public CustomStreamsProcessor(String brokers) {
    super();
    this.brokers = brokers;
  }

  public final void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    Serde customSerde = Serdes.serdeFrom(
        new HealthCheckSerializer(), new HealthCheckDeserializer());

    KStream healthCheckStream = streamsBuilder.stream(
        Constants.getHealthChecksTopic(), Consumed.with(Serdes.String(), customSerde));

    KStream uptimeStream = healthCheckStream.map(((KeyValueMapper) (k, v) -> {
      HealthCheck healthCheck = (HealthCheck) v;
      LocalDate startDateLocal =
          healthCheck.getLastStartedAt().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      int uptime = Period.between(startDateLocal, LocalDate.now()).getDays();
      return new KeyValue<>(healthCheck.getSerialNumber(), String.valueOf(uptime));
    }));

    uptimeStream.to(Constants.getUptimesTopic(), Produced.with(Serdes.String(), Serdes.String()));
    Topology topology = streamsBuilder.build();
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("application.id", "kioto");
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
  }

  public static void main(String[] args) {
    (new CustomStreamsProcessor("localhost:9092")).process();
  }
}
