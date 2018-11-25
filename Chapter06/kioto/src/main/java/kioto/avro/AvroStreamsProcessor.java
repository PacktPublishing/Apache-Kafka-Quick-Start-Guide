package kioto.avro;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import kioto.Constants;
import kioto.HealthCheck;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
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

public final class AvroStreamsProcessor {

  private final String brokers;
  private final String schemaRegistryUrl;

  public AvroStreamsProcessor(String brokers, String schemaRegistryUrl) {
    super();
    this.brokers = brokers;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  public final void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    GenericAvroSerde avroSerde = new GenericAvroSerde();

    avroSerde.configure(
        Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);

    KStream avroStream = streamsBuilder.stream(
        Constants.getHealthChecksAvroTopic(), Consumed.with(Serdes.String(), avroSerde));

    KStream healthCheckStream = avroStream.mapValues((v -> {
      GenericRecord healthCheckAvro = (GenericRecord) v;
      HealthCheck healthCheck = new HealthCheck(
          healthCheckAvro.get("event").toString(),
          healthCheckAvro.get("factory").toString(),
          healthCheckAvro.get("serialNumber").toString(),
          healthCheckAvro.get("type").toString(),
          healthCheckAvro.get("status").toString(),
          new Date((Long) healthCheckAvro.get("lastStartedAt")),
          Float.parseFloat(healthCheckAvro.get("temperature").toString()),
          healthCheckAvro.get("ipAddress").toString());
      return healthCheck;
    }));

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
    (new AvroStreamsProcessor("localhost:9092", "http://localhost:8081")).process();
  }
}
