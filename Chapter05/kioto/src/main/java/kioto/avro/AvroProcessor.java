package kioto.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import kioto.Constants;
import kioto.HealthCheck;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public final class AvroProcessor {
  private Consumer<String, GenericRecord> consumer;
  private Producer<String, String> producer;

  public AvroProcessor(String brokers, String schemaRegistryUrl) {
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", brokers);
    consumerProps.put("group.id", "healthcheck-processor");
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", KafkaAvroDeserializer.class);
    consumerProps.put("schema.registry.url", schemaRegistryUrl);
    consumer = new KafkaConsumer<>(consumerProps);

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", brokers);
    producerProps.put("key.serializer", StringSerializer.class);
    producerProps.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(producerProps);
  }

  public final void process() {
    consumer.subscribe(Collections.singletonList(Constants.getHealthChecksAvroTopic()));

    while (true) {
      ConsumerRecords records = consumer.poll(Duration.ofSeconds(1L));

      for (Object record : records) {
        ConsumerRecord it = (ConsumerRecord) record;
        GenericRecord healthCheckAvro = (GenericRecord) it.value();
        HealthCheck healthCheck = new HealthCheck(
            healthCheckAvro.get("event").toString(),
            healthCheckAvro.get("factory").toString(),
            healthCheckAvro.get("serialNumber").toString(),
            healthCheckAvro.get("type").toString(),
            healthCheckAvro.get("status").toString(),
            new Date((Long) healthCheckAvro.get("lastStartedAt")),
            Float.parseFloat(healthCheckAvro.get("temperature").toString()),
            healthCheckAvro.get("ipAddress").toString());
        LocalDate startDateLocal = healthCheck.getLastStartedAt().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int uptime = Period.between(startDateLocal, LocalDate.now()).getYears();
        Future future = producer.send(
            new ProducerRecord<>(Constants.getUptimesTopic(),
                healthCheck.getSerialNumber(), String.valueOf(uptime)));
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) {
    new AvroProcessor("localhost:9092", "http://localhost:8081").process();
  }
}
