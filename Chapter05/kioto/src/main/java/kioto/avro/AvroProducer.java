package kioto.avro;

import com.github.javafaker.Faker;
import kioto.Constants;
import kioto.HealthCheck;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class AvroProducer {

  private final Producer<String, GenericRecord> producer;
  private Schema schema;

  public AvroProducer(String brokers, String schemaRegistryUrl) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    producer = new KafkaProducer<>(props);
    try {
      schema = (new Parser()).parse(new File("src/main/resources/healthcheck.avsc"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public final void produce(int ratePerSecond) {
    long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond;
    Faker faker = new Faker();

    while(true) {
      HealthCheck fakeHealthCheck =
          new HealthCheck(
              "HEALTH_CHECK",
              faker.address().city(),
              faker.bothify("??##-??##", true),
              Constants.machineType.values()[faker.number().numberBetween(0,4)].toString(),
              Constants.machineStatus.values()[faker.number().numberBetween(0,3)].toString(),
              faker.date().past(100, TimeUnit.DAYS),
              faker.number().numberBetween(100L, 0L),
              faker.internet().ipV4Address());
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
      recordBuilder.set("event", fakeHealthCheck.getEvent());
      recordBuilder.set("factory", fakeHealthCheck.getFactory());
      recordBuilder.set("serialNumber", fakeHealthCheck.getSerialNumber());
      recordBuilder.set("type", fakeHealthCheck.getType());
      recordBuilder.set("status", fakeHealthCheck.getStatus());
      recordBuilder.set("lastStartedAt", fakeHealthCheck.getLastStartedAt().getTime());
      recordBuilder.set("temperature", fakeHealthCheck.getTemperature());
      recordBuilder.set("ipAddress", fakeHealthCheck.getIpAddress());
      Record avroHealthCheck = recordBuilder.build();
      Future futureResult = producer.send(new ProducerRecord<>(Constants.getHealthChecksAvroTopic(), avroHealthCheck));
      try {
        Thread.sleep(waitTimeBetweenIterationsMs);
        futureResult.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main( String[] args) {
    new AvroProducer("localhost:9092", "http://localhost:8081").produce(2);
  }
}