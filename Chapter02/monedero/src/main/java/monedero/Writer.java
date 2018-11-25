package monedero;

import org.apache.kafka.clients.producer.KafkaProducer;

public class Writer implements Producer {

  private final KafkaProducer<String, String> producer;
  private final String topic;

  Writer(String servers, String topic) {
    this.producer = new KafkaProducer<>(
        Producer.createConfig(servers));                    //1
    this.topic = topic;
  }

  @Override
  public void process(String message) {
    Producer.write(this.producer, this.topic, message);    //2
  }
}
