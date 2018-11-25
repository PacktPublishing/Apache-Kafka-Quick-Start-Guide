package kioto.events;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// $ kafka-topics --zookeeper localhost:2181 --create --topic events --replication-factor 1 --partitions 4

public final class EventProducer {
  private final Producer<String, String> producer;

  private EventProducer(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  private void produce() {
    long now = System.currentTimeMillis();
    long delay = 1300 - Math.floorMod(now, 1000);
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      public void run() {
        long ts = System.currentTimeMillis();
        long second = Math.floorMod(ts / 1000, 60);
        if (second != 54) {
          EventProducer.this.sendMessage(second, ts, "on time");
        }
        if (second == 6) {
          EventProducer.this.sendMessage(54, ts - 12000, "late");
        }
      }
    }, delay, 1000);
  }

  private void sendMessage(long id, long ts, String info) {
    long window = ts / 10000 * 10000;
    String value = "" + window + ',' + id + ',' + info;
    Future futureResult =
        this.producer.send(new ProducerRecord<>("events", null, ts, String.valueOf(id), value));
    try {
      futureResult.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    (new EventProducer("localhost:9092")).produce();
  }
}
