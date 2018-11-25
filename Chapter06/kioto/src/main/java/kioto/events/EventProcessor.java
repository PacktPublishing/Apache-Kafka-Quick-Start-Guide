package kioto.events;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

// $ kafka-topics --zookeeper localhost:2181 --create --topic aggregates --replication-factor 1 --partitions 4

public final class EventProcessor {

  private final String brokers;

  private EventProcessor(String brokers) {
    this.brokers = brokers;
  }

  private void process() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream stream = streamsBuilder.stream("events", Consumed.with(Serdes.String(), Serdes.String()));

    KTable aggregates =
        stream.groupBy( (k, v) -> "foo", Serialized.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(10000L))
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

    aggregates
        .toStream()
        .map( (ws, i) -> new KeyValue( ""+((Windowed)ws).window().start(), ""+i))
        .to("aggregates", Produced.with(Serdes.String(), Serdes.String()));

    Topology topology = streamsBuilder.build();
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("application.id", "kioto");
    props.put("auto.offset.reset", "latest");
    props.put("commit.interval.ms", 0);
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
  }

  public static void main(String[] args) {
    (new EventProcessor("localhost:9092")).process();
  }
}
