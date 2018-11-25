package monedero;

import java.util.Properties;

public interface Consumer {
  static Properties createConfig(String servers, String groupId) {
    Properties config = new Properties();
    config.put("bootstrap.servers", servers);
    config.put("group.id", groupId);
    config.put("enable.auto.commit", "true");
    config.put("auto.commit.interval.ms", "1000");
    config.put("auto.offset.reset", "earliest");
    config.put("session.timeout.ms", "30000");
    config.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    return config;
  }
}
