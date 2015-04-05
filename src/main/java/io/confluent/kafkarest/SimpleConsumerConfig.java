package io.confluent.kafkarest;

import kafka.consumer.ConsumerConfig;

import java.util.Properties;

public class SimpleConsumerConfig {

  final ConsumerConfig consumerConfig;

  public SimpleConsumerConfig(final Properties originalProperties) {
    final Properties props = (Properties) originalProperties.clone();
    // ConsumerConfig is intended to be used with the HighLevelConsumer. Therefore, it requires some properties
    // to be instantiated that are useless for a SimpleConsumer.
    // We use ConsumerConfig as a basis for SimpleConsumerConfig, because it contains
    // sensible defaults (buffer size, ...).
    props.setProperty("zookeeper.connect", "");
    props.setProperty("group.id", "");
    consumerConfig = new ConsumerConfig(props);
  }

  public int socketTimeoutMs() {
    return consumerConfig.socketTimeoutMs();
  }

  public int socketReceiveBufferBytes() {
    return consumerConfig.socketReceiveBufferBytes();
  }

  public int fetchMessageMaxBytes() {
    return consumerConfig.fetchMessageMaxBytes();
  }

  public int fetchWaitMaxMs() {
    return consumerConfig.fetchWaitMaxMs();
  }

  public int fetchMinBytes() {
    return consumerConfig.fetchMinBytes();
  }
}
