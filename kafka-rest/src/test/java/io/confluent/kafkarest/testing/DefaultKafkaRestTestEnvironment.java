package io.confluent.kafkarest.testing;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.rules.ExternalResource;

public final class DefaultKafkaRestTestEnvironment extends ExternalResource {

  private final SslFixture certificates =
      SslFixture.builder()
          .addKey("kafka-1")
          .addKey("kafka-2")
          .addKey("kafka-3")
          .addKey("schema-registry")
          .addKey("kafka-rest")
          .build();

  private final ZookeeperFixture zookeeper = ZookeeperFixture.create();

  private final KafkaClusterFixture kafkaCluster =
      KafkaClusterFixture.builder()
          .addUser("kafka-rest", "kafka-rest-pass")
          .addUser("schema-registry", "schema-registry-pass")
          .addSuperUser("kafka-rest")
          .addSuperUser("schema-registry")
          .setCertificates(certificates, "kafka-1", "kafka-2", "kafka-3")
          .setConfig("ssl.client.auth", "required")
          .setNumBrokers(3)
          .setSecurityProtocol(SecurityProtocol.SASL_SSL)
          .setZookeeper(zookeeper)
          .build();

  private final SchemaRegistryFixture schemaRegistry =
      SchemaRegistryFixture.builder()
          .setCertificates(certificates, "schema-registry")
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("schema-registry", "schema-registry-pass")
          .build();

  private final KafkaRestFixture kafkaRest =
      KafkaRestFixture.builder()
          .setCertificates(certificates, "kafka-rest")
          .setConfig("ssl.client.authentication", "REQUIRED")
          .setConfig("producer.max.block.ms", "10000")
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("kafka-rest", "kafka-rest-pass")
          .setSchemaRegistry(schemaRegistry)
          .build();

  @Override
  public void before() throws Exception {
    certificates.before();
    zookeeper.before();
    kafkaCluster.before();
    schemaRegistry.before();
    kafkaRest.before();
  }

  @Override
  public void after() {
    kafkaRest.after();
    schemaRegistry.after();
    kafkaCluster.after();
    zookeeper.after();
    certificates.after();
  }

  public SslFixture certificates() {
    return certificates;
  }

  public ZookeeperFixture zookeeper() {
    return zookeeper;
  }

  public KafkaClusterFixture kafkaCluster() {
    return kafkaCluster;
  }

  public SchemaRegistryFixture schemaRegistry() {
    return schemaRegistry;
  }

  public KafkaRestFixture kafkaRest() {
    return kafkaRest;
  }
}
