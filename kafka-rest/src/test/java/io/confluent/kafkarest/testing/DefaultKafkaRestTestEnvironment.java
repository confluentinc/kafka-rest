package io.confluent.kafkarest.testing;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class DefaultKafkaRestTestEnvironment
    implements BeforeEachCallback, AfterEachCallback {

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
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("kafka-rest", "kafka-rest-pass")
          .setSchemaRegistry(schemaRegistry)
          .build();

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    certificates.beforeEach(context);
    zookeeper.beforeEach(context);
    kafkaCluster.beforeEach(context);
    schemaRegistry.beforeEach(context);
    kafkaRest.beforeEach(context);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    kafkaRest.afterEach(context);
    schemaRegistry.afterEach(context);
    kafkaCluster.afterEach(context);
    zookeeper.afterEach(context);
    certificates.afterEach(context);
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
