/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
          .setConfig("producer.max.block.ms", "5000")
          .setConfig("ssl.client.authentication", "REQUIRED")
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("kafka-rest", "kafka-rest-pass")
          .setSchemaRegistry(schemaRegistry)
          .build();

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    certificates.beforeEach(extensionContext);
    zookeeper.beforeEach(extensionContext);
    kafkaCluster.beforeEach(extensionContext);
    schemaRegistry.beforeEach(extensionContext);
    kafkaRest.beforeEach(extensionContext);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    kafkaRest.afterEach(extensionContext);
    schemaRegistry.afterEach(extensionContext);
    kafkaCluster.afterEach(extensionContext);
    zookeeper.afterEach(extensionContext);
    certificates.afterEach(extensionContext);
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
