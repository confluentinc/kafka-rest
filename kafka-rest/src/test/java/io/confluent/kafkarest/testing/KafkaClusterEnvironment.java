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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.glassfish.jersey.internal.guava.Preconditions.checkArgument;
import static org.glassfish.jersey.internal.guava.Preconditions.checkState;

import io.confluent.kafkarest.common.CompletableFutures;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

public final class KafkaClusterEnvironment implements BeforeEachCallback, AfterEachCallback {

  private static final MockTime MOCK_TIME =
      new MockTime(System.currentTimeMillis(), System.nanoTime());

  private final ZookeeperEnvironment zookeeper;
  private final int numBrokers;
  private final HashMap<String, String> configs;

  @Nullable private List<KafkaServer> brokers;
  @Nullable private AdminClient adminClient;

  public KafkaClusterEnvironment(
      ZookeeperEnvironment zookeeper, int numBrokers, HashMap<String, String> configs) {
    this.zookeeper = requireNonNull(zookeeper);
    this.numBrokers = numBrokers;
    this.configs = requireNonNull(configs);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    checkState(adminClient == null && brokers == null);

    brokers =
        CompletableFutures.allAsList(
            IntStream.range(0, numBrokers)
                .mapToObj(brokerId -> KafkaConfig.fromProps(createBrokerConfigs(brokerId)))
                .map(
                    properties ->
                        CompletableFuture.supplyAsync(
                            () -> TestUtils.createServer(properties, MOCK_TIME)))
                .collect(toList()))
            .join();

    adminClient =
        AdminClient.create(
            singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers()));
  }

  private Properties createBrokerConfigs(int brokerId) {
    Properties properties =
        TestUtils.createBrokerConfig(
            /* nodeId= */ brokerId,
            /* zkConnect= */ zookeeper.getZookeeperConnect(),
            /* enableControlledShutdown= */ false,
            /* enableDeleteTopic= */ false,
            /* port= */ TestUtils.RandomPort(),
            /* interBrokerSecurityProtocol= */ Option.apply(SecurityProtocol.PLAINTEXT),
            /* trustStoreFile= */ Option.empty(),
            /* saslProperties= */ Option.empty(),
            /* enablePlaintext=*/ true,
            /* enableSaslPlaintext= */ false,
            /* saslPlaintextPort= */ TestUtils.RandomPort(),
            /* enableSsl= */ false,
            /* sslPort= */ TestUtils.RandomPort(),
            /* enableSaslSsl= */ false,
            /* saslSslPort= */ TestUtils.RandomPort(),
            /* rack= */ Option.empty(),
            /* logDirCount= */ 1,
            /* enableToken= */ false,
            /* numPartitions= */ 1,
            /* defaultReplicationFactor= */ (short) 1);

    properties.setProperty("auto.create.topics.enable", "false");
    properties.setProperty("zookeeper.connect", zookeeper.getZookeeperConnect());

    for (Map.Entry<String, String> config : configs.entrySet()) {
      properties.setProperty(config.getKey(), config.getValue());
    }

    return properties;
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    checkState(adminClient != null && brokers != null);

    adminClient.close();
    CompletableFutures.allAsList(
        brokers.stream()
            .map(
                broker ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          broker.shutdown();
                          CoreUtils.delete(broker.config().logDirs());
                          return null;
                        }))
            .collect(toList()))
        .join();

    adminClient = null;
    brokers = null;
  }

  public String getBootstrapServers() {
    checkState(brokers != null);
    return TestUtils.getBrokerListStrFromServers(
        CollectionConverters.asScala(brokers), SecurityProtocol.PLAINTEXT);
  }

  public AdminClient getAdminClient() {
    checkState(adminClient != null);
    return adminClient;
  }

  public <K, V> KafkaConsumer<K, V> createConsumer(
      Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
  }

  public String getClusterId() throws Exception{
    return getAdminClient().describeCluster().clusterId().get();
  }

  public void createTopic(String topicName, int numPartitions, short replicationFactor)
      throws Exception {
    getAdminClient()
        .createTopics(singletonList(new NewTopic(topicName, numPartitions, replicationFactor)))
        .all()
        .get();
  }

  public <K, V> ConsumerRecord<K, V> getRecord(
      String topicName,
      int partitionId,
      long offset,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    KafkaConsumer<K, V> consumer = createConsumer(keyDeserializer, valueDeserializer);
    consumer.assign(singletonList(new TopicPartition(topicName, partitionId)));
    consumer.seek(new TopicPartition(topicName, partitionId), offset);
    List<ConsumerRecord<K, V>> records =
        consumer.poll(Duration.ofSeconds(1))
            .records(new TopicPartition(topicName, partitionId));
    ConsumerRecord<K, V> record = records.iterator().next();
    consumer.close();
    return record;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final HashMap<String, String> configs = new HashMap<>();
    private int numBrokers;
    private ZookeeperEnvironment zookeeper;

    private Builder() {
    }

    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    public Builder setNumBrokers(int numBrokers) {
      checkArgument(numBrokers > 0, "numBrokers should be greater than zero.");
      this.numBrokers = numBrokers;
      return this;
    }

    public Builder setZookeeper(ZookeeperEnvironment zookeeper) {
      this.zookeeper = requireNonNull(zookeeper);
      return this;
    }

    public KafkaClusterEnvironment build() {
      return new KafkaClusterEnvironment(zookeeper, numBrokers, configs);
    }
  }
}
