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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.glassfish.jersey.internal.guava.Preconditions.checkArgument;
import static org.glassfish.jersey.internal.guava.Preconditions.checkState;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ByteString;
import io.confluent.kafkarest.common.CompletableFutures;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

  @Nullable
  private List<KafkaServer> brokers;

  public KafkaClusterEnvironment(
      ZookeeperEnvironment zookeeper, int numBrokers, HashMap<String, String> configs) {
    this.zookeeper = requireNonNull(zookeeper);
    this.numBrokers = numBrokers;
    this.configs = requireNonNull(configs);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    checkState(brokers == null, "Starting environment that already started.");

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
    checkState(brokers != null, "Stopping environment that never started.");

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

    brokers = null;
  }

  public String getBootstrapServers() {
    return TestUtils.getBrokerListStrFromServers(
        CollectionConverters.asScala(brokers), SecurityProtocol.PLAINTEXT);
  }

  public AdminClient createAdminClient() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return AdminClient.create(properties);
  }

  public KafkaConsumer<ByteString, ByteString> createConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return new KafkaConsumer<>(
        properties,
        (topic, data) -> ByteString.copyFrom(data),
        (topic, data) -> ByteString.copyFrom(data));
  }

  public String getClusterId() {
    try {
      return createAdminClient().describeCluster().clusterId().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public int getControllerID() {
    try {
      return createAdminClient().describeCluster().controller().get().id();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void createTopic(String topicName, int numPartitions, short replicationFactor) {
    try {
      createAdminClient()
          .createTopics(singletonList(new NewTopic(topicName, numPartitions, replicationFactor)))
          .all()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public ConsumerRecord<ByteString, ByteString> getRecord(String topicName, int partitionId, long offset) {
    KafkaConsumer<ByteString, ByteString> consumer = createConsumer();
    consumer.assign(singletonList(new TopicPartition(topicName, partitionId)));
    consumer.seek(new TopicPartition(topicName, partitionId), offset);
    List<ConsumerRecord<ByteString, ByteString>> records =
        consumer.poll(Duration.ofSeconds(1)).records(new TopicPartition(topicName, partitionId));
    return records.get(0);
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
