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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.internal.guava.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafkarest.common.CompletableFutures;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An extension that runs a {@link KafkaBrokerFixture Kafka} cluster. */
public final class KafkaClusterFixture implements BeforeEachCallback, AfterEachCallback {

  private final ImmutableList<KafkaBrokerFixture> brokers;

  @Nullable private AdminClient adminClient;

  public KafkaClusterFixture(List<KafkaBrokerFixture> brokers) {
    this.brokers = ImmutableList.copyOf(brokers);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    CompletableFutures.allAsList(
            brokers.stream()
                .map(
                    broker ->
                        CompletableFuture.runAsync(
                            () -> {
                              try {
                                broker.beforeEach(extensionContext);
                              } catch (Exception e) {
                                throw new RuntimeException(e);
                              }
                            }))
                .collect(Collectors.toList()))
        .join();

    adminClient = AdminClient.create(getAdminConfigs());
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    if (adminClient != null) {
      adminClient.close();
    }

    CompletableFutures.allAsList(
            brokers.stream()
                .map(
                    broker ->
                        CompletableFuture.runAsync(
                            () -> {
                              try {
                                broker.afterEach(extensionContext);
                              } catch (Exception e) {
                                throw new RuntimeException(e);
                              }
                            }))
                .collect(Collectors.toList()))
        .join();
  }

  public boolean isSaslSecurity() {
    return brokers.stream().findAny().get().isSaslSecurity();
  }

  public boolean isSslSecurity() {
    return brokers.stream().findAny().get().isSslSecurity();
  }

  public String getBootstrapServers() {
    return brokers.stream()
        .map(KafkaBrokerFixture::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  public SecurityProtocol getSecurityProtocol() {
    return brokers.stream().findAny().get().getSecurityProtocol();
  }

  private Properties getAdminConfigs() {
    Properties properties = brokers.stream().findAny().get().getAdminConfigs();
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return properties;
  }

  private Properties getConsumerConfigs() {
    Properties properties = brokers.stream().findAny().get().getConsumerConfigs();
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return properties;
  }

  public Admin getAdmin() {
    checkState(adminClient != null);
    return adminClient;
  }

  public <K, V> KafkaConsumer<K, V> getConsumer(
      Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    return new KafkaConsumer<>(getConsumerConfigs(), keyDeserializer, valueDeserializer);
  }

  public String getClusterId() throws Exception {
    checkState(adminClient != null);
    return adminClient.describeCluster().clusterId().get();
  }

  public <K, V> ConsumerRecord<K, V> getRecord(
      String topicName,
      int partitionId,
      long offset,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    KafkaConsumer<K, V> consumer = getConsumer(keyDeserializer, valueDeserializer);
    consumer.assign(singletonList(new TopicPartition(topicName, partitionId)));
    consumer.seek(new TopicPartition(topicName, partitionId), offset);
    List<ConsumerRecord<K, V>> records =
        consumer.poll(Duration.ofSeconds(1)).records(new TopicPartition(topicName, partitionId));
    ConsumerRecord<K, V> record = records.iterator().next();
    consumer.close();
    return record;
  }

  public void createTopic(String topicName, int numPartitions, short replicationFactor)
      throws Exception {
    checkState(adminClient != null);
    adminClient
        .createTopics(singletonList(new NewTopic(topicName, numPartitions, replicationFactor)))
        .all()
        .get();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private SslFixture certificates = null;
    private final HashMap<String, String> configs = new HashMap<>();
    private ImmutableList<String> keyNames = null;
    private int numBrokers = 0;
    private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    private final ImmutableMap.Builder<String, String> users = ImmutableMap.builder();
    private final ImmutableSet.Builder<String> superUsers = ImmutableSet.builder();
    private ZookeeperFixture zookeeper = null;

    private Builder() {}

    /** @see KafkaBrokerFixture.Builder#addUser(String, String) */
    public Builder addUser(String username, String password) {
      users.put(username, password);
      return this;
    }

    /** @see KafkaBrokerFixture.Builder#addSuperUser(String) */
    public Builder addSuperUser(String username) {
      checkArgument(users.build().containsKey(username));
      superUsers.add(username);
      return this;
    }

    public Builder setCertificates(SslFixture certificates, String... keyNames) {
      this.certificates = requireNonNull(certificates);
      this.keyNames = ImmutableList.copyOf(keyNames);
      return this;
    }

    /** @see KafkaBrokerFixture.Builder#setConfig(String, String) */
    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    public Builder setNumBrokers(int numBrokers) {
      checkArgument(numBrokers > 0);
      this.numBrokers = numBrokers;
      return this;
    }

    /** @see KafkaBrokerFixture.Builder#setSecurityProtocol(SecurityProtocol) */
    public Builder setSecurityProtocol(SecurityProtocol securityProtocol) {
      this.securityProtocol = requireNonNull(securityProtocol);
      return this;
    }

    public Builder setZookeeper(ZookeeperFixture zookeeper) {
      this.zookeeper = requireNonNull(zookeeper);
      return this;
    }

    public KafkaClusterFixture build() {
      checkState(numBrokers > 0);
      checkState(zookeeper != null);
      return new KafkaClusterFixture(
          IntStream.range(0, numBrokers)
              .mapToObj(
                  brokerId -> {
                    KafkaBrokerFixture.Builder broker =
                        KafkaBrokerFixture.builder()
                            .setBrokerId(brokerId)
                            .addUsers(users.build())
                            .addSuperUsers(superUsers.build())
                            .setConfigs(configs)
                            .setSecurityProtocol(securityProtocol)
                            .setZookeeper(zookeeper);
                    if (certificates != null) {
                      broker.setCertificate(certificates, keyNames.get(brokerId));
                    }
                    return broker.build();
                  })
              .collect(Collectors.toList()));
    }
  }
}
