/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafkarest.testing.fake;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.testing.KafkaRestFixture;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class FakeKafkaRestTestEnvironment implements AfterEachCallback, BeforeEachCallback {
  private final FakeKafkaCluster kafkaCluster = new FakeKafkaCluster();
  private final FakeSchemaRegistry schemaRegistry = new FakeSchemaRegistry();
  private final KafkaRestFixture kafkaRest =
      KafkaRestFixture.builder()
          .setConfig("bootstrap.servers", "foobar")
          .addModule(new FakeKafkaModule(kafkaCluster, schemaRegistry))
          .build();

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    kafkaRest.beforeEach(extensionContext);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    kafkaRest.afterEach(extensionContext);
  }

  public FakeKafkaCluster kafkaCluster() {
    return kafkaCluster;
  }

  public FakeSchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }

  public KafkaRestFixture kafkaRest() {
    return kafkaRest;
  }

  private static final class FakeKafkaModule extends AbstractBinder {
    private final FakeKafkaCluster kafkaCluster;
    private final FakeSchemaRegistry schemaRegistry;

    private FakeKafkaModule(FakeKafkaCluster kafkaCluster, FakeSchemaRegistry schemaRegistry) {
      this.kafkaCluster = requireNonNull(kafkaCluster);
      this.schemaRegistry = requireNonNull(schemaRegistry);
    }

    @Override
    protected void configure() {
      bind(kafkaCluster).to(FakeKafkaCluster.class).ranked(1);
      bind(schemaRegistry).to(FakeSchemaRegistry.class).ranked(1);
      bind(FakeKafkaRestContext.class).to(KafkaRestContext.class).ranked(1);
    }
  }

  private static final class FakeKafkaRestContext implements KafkaRestContext {
    private final KafkaRestConfig config;
    private final FakeKafkaCluster kafkaCluster;
    private final FakeSchemaRegistry schemaRegistry;

    @Inject
    FakeKafkaRestContext(
        KafkaRestConfig config, FakeKafkaCluster kafkaCluster, FakeSchemaRegistry schemaRegistry) {
      this.config = requireNonNull(config);
      this.kafkaCluster = requireNonNull(kafkaCluster);
      this.schemaRegistry = requireNonNull(schemaRegistry);
    }

    @Override
    public KafkaRestConfig getConfig() {
      return config;
    }

    @Override
    public ProducerPool getProducerPool() {
      throw new UnsupportedOperationException();
    }

    @Override
    public KafkaConsumerManager getKafkaConsumerManager() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Admin getAdmin() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
      return new FakeKafkaProducer<>(
          kafkaCluster, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public SchemaRegistryClient getSchemaRegistryClient() {
      return schemaRegistry.getClient();
    }

    @Override
    public void shutdown() {}
  }
}
