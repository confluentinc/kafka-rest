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

package io.confluent.kafkarest.integration;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.testing.KafkaClusterFixture;
import io.confluent.kafkarest.testing.KafkaRestFixture;
import io.confluent.kafkarest.testing.QuorumControllerFixture;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag("IntegrationTest")
public class ProducerLeakTest {
  private static final String TOPIC_NAME = "topic-1";

  @Order(1)
  @RegisterExtension
  private final QuorumControllerFixture quorumController = QuorumControllerFixture.create();

  @Order(2)
  @RegisterExtension
  private final KafkaClusterFixture kafkaCluster =
      KafkaClusterFixture.builder().setNumBrokers(3).setQuorumController(quorumController).build();

  @Order(3)
  @RegisterExtension
  private final KafkaRestFixture kafkaRest =
      KafkaRestFixture.builder()
          .setConfig("producer.max.block.ms", "5000")
          .setConfig("kafka.rest.resource.extension.class", LeakyContextExtension.class.getName())
          .setKafkaCluster(kafkaCluster)
          .build();

  @BeforeEach
  public void setUp() throws Exception {
    kafkaCluster.createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @Test
  public void producerDoesNotLeak() throws Exception {
    String clusterId = kafkaCluster.getClusterId();

    Response response =
        kafkaRest
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.json(new byte[1]));

    // Should give enough time for the client to close.
    response.readEntity(String.class);

    List<String> aliveClients =
        Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .filter(name -> name.contains("proxy"))
            .collect(Collectors.toList());
    assertEquals(
        0,
        aliveClients.size(),
        "Expected no live Kafka client threads, but some got left behind instead: " + aliveClients);
  }

  public static final class LeakyContextExtension implements RestResourceExtension {

    @Override
    public void register(Configurable<?> configurable, KafkaRestConfig config) {
      configurable.register(LeakyFilter.class);
      configurable.register(LeakyModule.class);
    }

    @Override
    public void clean() {}
  }

  private static final class LeakyFilter implements ContainerRequestFilter {
    private final Provider<Producer<byte[], byte[]>> producer;

    @Inject
    public LeakyFilter(Provider<Producer<byte[], byte[]>> producer) {
      this.producer = requireNonNull(producer);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
      producer.get();
    }
  }

  private static final class LeakyModule extends AbstractBinder {

    @Override
    protected void configure() {
      bindFactory(LeakyProducerFactory.class)
          .to(new TypeLiteral<Producer<byte[], byte[]>>() {})
          .in(RequestScoped.class)
          .ranked(1);
    }
  }

  private static final class LeakyProducerFactory implements Factory<Producer<byte[], byte[]>> {
    private final KafkaRestConfig config;

    @Inject
    private LeakyProducerFactory(KafkaRestConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public Producer<byte[], byte[]> provide() {
      Map<String, Object> producerConfigs = config.getProducerConfigs();
      producerConfigs.put("client.id", "proxy");
      return new KafkaProducer<>(
          producerConfigs, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public void dispose(Producer<byte[], byte[]> producer) {
      producer.close();
    }
  }
}
