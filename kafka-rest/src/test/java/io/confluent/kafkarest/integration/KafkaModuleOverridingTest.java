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

package io.confluent.kafkarest.integration;

import static java.util.Objects.requireNonNull;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.testing.KafkaRestFixture;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.exceptions.RestException;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.easymock.Capture;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag("IntegrationTest")
public class KafkaModuleOverridingTest {
  /** HTTP 418 I'm a teapot */
  private static final int I_M_A_TEAPOT_STATUS_CODE = 418;

  private static final String KAFKA_REST_CONTEXT_PROPERTY_NAME = "i.m.a.teapot";

  @RegisterExtension
  public final KafkaRestFixture kafkaRest =
      KafkaRestFixture.builder()
          .setConfig("bootstrap.servers", "foobar")
          .setConfig(
              "kafka.rest.resource.extension.class", TeapotRestContextExtension.class.getName())
          .build();

  @Test
  public void adminIsCreatedPerRequestAndDisposedAfterRequest() {
    Response response =
        kafkaRest
            .target()
            .path("/v3/clusters")
            .request()
            .header("X-Teapot-Context", "")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(I_M_A_TEAPOT_STATUS_CODE, response.getStatus());
  }

  @Test
  public void producerIsCreatedPerRequestAndDisposedAfterRequest() {
    Response response =
        kafkaRest
            .target()
            .path("/v3/clusters/foo/topics/bar/records")
            .request()
            .header("X-Teapot-Context", "")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ErrorResponse error = response.readEntity(ErrorResponse.class);
    assertEquals(I_M_A_TEAPOT_STATUS_CODE, error.getErrorCode());
  }

  @Test
  public void consumersResourceIsInitializedCorrectly() {
    // ConsumersResource is the only resource requiring a KafkaRestContext. If the context is not
    // injected via a Provider, it will be resolved too early (before filters augmenting the
    // context have kicked in), resulting in NullPointerExceptions leading to HTTP 500 errors
    // (see KREST-4243 to KREST-4245 and KREST-4374 to KREST-4376).
    Response response =
        kafkaRest
            .target()
            .path("/consumers/bar")
            .request()
            .header("X-Teapot-Context", "")
            .accept(Versions.KAFKA_V2_JSON)
            .post(Entity.entity(CreateConsumerInstanceRequest.PROTOTYPE, Versions.KAFKA_V2_JSON));
    assertEquals(I_M_A_TEAPOT_STATUS_CODE, response.getStatus());
  }

  public static final class TeapotRestContextExtension implements RestResourceExtension {

    @Override
    public void register(Configurable<?> configurable, KafkaRestConfig config) {
      configurable.register(TeapotContainerRequestFilter.class);
      configurable.register(TeapotModule.class);
    }

    @Override
    public void clean() {}
  }

  private static final class TeapotContainerRequestFilter implements ContainerRequestFilter {
    private final KafkaRestConfig config;

    @Inject
    public TeapotContainerRequestFilter(KafkaRestConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
      if (requestContext.getHeaderString("X-Teapot-Context") != null) {
        requestContext.setProperty(
            KAFKA_REST_CONTEXT_PROPERTY_NAME, new TeapotKafkaRestContext(config));
      } else {
        requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
      }
    }
  }

  private static final class TeapotKafkaRestContext implements KafkaRestContext {
    private final KafkaRestConfig config;

    private TeapotKafkaRestContext(KafkaRestConfig config) {
      this.config = requireNonNull(config);
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
      KafkaConsumerManager kafkaConsumerManager = mock(KafkaConsumerManager.class);
      expect(
              kafkaConsumerManager.createConsumer(
                  anyString(), anyObject(ConsumerInstanceConfig.class)))
          .andThrow(
              new RestException(
                  "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE));
      kafkaConsumerManager.shutdown();
      expectLastCall();
      replay(kafkaConsumerManager);
      return kafkaConsumerManager;
    }

    @Override
    public synchronized Admin getAdmin() {
      Admin adminClient = mock(Admin.class);
      DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
      expect(adminClient.describeCluster(anyObject())).andReturn(describeClusterResult);
      expect(describeClusterResult.clusterId())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      expect(describeClusterResult.controller())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      expect(describeClusterResult.nodes())
          .andReturn(
              KafkaFutures.failedFuture(
                  new RestException(
                      "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE)));
      adminClient.close();
      expectLastCall();
      replay(adminClient, describeClusterResult);
      return adminClient;
    }

    @Override
    public synchronized Producer<byte[], byte[]> getProducer() {
      Producer<byte[], byte[]> producer = strictMock(Producer.class);
      Capture<Callback> callback = newCapture();
      expect(producer.send(anyObject(), capture(callback)))
          .andAnswer(
              () -> {
                callback
                    .getValue()
                    .onCompletion(
                        /* metadata= */ null,
                        new RestException(
                            "I'm a teapot", I_M_A_TEAPOT_STATUS_CODE, I_M_A_TEAPOT_STATUS_CODE));
                return null;
              });
      producer.close();
      expectLastCall();
      replay(producer);
      return producer;
    }

    @Override
    public void shutdown() {}
  }

  private static final class TeapotModule extends AbstractBinder {

    @Override
    protected void configure() {
      bindFactory(TeapotKafkaRestContextFactory.class)
          .to(KafkaRestContext.class)
          .in(RequestScoped.class)
          .ranked(1);

      bindFactory(TeapotAdminFactory.class).to(Admin.class).in(RequestScoped.class).ranked(1);

      bindFactory(TeapotProducerFactory.class)
          .to(new TypeLiteral<Producer<byte[], byte[]>>() {})
          .in(RequestScoped.class)
          .ranked(1);
    }
  }

  private static final class TeapotKafkaRestContextFactory implements Factory<KafkaRestContext> {
    private final Provider<ContainerRequestContext> requestContext;

    @Inject
    private TeapotKafkaRestContextFactory(
        @Context Provider<ContainerRequestContext> requestContext) {
      this.requestContext = requireNonNull(requestContext);
    }

    @Override
    public KafkaRestContext provide() {
      return (KafkaRestContext)
          requireNonNull(
              requestContext.get().getProperty(KAFKA_REST_CONTEXT_PROPERTY_NAME),
              "KafkaRestContext cannot be null.");
    }

    @Override
    public void dispose(KafkaRestContext context) {
      context.shutdown();
    }
  }

  private static final class TeapotAdminFactory implements Factory<Admin> {
    private final Provider<KafkaRestContext> context;

    @Inject
    private TeapotAdminFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Admin provide() {
      return context.get().getAdmin();
    }

    @Override
    public void dispose(Admin admin) {
      admin.close();
      verify(admin);
    }
  }

  private static final class TeapotProducerFactory implements Factory<Producer<byte[], byte[]>> {
    private final Provider<KafkaRestContext> context;

    @Inject
    private TeapotProducerFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Producer<byte[], byte[]> provide() {
      return context.get().getProducer();
    }

    @Override
    public void dispose(Producer<byte[], byte[]> producer) {
      producer.close();
      verify(producer);
    }
  }
}
