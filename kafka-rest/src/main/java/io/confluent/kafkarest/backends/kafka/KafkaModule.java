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

package io.confluent.kafkarest.backends.kafka;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerMetrics;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.utils.Time;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;

/**
 * A module to configure access to Kafka.
 *
 * <p>Right now this module does little but delegate to {@link KafkaRestContext}, since access to
 * Kafka is currently being configured there. It's the author's intention to move such logic here,
 * and eliminate {@code KafkaRestContext}, once dependence injection is properly used elsewhere.
 */
public final class KafkaModule extends AbstractBinder {

  public static final String KAFKA_REST_CONTEXT_PROPERTY_NAME = "io.confluent.kafkarest.context";

  @Override
  protected void configure() {
    bindFactory(KafkaRestContextFactory.class, Singleton.class)
        .to(KafkaRestContext.class)
        .in(RequestScoped.class);

    bindFactory(AdminFactory.class).to(Admin.class).in(RequestScoped.class);

    bindFactory(ProducerFactory.class)
        .to(new TypeLiteral<Producer<byte[], byte[]>>() {})
        .in(RequestScoped.class);

    bindFactory(ProducerMetricsFactory.class, Singleton.class)
        .to(ProducerMetrics.class)
        .in(Singleton.class);
  }

  private static final class KafkaRestContextFactory implements Factory<KafkaRestContext> {

    private final Provider<ContainerRequestContext> requestContext;
    private final KafkaRestContext defaultContext;

    @Inject
    private KafkaRestContextFactory(
        @Context Provider<ContainerRequestContext> requestContext, KafkaRestConfig config) {
      this.requestContext = requireNonNull(requestContext);
      this.defaultContext = new DefaultKafkaRestContext(config);
    }

    @Override
    public KafkaRestContext provide() {
      KafkaRestContext context =
          (KafkaRestContext) requestContext.get().getProperty(KAFKA_REST_CONTEXT_PROPERTY_NAME);
      return context != null ? context : defaultContext;
    }

    @Override
    public void dispose(KafkaRestContext instance) {
      // The context is either disposed by whoever set it in the ContainerRequestContext, or it is
      // the defaultContext, in which case it should not be disposed.
    }
  }

  private static final class AdminFactory implements Factory<Admin> {

    private final Provider<KafkaRestContext> context;

    @Inject
    private AdminFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Admin provide() {
      return context.get().getAdmin();
    }

    @Override
    public void dispose(Admin instance) {
      // AdminClient is disposed when the KafkaRestContext is disposed.
    }
  }

  private static final class ProducerFactory implements Factory<Producer<byte[], byte[]>> {

    private final Provider<KafkaRestContext> context;

    @Inject
    private ProducerFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Producer<byte[], byte[]> provide() {
      return context.get().getProducer();
    }

    @Override
    public void dispose(Producer<byte[], byte[]> producer) {
      // Producer is disposed when the KafkaRestContext is disposed.
    }
  }

  private static final class ProducerMetricsFactory implements Factory<ProducerMetrics> {

    private final Provider<KafkaRestContext> context;
    private volatile ProducerMetrics producerMetrics;

    @Inject
    ProducerMetricsFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public ProducerMetrics provide() {
      if (producerMetrics != null) {
        throw new IllegalStateException("Attempted to recreate existing global ProducerMetrics");
      }
      producerMetrics = new ProducerMetrics(context.get().getConfig(), Time.SYSTEM);
      return producerMetrics;
    }

    @Override
    public void dispose(ProducerMetrics producerMetrics) {
      // the JVM will close JMX
    }
  }
}
