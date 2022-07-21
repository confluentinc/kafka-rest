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
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * A module to configure access to Kafka.
 *
 * <p>Right now this module does little but delegate to {@link KafkaRestContext}, since access to
 * Kafka is currently being configured there. It's the author's intention to move such logic here,
 * and eliminate {@code KafkaRestContext}, once dependence injection is properly used elsewhere.
 */
public final class KafkaModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(KafkaRestContextFactory.class).to(KafkaRestContext.class).in(Singleton.class);

    bindFactory(AdminFactory.class).to(Admin.class).in(Singleton.class);

    bindFactory(ProducerFactory.class)
        .to(new TypeLiteral<Producer<byte[], byte[]>>() {})
        .in(Singleton.class);
  }

  private static final class KafkaRestContextFactory implements Factory<KafkaRestContext> {
    private final KafkaRestConfig config;

    @Inject
    private KafkaRestContextFactory(KafkaRestConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public KafkaRestContext provide() {
      return new DefaultKafkaRestContext(config);
    }

    @Override
    public void dispose(KafkaRestContext context) {
      context.shutdown();
    }
  }

  private static final class AdminFactory implements Factory<Admin> {
    private final KafkaRestContext context;

    @Inject
    private AdminFactory(KafkaRestContext context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Admin provide() {
      return context.getAdmin();
    }

    @Override
    public void dispose(Admin admin) {
      admin.close();
    }
  }

  private static final class ProducerFactory implements Factory<Producer<byte[], byte[]>> {
    private final KafkaRestContext context;

    @Inject
    private ProducerFactory(KafkaRestContext context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Producer<byte[], byte[]> provide() {
      return context.getProducer();
    }

    @Override
    public void dispose(Producer<byte[], byte[]> producer) {
      producer.close();
    }
  }
}
