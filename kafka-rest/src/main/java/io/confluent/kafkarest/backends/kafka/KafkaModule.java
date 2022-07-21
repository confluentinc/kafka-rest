/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.extension.KafkaRestContextProvider;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.kafka.clients.admin.Admin;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;

/**
 * A module to configure access to Kafka.
 *
 * <p>Right now this module does little but delegate to {@link KafkaRestContext}, since access to
 * Kafka is currently being configured there. It's the author's intention to move such logic here,
 * and eliminate {@code KafkaRestContext}, once dependence injection is properly used elsewhere.</p>
 */
public final class KafkaModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(KafkaRestContextFactory.class)
        .to(KafkaRestContext.class)
        .in(RequestScoped.class);

    bindFactory(AdminFactory.class)
        .to(Admin.class)
        .in(RequestScoped.class);
  }

  private static final class KafkaRestContextFactory implements Factory<KafkaRestContext> {

    @Override
    public KafkaRestContext provide() {
      return KafkaRestContextProvider.getCurrentContext();
    }

    @Override
    public void dispose(KafkaRestContext instance) {
      // Do nothing.
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
      // Do nothing.
    }
  }
}
