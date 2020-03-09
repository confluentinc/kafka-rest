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

import io.confluent.kafkarest.KafkaRestContext;
import java.util.Objects;
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

  private final KafkaRestContext context;

  public KafkaModule(KafkaRestContext context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  protected void configure() {
    // Reuse the AdminClient being constructed in KafkaRestContext. The request-scope is needed
    // because the Admin creation logic uses request-scoped auth information. KafkaRestContext
    // itself is a proxied instance, so the result will be cached there appropriately, after it is
    // created. See KafkaRestContextProvider and ContextInvocationHandler for more information.
    bindFactory(new AdminFactory(context))
        .to(Admin.class)
        .in(RequestScoped.class)
        .proxy(true)
        .proxyForSameScope(true);
  }

  private static final class AdminFactory implements Factory<Admin> {

    private final KafkaRestContext context;

    private AdminFactory(KafkaRestContext context) {
      this.context = Objects.requireNonNull(context);
    }

    @Override
    public Admin provide() {
      return context.getAdmin();
    }

    @Override
    public void dispose(Admin instance) {
      // Do nothing.
    }
  }
}
