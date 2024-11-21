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

package io.confluent.kafkarest.backends.schemaregistry;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.KafkaRestContext;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;

public final class SchemaRegistryModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(SchemaRegistryClientFactory.class)
        .to(new TypeLiteral<Optional<SchemaRegistryClient>>() {})
        .in(RequestScoped.class);
  }

  private static final class SchemaRegistryClientFactory
      implements Factory<Optional<SchemaRegistryClient>> {

    private final Provider<KafkaRestContext> context;

    @Inject
    private SchemaRegistryClientFactory(Provider<KafkaRestContext> context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Optional<SchemaRegistryClient> provide() {
      return Optional.ofNullable(context.get().getSchemaRegistryClient());
    }

    @Override
    public void dispose(Optional<SchemaRegistryClient> schemaRegistryClient) {}
  }
}
