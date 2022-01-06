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
import io.confluent.kafkarest.config.ConfigModule.AvroSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.JsonschemaSerializerConfigs;
import io.confluent.kafkarest.config.ConfigModule.ProtobufSerializerConfigs;
import io.confluent.kafkarest.controllers.SchemaRecordSerializer;
import io.confluent.kafkarest.controllers.SchemaRecordSerializerImpl;
import io.confluent.kafkarest.controllers.SchemaRecordSerializerThrowing;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaRegistryModule extends AbstractBinder {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryModule.class);

  @Override
  protected void configure() {
    bindFactory(SchemaRegistryClientFactory.class)
        .to(new TypeLiteral<Optional<SchemaRegistryClient>>() {})
        .in(RequestScoped.class);

    bindFactory(SchemaRecordSerializerFactory.class)
        .to(SchemaRecordSerializer.class)
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

  private static final class SchemaRecordSerializerFactory
      implements Factory<SchemaRecordSerializer> {

    private final Optional<SchemaRegistryClient> schemaRegistryClient;
    private final Map<String, Object> avroSerializerConfigs;
    private final Map<String, Object> jsonschemaSerializerConfigs;
    private final Map<String, Object> protobufSerializerConfigs;

    @Inject
    private SchemaRecordSerializerFactory(
        Optional<SchemaRegistryClient> schemaRegistryClient,
        @AvroSerializerConfigs Map<String, Object> avroSerializerConfigs,
        @JsonschemaSerializerConfigs Map<String, Object> jsonschemaSerializerConfigs,
        @ProtobufSerializerConfigs Map<String, Object> protobufSerializerConfigs) {
      this.schemaRegistryClient = schemaRegistryClient;
      this.avroSerializerConfigs = avroSerializerConfigs;
      this.jsonschemaSerializerConfigs = jsonschemaSerializerConfigs;
      this.protobufSerializerConfigs = protobufSerializerConfigs;
    }

    @Override
    public SchemaRecordSerializer provide() {
      if (schemaRegistryClient.isPresent()) {
        return new SchemaRecordSerializerImpl(
            schemaRegistryClient,
            avroSerializerConfigs,
            jsonschemaSerializerConfigs,
            protobufSerializerConfigs);
      } else {
        return new SchemaRecordSerializerThrowing();
      }
    }

    @Override
    public void dispose(SchemaRecordSerializer schemaRecordSerializer) {}
  }
}
