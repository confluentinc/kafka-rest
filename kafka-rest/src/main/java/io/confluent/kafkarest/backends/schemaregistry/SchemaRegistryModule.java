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

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.config.ConfigModule.MaxSchemasPerSubjectConfig;
import io.confluent.kafkarest.config.ConfigModule.SchemaRegistryConfigs;
import io.confluent.kafkarest.config.ConfigModule.SchemaRegistryRequestHeadersConfig;
import io.confluent.kafkarest.config.ConfigModule.SchemaRegistryUrlsConfig;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public final class SchemaRegistryModule extends AbstractBinder {

  @Override
  protected void configure() {
    bindFactory(SchemaRegistryClientFactory.class)
        .to(SchemaRegistryClient.class)
        .in(Singleton.class);
  }

  private static final class SchemaRegistryClientFactory implements Factory<SchemaRegistryClient> {
    private static final List<SchemaProvider> SCHEMA_PROVIDERS =
        Arrays.asList(
            EmbeddedFormat.AVRO.getSchemaProvider(),
            EmbeddedFormat.JSONSCHEMA.getSchemaProvider(),
            EmbeddedFormat.PROTOBUF.getSchemaProvider());

    private final List<URI> schemaRegistryUrls;
    private final int maxSchemasPerSubject;
    private final Map<String, String> requestHeaders;
    private final Map<String, Object> configs;

    @Inject
    private SchemaRegistryClientFactory(
        @SchemaRegistryUrlsConfig List<URI> schemaRegistryUrls,
        @MaxSchemasPerSubjectConfig int maxSchemasPerSubject,
        @SchemaRegistryRequestHeadersConfig Map<String, String> requestHeaders,
        @SchemaRegistryConfigs Map<String, Object> configs) {
      this.schemaRegistryUrls = requireNonNull(schemaRegistryUrls);
      this.maxSchemasPerSubject = maxSchemasPerSubject;
      this.requestHeaders = requireNonNull(requestHeaders);
      this.configs = requireNonNull(configs);
    }

    @Override
    public SchemaRegistryClient provide() {
      return new CachedSchemaRegistryClient(
          schemaRegistryUrls.stream().map(Object::toString).collect(Collectors.toList()),
          maxSchemasPerSubject,
          SCHEMA_PROVIDERS,
          configs,
          requestHeaders);
    }

    @Override
    public void dispose(SchemaRegistryClient schemaRegistryClient) {
    }
  }
}
