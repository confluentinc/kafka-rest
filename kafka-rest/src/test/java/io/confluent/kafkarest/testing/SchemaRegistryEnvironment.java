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

package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkState;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import javax.annotation.Nullable;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class SchemaRegistryEnvironment implements BeforeEachCallback, AfterEachCallback {

  private final KafkaClusterEnvironment kafkaCluster;

  @Nullable
  private URI baseUri;
  @Nullable
  private Server server;
  @Nullable
  private SchemaRegistryClient client;

  private SchemaRegistryEnvironment(KafkaClusterEnvironment kafkaCluster) {
    this.kafkaCluster = requireNonNull(kafkaCluster);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    checkState(server == null);
    baseUri = URI.create(String.format("http://localhost:%d", findUnusedPort()));
    server = new SchemaRegistryRestApplication(createConfigs(baseUri)).createServer();
    server.start();
    client =
        new CachedSchemaRegistryClient(
            singletonList(baseUri.toString()),
            MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
            Arrays.asList(
                new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()),
            emptyMap());
  }

  private SchemaRegistryConfig createConfigs(URI baseUri) throws Exception {
    Properties properties = new Properties();
    properties.put(
        SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG,
        kafkaCluster.getBootstrapServers());
    properties.put(SchemaRegistryConfig.LISTENERS_CONFIG, baseUri.toString());
    return new SchemaRegistryConfig(properties);
  }

  private static int findUnusedPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    checkState(server != null);
    server.stop();
    server = null;
    baseUri = null;
  }

  public URI getBaseUri() {
    checkState(server != null);
    return baseUri;
  }

  public SchemaRegistryClient getClient() {
    checkState(client != null);
    return client;
  }

  public SchemaKey createSchema(String subject, ParsedSchema schema) throws Exception {
    int schemaId = getClient().register(subject, schema);
    int schemaVersion = getClient().getVersion(subject, schema);
    return SchemaKey.create(subject, schemaId, schemaVersion);
  }

  public KafkaAvroDeserializer createAvroDeserializer() {
    return new KafkaAvroDeserializer(client);
  }

  public KafkaJsonSchemaDeserializer<Object> createJsonSchemaDeserializer() {
    return new KafkaJsonSchemaDeserializer<>(client);
  }

  public KafkaProtobufDeserializer<Message> createProtobufDeserializer() {
    return new KafkaProtobufDeserializer<>(client);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private KafkaClusterEnvironment kafkaCluster;

    private Builder() {
    }

    public Builder setKafkaCluster(KafkaClusterEnvironment kafkaCluster) {
      this.kafkaCluster = requireNonNull(kafkaCluster);
      return this;
    }

    public SchemaRegistryEnvironment build() {
      return new SchemaRegistryEnvironment(kafkaCluster);
    }
  }

  @AutoValue
  public abstract static class SchemaKey {

    SchemaKey() {
    }

    public abstract String getSubject();

    public abstract int getSchemaId();

    public abstract int getSchemaVersion();

    public static SchemaKey create(String subject, int schemaId, int schemaVersion) {
      return new AutoValue_SchemaRegistryEnvironment_SchemaKey(subject, schemaId, schemaVersion);
    }
  }
}
