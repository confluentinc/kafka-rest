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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
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
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An extension that runs a Schema Registry server. */
public final class SchemaRegistryFixture implements BeforeEachCallback, AfterEachCallback {

  @Nullable private final SslFixture certificates;
  private final ImmutableMap<String, String> clientConfigs;
  private final ImmutableMap<String, String> configs;
  private final KafkaClusterFixture kafkaCluster;
  @Nullable private final String kafkaPassword;
  @Nullable private final String kafkaUser;
  @Nullable private final String keyName;

  @Nullable private URI baseUri;
  @Nullable private Server server;
  @Nullable private SchemaRegistryClient client;

  private SchemaRegistryFixture(
      @Nullable SslFixture certificates,
      Map<String, String> clientConfigs,
      Map<String, String> configs,
      KafkaClusterFixture kafkaCluster,
      @Nullable String kafkaPassword,
      @Nullable String kafkaUser,
      @Nullable String keyName) {
    checkArgument(kafkaUser != null ^ kafkaPassword == null);
    checkArgument(certificates != null ^ keyName == null);
    this.certificates = certificates;
    this.clientConfigs = ImmutableMap.copyOf(clientConfigs);
    this.configs = ImmutableMap.copyOf(configs);
    this.kafkaCluster = requireNonNull(kafkaCluster);
    this.kafkaPassword = kafkaPassword;
    this.kafkaUser = kafkaUser;
    this.keyName = keyName;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    checkState(server == null);
    server = new SchemaRegistryRestApplication(createConfigs()).createServer();
    server.start();
    baseUri = server.getURI();
    client =
        new CachedSchemaRegistryClient(
            singletonList(baseUri.toString()),
            MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
            Arrays.asList(
                new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()),
            getClientConfigs());
  }

  private SchemaRegistryConfig createConfigs() throws Exception {
    Properties properties = new Properties();
    properties.put(
        SchemaRegistryConfig.LISTENERS_CONFIG,
        String.format("%s://localhost:0", certificates != null ? "https" : "http"));
    properties.put(
        SchemaRegistryConfig.INTER_INSTANCE_PROTOCOL_CONFIG,
        certificates != null ? "https" : "http");
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName, ""));
    }
    properties.putAll(getKafkaConfigs());
    properties.putAll(configs);
    return new SchemaRegistryConfig(properties);
  }

  private Properties getKafkaConfigs() {
    Properties properties = new Properties();
    properties.put(
        SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG,
        kafkaCluster.getBootstrapServers());
    properties.put("kafkastore.security.protocol", kafkaCluster.getSecurityProtocol().name());
    if (kafkaCluster.isSaslSecurity() && kafkaUser != null) {
      properties.put(
          "kafkastore.sasl.jaas.config",
          String.format(
              "org.apache.kafka.common.security.plain.PlainLoginModule required "
                  + "username=\"%s\" "
                  + "password=\"%s\";",
              kafkaUser, kafkaPassword));
      properties.put("kafkastore.sasl.mechanism", "PLAIN");
    }
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName, "kafkastore."));
    }
    return properties;
  }

  private Map<String, String> getClientConfigs() {
    checkState(baseUri != null);
    ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();
    configs.put("schema.registry.url", baseUri.toString());
    if (certificates != null) {
      configs.putAll(certificates.getSslConfigs(keyName, "schema.registry."));
    }
    configs.putAll(clientConfigs);
    return configs.build();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        // Do nothing.
      }
    }
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
    private SslFixture certificates = null;
    private final ImmutableMap.Builder<String, String> clientConfigs = ImmutableMap.builder();
    private final ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();
    private KafkaClusterFixture kafkaCluster;
    private String kafkaPassword = null;
    private String kafkaUser = null;
    private String keyName = null;

    private Builder() {}

    /**
     * Sets the SSL certificate store, and the name of the certificate to use as the schema registry
     * certificate.
     */
    public Builder setCertificates(SslFixture certificates, String keyName) {
      this.certificates = requireNonNull(certificates);
      this.keyName = requireNonNull(keyName);
      return this;
    }

    /**
     * Sets a {@link SchemaRegistryClient client} config.
     *
     * @see SchemaRegistryFixture#getClient()
     */
    public Builder setClientConfig(String name, String value) {
      clientConfigs.put(name, value);
      return this;
    }

    /** Sets a Schema Registry server config. */
    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    public Builder setKafkaCluster(KafkaClusterFixture kafkaCluster) {
      this.kafkaCluster = requireNonNull(kafkaCluster);
      return this;
    }

    /** Sets the Kafka SASL PLAIN credentials. */
    public Builder setKafkaUser(String username, String password) {
      this.kafkaUser = requireNonNull(username);
      this.kafkaPassword = requireNonNull(password);
      return this;
    }

    public SchemaRegistryFixture build() {
      return new SchemaRegistryFixture(
          certificates,
          clientConfigs.build(),
          configs.build(),
          kafkaCluster,
          kafkaPassword,
          kafkaUser,
          keyName);
    }
  }

  @AutoValue
  public abstract static class SchemaKey {

    SchemaKey() {}

    public abstract String getSubject();

    public abstract int getSchemaId();

    public abstract int getSchemaVersion();

    public static SchemaKey create(String subject, int schemaId, int schemaVersion) {
      return new AutoValue_SchemaRegistryFixture_SchemaKey(subject, schemaId, schemaVersion);
    }
  }
}
