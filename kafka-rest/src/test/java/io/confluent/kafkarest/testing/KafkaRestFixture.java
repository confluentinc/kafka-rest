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
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An extension that runs a Kafka REST server. */
public final class KafkaRestFixture implements BeforeEachCallback, AfterEachCallback {

  @Nullable private final SslFixture certificates;
  private final HashMap<String, String> configs;
  @Nullable private final KafkaClusterFixture kafkaCluster;
  @Nullable private final String kafkaPassword;
  @Nullable private final String kafkaUser;
  @Nullable private final String keyName;
  @Nullable private final SchemaRegistryFixture schemaRegistry;

  @Nullable private URI baseUri;
  @Nullable private KafkaRestApplication application;
  @Nullable private Server server;

  private KafkaRestFixture(
      @Nullable SslFixture certificates,
      HashMap<String, String> configs,
      @Nullable KafkaClusterFixture kafkaCluster,
      @Nullable String kafkaPassword,
      @Nullable String kafkaUser,
      @Nullable String keyName,
      @Nullable SchemaRegistryFixture schemaRegistry) {
    checkArgument(kafkaUser != null ^ kafkaPassword == null);
    checkArgument(certificates != null ^ keyName == null);
    this.certificates = certificates;
    this.configs = requireNonNull(configs);
    this.kafkaCluster = kafkaCluster;
    this.kafkaPassword = kafkaPassword;
    this.kafkaUser = kafkaUser;
    this.keyName = keyName;
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    checkState(server == null);
    application = new KafkaRestApplication(createConfigs());
    server = application.createServer();
    server.start();
    baseUri = server.getURI();
  }

  private KafkaRestConfig createConfigs() {
    Properties properties = new Properties();
    properties.put(
        RestConfig.LISTENERS_CONFIG,
        String.format("%s://localhost:0", certificates != null ? "https" : "http"));
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName, ""));
    }
    properties.putAll(getKafkaConfigs());
    properties.putAll(getSchemaRegistryConfigs());
    properties.putAll(configs);
    return new KafkaRestConfig(properties);
  }

  private Properties getKafkaConfigs() {
    Properties properties = new Properties();
    if (kafkaCluster != null) {
      properties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
      properties.put("client.security.protocol", kafkaCluster.getSecurityProtocol().name());
      if (kafkaCluster.isSaslSecurity() && kafkaUser != null) {
        properties.put(
            "client.sasl.jaas.config",
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"%s\" "
                    + "password=\"%s\";",
                kafkaUser, kafkaPassword));
        properties.put("client.sasl.mechanism", "PLAIN");
      }
    }
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName, "client."));
    }
    return properties;
  }

  private Properties getSchemaRegistryConfigs() {
    Properties properties = new Properties();
    if (schemaRegistry == null) {
      return properties;
    }
    properties.put(
        KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getBaseUri().toString());
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName, "schema.registry."));
    }
    return properties;
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
    application = null;
    baseUri = null;
  }

  public URI getBaseUri() {
    checkState(baseUri != null);
    return baseUri;
  }

  public ObjectMapper getObjectMapper() {
    checkState(application != null);
    return application.getJsonMapper();
  }

  public WebTarget target() {
    checkState(application != null);
    if (certificates != null) {
      return targetAs(keyName);
    } else {
      return target(ClientBuilder.newClient());
    }
  }

  public WebTarget targetAs(String keyName) {
    checkState(application != null);
    checkState(certificates != null);
    return target(
        ClientBuilder.newBuilder().sslContext(certificates.getSslContext(keyName)).build());
  }

  private WebTarget target(Client client) {
    checkState(application != null);
    application.configureBaseApplication(client);
    return client.target(baseUri);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private SslFixture certificates = null;
    private final HashMap<String, String> configs = new HashMap<>();
    private KafkaClusterFixture kafkaCluster = null;
    private String kafkaPassword = null;
    private String kafkaUser = null;
    private String keyName = null;
    private SchemaRegistryFixture schemaRegistry = null;

    private Builder() {}

    /**
     * Sets the SSL certificate store, and the name of the certificate to use as the Kafka REST
     * certificate.
     */
    public Builder setCertificates(SslFixture certificates, String keyName) {
      this.certificates = requireNonNull(certificates);
      this.keyName = requireNonNull(keyName);
      return this;
    }

    /** Sets a Kafka REST config. */
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

    public Builder setSchemaRegistry(SchemaRegistryFixture schemaRegistry) {
      this.schemaRegistry = requireNonNull(schemaRegistry);
      return this;
    }

    public KafkaRestFixture build() {
      return new KafkaRestFixture(
          certificates, configs, kafkaCluster, kafkaPassword, kafkaUser, keyName, schemaRegistry);
    }
  }
}
