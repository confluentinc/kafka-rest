package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.RestConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class KafkaRestEnvironment implements BeforeEachCallback, AfterEachCallback {

  private final KafkaClusterEnvironment kafkaCluster;
  private final HashMap<String, String> configs;

  @Nullable
  private URI baseUri;
  @Nullable
  private KafkaRestApplication application;
  @Nullable
  private Server server;

  private KafkaRestEnvironment(
      KafkaClusterEnvironment kafkaCluster, HashMap<String, String> configs) {
    this.kafkaCluster = requireNonNull(kafkaCluster);
    this.configs = requireNonNull(configs);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    checkState(server == null);
    baseUri = URI.create(String.format("http://localhost:%d", findUnusedPort()));
    application = new KafkaRestApplication(createConfigs(baseUri.toString()));
    server = application.createServer();
    server.start();
  }

  private KafkaRestConfig createConfigs(String baseUrl) {
    Properties properties = new Properties();
    properties.put(RestConfig.LISTENERS_CONFIG, baseUrl);
    properties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
    for (Map.Entry<String, String> config : configs.entrySet()) {
      properties.put(config.getKey(), config.getValue());
    }
    return new KafkaRestConfig(properties);
  }

  private static int findUnusedPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    checkState(server != null);
    server.stop();
    server = null;
    application = null;
    baseUri = null;
  }

  public URI getBaseUri() {
    checkState(server != null);
    return baseUri;
  }

  public Invocation.Builder request(String path) {
    checkState(server != null);
    Client client = ClientBuilder.newClient();
    application.configureBaseApplication(client);
    return client.target(baseUri).path(path).request();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final HashMap<String, String> configs = new HashMap<>();
    private KafkaClusterEnvironment kafkaCluster;

    private Builder() {
    }

    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    public Builder setKafkaCluster(KafkaClusterEnvironment kafkaCluster) {
      this.kafkaCluster = requireNonNull(kafkaCluster);
      return this;
    }

    public KafkaRestEnvironment build() {
      return new KafkaRestEnvironment(kafkaCluster, configs);
    }
  }
}
