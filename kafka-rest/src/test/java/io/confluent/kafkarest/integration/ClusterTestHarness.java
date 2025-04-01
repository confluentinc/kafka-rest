/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.integration;

import static com.google.common.base.Preconditions.checkState;
import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.rest.RestConfig;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import kafka.security.JaasTestUtils;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.QuorumTestHarness;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.DelegationTokenManagerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's QuorumTestHarness and KafkaServerTestHarness traits combined and ported to Java with the
 * addition of the REST proxy.
 */
@Tag("IntegrationTest")
public abstract class ClusterTestHarness {

  private static final Logger log = LogManager.getLogger(ClusterTestHarness.class);

  public static final int DEFAULT_NUM_BROKERS = 1;
  public static final int MAX_MESSAGE_SIZE = (2 << 20) * 10; // 10 MiB

  private final int numBrokers;
  private final boolean withSchemaRegistry;
  private final boolean manageRest;

  // Quorum controller
  private TestInfo testInfo;
  private QuorumTestHarness quorumTestHarness;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaBroker> servers = null;
  protected String brokerList = null;
  // used for test consumer
  protected String plaintextBrokerList = null;

  // Schema registry config
  protected String schemaRegCompatibility = CompatibilityLevel.NONE.name;
  protected Properties schemaRegProperties;
  protected String schemaRegConnect = null;
  protected SchemaRegistryRestApplication schemaRegApp = null;
  protected Server schemaRegServer = null;

  protected Properties restProperties;
  protected KafkaRestConfig restConfig = null;
  protected KafkaRestApplication restApp = null;
  protected Server restServer = null;
  protected String restConnect = null;

  public ClusterTestHarness() {
    this(DEFAULT_NUM_BROKERS, false);
  }

  public ClusterTestHarness(int numBrokers, boolean withSchemaRegistry) {
    this(numBrokers, withSchemaRegistry, true);
  }

  /** @param manageRest If false, child-class is expected to create and start/stop REST-app. */
  public ClusterTestHarness(int numBrokers, boolean withSchemaRegistry, boolean manageRest) {
    this.manageRest = manageRest;
    this.numBrokers = numBrokers;
    this.withSchemaRegistry = withSchemaRegistry;

    this.schemaRegProperties = new Properties();
    this.restProperties = new Properties();
  }

  /** Choose a number of random available ports */
  public static int[] choosePorts(int count) {
    try {
      ServerSocket[] sockets = new ServerSocket[count];
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        sockets[i] = new ServerSocket(0);
        ports[i] = sockets[i].getLocalPort();
      }
      for (int i = 0; i < count; i++) {
        sockets[i].close();
      }
      return ports;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Choose an available port */
  public static int choosePort() {
    return choosePorts(1)[0];
  }

  public Properties overrideBrokerProperties(int i, Properties props) {
    return props;
  }

  public Properties overrideSchemaRegistryProps(Properties props) {
    return props;
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    this.testInfo = testInfo;
    log.info("Starting setup of {}", getClass().getSimpleName());
    setupMethod();
    log.info("Completed setup of {}", getClass().getSimpleName());
  }

  // Calling setup() in this class calls the setup() from the calling sub-class, which includes the
  // createTopic calls, which then causes an infinite loop on topic creation.
  // Pulling out the functionality to a separate method so we can call it without this behaviour
  // getting in the way.
  private void setupMethod() throws Exception {
    checkState(testInfo != null);
    log.info("Starting controller of {}", getClass().getSimpleName());
    // start controller (either Zk or Kraft)
    this.quorumTestHarness =
        new DefaultQuorumTestHarness(
            overrideKraftControllerSecurityProtocol(), overrideKraftControllerConfig());
    quorumTestHarness.setUp(testInfo);

    // start brokers concurrently
    startBrokersConcurrently(numBrokers);

    brokerList =
        TestUtils.getBrokerListStrFromServers(
            JavaConverters.asScalaBuffer(servers), getBrokerSecurityProtocol());
    plaintextBrokerList =
        TestUtils.getBrokerListStrFromServers(
            JavaConverters.asScalaBuffer(servers), SecurityProtocol.PLAINTEXT);

    setupAcls();
    if (withSchemaRegistry) {
      doStartSchemaRegistry();
    }
    if (manageRest) {
      startRest(brokerList, null, null);
    }
  }

  private void doStartSchemaRegistry() throws Exception {
    int schemaRegPort = choosePort();
    schemaRegProperties.put(
        SchemaRegistryConfig.LISTENERS_CONFIG, String.format("http://127.0.0.1:%d", schemaRegPort));
    schemaRegProperties.put(
        SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
        SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC);
    schemaRegProperties.put(
        SchemaRegistryConfig.SCHEMA_COMPATIBILITY_CONFIG, schemaRegCompatibility);
    String broker =
        SecurityProtocol.PLAINTEXT.name
            + "://"
            + TestUtils.getBrokerListStrFromServers(
                JavaConverters.asScalaBuffer(servers), SecurityProtocol.PLAINTEXT);
    schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, broker);
    schemaRegConnect = String.format("http://localhost:%d", schemaRegPort);

    schemaRegProperties = overrideSchemaRegistryProps(schemaRegProperties);

    schemaRegApp = new SchemaRegistryRestApplication(new SchemaRegistryConfig(schemaRegProperties));
    schemaRegServer = schemaRegApp.createServer();
    schemaRegServer.start();
    schemaRegApp.postServerStart();
  }

  protected void startRest(RequestLog.Writer requestLogWriter, String requestLogFormat)
      throws Exception {
    startRest(brokerList, requestLogWriter, requestLogFormat);
  }

  protected void startRest(
      String bootstrapServers, RequestLog.Writer requestLogWriter, String requestLogFormat)
      throws Exception {
    if (restServer != null && restServer.isRunning()) {
      log.warn("Rest server already started, skipping start");
      return;
    }
    log.info("Setting up REST.");
    restProperties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    overrideKafkaRestConfigs(restProperties);
    if (withSchemaRegistry && schemaRegConnect != null) {
      restProperties.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegConnect);
    }
    restProperties.put(RestConfig.LISTENERS_CONFIG, getRestConnectString(0));

    // Reduce the metadata fetch timeout so requests for topics that don't exist timeout much
    // faster than the default
    restProperties.put("producer." + ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
    restProperties.put(
        "producer." + ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf((2 << 20) * 10));

    restConfig = new KafkaRestConfig(restProperties);

    try {
      doStartRest(requestLogWriter, requestLogFormat);
    } catch (IOException e) { // sometimes we get an address already in use exception
      log.warn("IOException when attempting to start rest, trying again", e);
      stopRest();
      Thread.sleep(Duration.ofSeconds(1).toMillis());
      try {
        doStartRest(requestLogWriter, requestLogFormat);
      } catch (IOException e2) {
        log.error("Restart of rest server failed", e2);
        throw e2;
      }
    }
    restConnect = getRestConnectString(restServer.getURI().getPort());
  }

  /**
   * Return the bootstrap servers string for the given security protocol.
   *
   * @param securityProtocol security protocol
   * @return bootstrap servers string
   */
  public String getBootstrapServers(SecurityProtocol securityProtocol) {
    return TestUtils.getBrokerListStrFromServers(
        JavaConverters.asScalaBuffer(servers), securityProtocol);
  }

  /**
   * Return the bootstrap servers string for the given listener name.
   *
   * @param listenerName listener name
   * @return bootstrap servers string
   */
  public String getBootstrapServers(ListenerName listenerName) {
    return TestUtils.bootstrapServers(JavaConverters.asScalaBuffer(servers), listenerName);
  }

  private void doStartRest(RequestLog.Writer requestLogWriter, String requestLogFormat)
      throws Exception {
    restApp = new KafkaRestApplication(restConfig, "", null, requestLogWriter, requestLogFormat);
    restServer = restApp.createServer();
    restServer.start();
  }

  protected void stopRest() throws Exception {
    log.info("Stopping REST.");
    restProperties.clear();
    if (restApp != null) {
      restApp.stop();
      restApp.getMetrics().close();
      restApp.getMetrics().metrics().clear();
    }
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }

  protected Time brokerTime(int brokerId) {
    return Time.SYSTEM;
  }

  private void startBrokersConcurrently(int numBrokers) {
    log.info("Starting concurrently {} brokers for {}", numBrokers, getClass().getSimpleName());
    configs =
        IntStream.range(0, numBrokers)
            .mapToObj(
                brokerId ->
                    KafkaConfig.fromProps(
                        overrideBrokerProperties(brokerId, getBrokerProperties(brokerId))))
            .collect(toList());
    servers =
        CompletableFutures.allAsList(
                configs.stream()
                    .map(
                        config ->
                            CompletableFuture.supplyAsync(
                                () ->
                                    quorumTestHarness.createBroker(
                                        config,
                                        brokerTime(config.brokerId()),
                                        true,
                                        Option.empty())))
                    .collect(toList()))
            .join();
    log.info("Started all {} brokers for {}", numBrokers, getClass().getSimpleName());
  }

  protected void setupAcls() {}

  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected String getRestConnectString(int restPort) {
    return String.format("http://localhost:%d", restPort);
  }

  /** Only applicable in Kraft tests, no effect in Zk tests */
  protected Properties overrideKraftControllerConfig() {
    return new Properties();
  }

  /** Only applicable in Kraft tests, no effect in Zk tests */
  protected SecurityProtocol overrideKraftControllerSecurityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected void overrideKafkaRestConfigs(Properties restProperties) {}

  protected Properties getBrokerProperties(int i) {
    Properties props =
        createBrokerConfig(
            i,
            false,
            false,
            TestUtils.RandomPort(),
            Option.apply(getBrokerSecurityProtocol()),
            Option.apply(null),
            Option.empty(),
            true,
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            Option.empty(),
            1,
            false,
            1,
            (short) 1,
            false);
    // Make sure that broker only role is "broker"
    props.setProperty("process.roles", "broker");
    props.setProperty("auto.create.topics.enable", "false");
    props.setProperty("message.max.bytes", String.valueOf(MAX_MESSAGE_SIZE));
    return props;
  }

  private static boolean shouldEnable(
      Option<SecurityProtocol> interBrokerSecurityProtocol, SecurityProtocol protocol) {
    if (interBrokerSecurityProtocol.isDefined()) {
      return interBrokerSecurityProtocol.get() == protocol;
    }
    return false;
  }

  /**
   * Taken from the same function name in kafka.utils.TestUtils of AK
   * https://github.com/confluentinc/kafka/blob/0ce5fb0dbb87661e794cdfc40badbe3b91d8d825/core/src/test/scala
   * /unit/kafka/utils/TestUtils.scala#L228
   */
  public static Properties createBrokerConfig(
      int nodeId,
      boolean enableControlledShutdown,
      boolean enableDeleteTopic,
      int port,
      Option<SecurityProtocol> interBrokerSecurityProtocol,
      Option<File> trustStoreFile,
      Option<Properties> saslProperties,
      boolean enablePlaintext,
      boolean enableSaslPlaintext,
      int saslPlaintextPort,
      boolean enableSsl,
      int sslPort,
      boolean enableSaslSsl,
      int saslSslPort,
      Option<String> rack,
      int logDirCount,
      boolean enableToken,
      int numPartitions,
      short defaultReplicationFactor,
      boolean enableFetchFromFollower) {
    List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts = new ArrayList<>();
    if (enablePlaintext || shouldEnable(interBrokerSecurityProtocol, SecurityProtocol.PLAINTEXT)) {
      protocolAndPorts.add(new SimpleEntry<>(SecurityProtocol.PLAINTEXT, port));
    }
    if (enableSsl || shouldEnable(interBrokerSecurityProtocol, SecurityProtocol.SSL)) {
      protocolAndPorts.add(new SimpleEntry<>(SecurityProtocol.SSL, sslPort));
    }
    if (enableSaslPlaintext
        || shouldEnable(interBrokerSecurityProtocol, SecurityProtocol.SASL_PLAINTEXT)) {
      protocolAndPorts.add(new SimpleEntry<>(SecurityProtocol.SASL_PLAINTEXT, saslPlaintextPort));
    }
    if (enableSaslSsl || shouldEnable(interBrokerSecurityProtocol, SecurityProtocol.SASL_SSL)) {
      protocolAndPorts.add(new SimpleEntry<>(SecurityProtocol.SASL_SSL, saslSslPort));
    }

    String listeners =
        protocolAndPorts.stream()
            .map(a -> String.format("%s://localhost:%d", a.getKey().name, a.getValue()))
            .collect(Collectors.joining(","));

    Properties props = new Properties();
    props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
    props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
    props.setProperty(
        KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(10)));
    props.put(KRaftConfigs.NODE_ID_CONFIG, String.valueOf(nodeId));
    props.put(ServerConfigs.BROKER_ID_CONFIG, String.valueOf(nodeId));
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners);
    props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners);
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
    props.put(
        SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
        protocolAndPorts.stream()
                .map(p -> String.format("%s:%s", p.getKey(), p.getKey()))
                .collect(Collectors.joining(","))
            + ",CONTROLLER:PLAINTEXT");

    if (logDirCount > 1) {
      String logDirs =
          IntStream.rangeClosed(1, logDirCount)
              .mapToObj(
                  i -> {
                    // We would like to allow user to specify both relative path and absolute path
                    // as log directory for backward-compatibility reason
                    // We can verify this by using a mixture of relative path and absolute path as
                    // log directories in the test
                    return (i % 2 == 0)
                        ? TestUtils.tempDir().getAbsolutePath()
                        : TestUtils.tempRelativeDir("data").getAbsolutePath();
                  })
              .collect(Collectors.joining(","));
      props.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDirs);
    } else {
      props.put(ServerLogConfigs.LOG_DIR_CONFIG, TestUtils.tempDir().getAbsolutePath());
    }
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
    // Note: this is just a placeholder value for controller.quorum.voters. JUnit
    // tests use random port assignment, so the controller ports are not known ahead of
    // time. Therefore, we ignore controller.quorum.voters and use
    // controllerQuorumVotersFuture instead.
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:0");
    props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1500");
    props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1500");
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, enableControlledShutdown);
    props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, enableDeleteTopic);
    props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000");
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "100");
    if (!props.containsKey(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG)) {
      props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5");
    }
    if (!props.containsKey(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG)) {
      props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
    }
    if (rack.isDefined()) {
      props.put(ServerConfigs.BROKER_RACK_CONFIG, rack.get());
    }
    // Reduce number of threads per broker
    props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2");
    props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "2");

    if (protocolAndPorts.stream().anyMatch(p -> JaasTestUtils.usesSslTransportLayer(p.getKey()))) {
      try {
        props.putAll(
            JaasTestUtils.sslConfigs(
                ConnectionMode.SERVER,
                false,
                trustStoreFile.isEmpty() ? Optional.empty() : Optional.of(trustStoreFile.get()),
                String.format("server%d", nodeId)));
      } catch (Exception e) {
        fail("Failed to create SSL configs", e);
      }
    }

    if (protocolAndPorts.stream().anyMatch(p -> JaasTestUtils.usesSaslAuthentication(p.getKey()))) {
      props.putAll(
          JaasTestUtils.saslConfigs(
              saslProperties.isEmpty() ? Optional.empty() : Optional.of(saslProperties.get())));
    }

    if (interBrokerSecurityProtocol.isDefined()) {
      props.put(
          ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG,
          interBrokerSecurityProtocol.get().name);
    }
    if (enableToken) {
      props.put(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "secretkey");
    }

    props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, String.valueOf(numPartitions));
    props.put(
        ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG,
        String.valueOf(defaultReplicationFactor));

    if (enableFetchFromFollower) {
      props.put(ServerConfigs.BROKER_RACK_CONFIG, String.valueOf(nodeId));
      props.put(
          ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG,
          "org.apache.kafka.common.replica.RackAwareReplicaSelector");
    }
    return props;
  }

  @AfterEach
  public void tearDown() throws Exception {
    log.info("Starting teardown of {}", getClass().getSimpleName());
    if (manageRest) {
      stopRest();
    }
    tearDownMethod();
    log.info("Completed teardown of {}", getClass().getSimpleName());
  }

  private void tearDownMethod() throws Exception {
    checkState(quorumTestHarness != null);
    restProperties.clear();

    schemaRegProperties.clear();
    if (schemaRegApp != null) {
      schemaRegApp.stop();
    }

    if (schemaRegServer != null) {
      schemaRegServer.stop();
      schemaRegServer.join();
    }

    TestUtils.shutdownServers(JavaConverters.asScalaBuffer(servers), true);

    log.info("Stopping controller of {}", getClass().getSimpleName());
    quorumTestHarness.tearDown();
  }

  protected Invocation.Builder request(String path, boolean useAlternateConnectorProvider) {
    return request(path, null, null, null, true);
  }

  protected Invocation.Builder request(String path) {
    return request(path, null, null, null, false);
  }

  protected Invocation.Builder request(String path, Map<String, String> queryParams) {
    return request(path, null, null, queryParams, false);
  }

  protected Invocation.Builder request(String path, String templateName, Object templateValue) {
    return request(path, templateName, templateValue, null, false);
  }

  protected Invocation.Builder request(
      String path,
      String templateName,
      Object templateValue,
      Map<String, String> queryParams,
      boolean useAlternateConnectorProvider) {

    Client client;
    if (useAlternateConnectorProvider) {
      // The PATCH method requires a workaround of
      // .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true) to be set for the
      // Jersey client we use for testing to recognise the PATCH call.
      // The default client does not support this property at Java 17 (it does at Java 8)
      // so we need to use an alternative provider for any tests that use PATCH.
      // Leaving existing client behaviour for other integration tests so we don't unintentionally
      // change other test behaviour
      ClientConfig clientConfig = new ClientConfig();
      clientConfig.connectorProvider(new ApacheConnectorProvider());
      client =
          ClientBuilder.newClient(clientConfig)
              .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
    } else {
      client = getClient();
    }

    // Only configure base application here because as a client we shouldn't need the resources
    // registered
    restApp.configureBaseApplication(client);
    WebTarget target;
    URI pathUri = null;
    try {
      pathUri = new URI(path);
    } catch (URISyntaxException e) {
      // Ignore, use restConnect and assume this is a valid path part
    }
    if (pathUri != null && pathUri.isAbsolute()) {
      target = client.target(path);
    } else {
      target = client.target(restConnect).path(path);
    }
    if (templateName != null && templateValue != null) {
      target = target.resolveTemplate(templateName, templateValue);
    }
    if (queryParams != null) {
      for (Map.Entry<String, String> queryParam : queryParams.entrySet()) {
        target = target.queryParam(queryParam.getKey(), queryParam.getValue());
      }
    }
    return target.request();
  }

  protected Client getClient() {
    return ClientBuilder.newClient();
  }

  protected final String getClusterId() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      return adminClient.describeCluster().clusterId().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /** This method is non-deterministic on Kraft cluster, please use this with care */
  protected final int getControllerID() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      return adminClient.describeCluster().controller().get().id();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected final ArrayList<Node> getBrokers() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      return new ArrayList<>(adminClient.describeCluster().nodes().get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected final Set<String> getTopicNames() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      return adminClient.listTopics().names().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(String.format("Failed to get topic: %s", e.getMessage()));
    }
  }

  protected final void createTopic(String topicName, int numPartitions, short replicationFactor) {
    createTopic(topicName, numPartitions, replicationFactor, restConfig.getAdminProperties());
  }

  protected final void createTopic(
      String topicName, Map<Integer, List<Integer>> replicasAssignments) {
    createTopic(
        topicName,
        Optional.empty(),
        Optional.empty(),
        Optional.of(replicasAssignments),
        restConfig.getAdminProperties(),
        new Properties());
  }

  protected final void createTopic(
      String topicName, int numPartitions, short replicationFactor, Properties adminProperties) {
    createTopic(
        topicName,
        Optional.of(numPartitions),
        Optional.of(replicationFactor),
        Optional.empty(),
        adminProperties,
        new Properties());
  }

  protected final void createTopic(
      String topicName,
      int numPartitions,
      short replicationFactor,
      Properties adminProperties,
      Properties topicConfig) {
    createTopic(
        topicName,
        Optional.of(numPartitions),
        Optional.of(replicationFactor),
        Optional.empty(),
        adminProperties,
        topicConfig);
  }

  protected final void createTopic(
      String topicName,
      Optional<Integer> numPartitions,
      Optional<Short> replicationFactor,
      Optional<Map<Integer, List<Integer>>> replicasAssignments,
      Properties adminProperties,
      Properties topicConfig) {
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProperties)) {
      TestUtils.createTopicWithAdmin(
          admin,
          topicName,
          JavaConverters.asScalaBuffer(servers),
          quorumTestHarness.controllerServers(),
          numPartitions.orElse(1),
          replicationFactor.orElse((short) 1),
          JavaConverters.mapAsScalaMapConverter(
                  convertReplicasAssignmentToScalaCompatibleType(replicasAssignments))
              .asScala(),
          topicConfig);
    }
  }

  protected final void createAcls(Collection<AclBinding> acls, Properties adminProperties)
      throws Exception {
    try (AdminClient admin = AdminClient.create(adminProperties)) {
      admin.createAcls(acls).all().get(60, TimeUnit.SECONDS);
    }
  }

  @SuppressWarnings({"unchecked"})
  private static Map<Object, Seq<Object>> convertReplicasAssignmentToScalaCompatibleType(
      Optional<Map<Integer, List<Integer>>> replicasAssignments) {
    return replicasAssignments
        .<Map<Integer, Seq<Integer>>>map(
            integerListMap ->
                integerListMap.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Entry::getKey, e -> JavaConverters.asScalaBuffer(e.getValue()))))
        .orElse(EMPTY_MAP);
  }

  protected final void setTopicConfig(String topicName, String configName, String value) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      adminClient
          .incrementalAlterConfigs(
              singletonMap(
                  new ConfigResource(ConfigResource.Type.TOPIC, topicName),
                  singletonList(
                      new AlterConfigOp(
                          new ConfigEntry(configName, value), AlterConfigOp.OpType.SET))))
          .all()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      fail(
          String.format(
              "Failed to alter config %s for topic %s: %s", configName, topicName, e.getMessage()));
    }
  }

  protected final void alterPartitionReassignment(
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      adminClient.alterPartitionReassignments(reassignments);
    }
  }

  protected final void produceAvroMessages(List<ProducerRecord<Object, Object>> records) {
    HashMap<String, Object> serProps = new HashMap<>();
    serProps.put("schema.registry.url", schemaRegConnect);
    try (final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
        final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer(); ) {
      avroKeySerializer.configure(serProps, true);
      avroValueSerializer.configure(serProps, false);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

      for (ProducerRecord<Object, Object> rec : records) {
        doProduce(rec, () -> new KafkaProducer<>(props, avroKeySerializer, avroValueSerializer));
      }
    }
  }

  protected final void produceBinaryMessages(List<ProducerRecord<byte[], byte[]>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    for (ProducerRecord<byte[], byte[]> rec : records) {
      doProduce(rec, () -> new KafkaProducer<>(props));
    }
  }

  protected final void produceJsonMessages(List<ProducerRecord<Object, Object>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    for (ProducerRecord<Object, Object> rec : records) {
      doProduce(rec, () -> new KafkaProducer<>(props));
    }
  }

  private <T> void doProduce(
      ProducerRecord<T, T> rec, Supplier<KafkaProducer<T, T>> createProducer) {

    testWithRetry(
        () -> {
          final KafkaProducer<T, T> producer = createProducer.get();
          boolean sent = false;
          try {
            RecordMetadata result = producer.send(rec).get();
            if (result.hasOffset()) {
              sent = true;
            } else {
              log.info(
                  "Failed to get offset back from produce for record {}.  Result is {}",
                  rec,
                  result);
            }
          } catch (Exception e) {
            log.info("Produce failed within testWithRetry", e);
          }
          producer.close();
          assertTrue(sent);
        });
  }

  protected final <K, V> ConsumerRecord<K, V> getMessage(
      String topic,
      int partition,
      long offset,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    TopicPartition tp = new TopicPartition(topic, partition);
    consumer.assign(Collections.singleton(tp));
    consumer.seek(tp, offset);

    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(60));
    consumer.close();

    return records.isEmpty() ? null : records.records(tp).get(0);
  }

  protected final <K, V> ConsumerRecords<K, V> getMessages(
      String topic,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      int messageCount) {

    List<ConsumerRecord<K, V>> accumulator = new ArrayList<>(messageCount);
    int numMessages = 0;

    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    TopicPartition tp = new TopicPartition(topic, 0);
    consumer.assign(Collections.singleton(tp));
    consumer.seekToBeginning(Collections.singleton(tp));

    ConsumerRecords<K, V> records;
    while (numMessages < messageCount) {
      records = consumer.poll(Duration.ofSeconds(60));
      Iterator<ConsumerRecord<K, V>> it = records.iterator();
      while (it.hasNext() && (numMessages < messageCount)) {
        ConsumerRecord<K, V> rec = it.next();
        accumulator.add(rec);
        numMessages++;
      }
    }
    consumer.close();

    return new ConsumerRecords<>(Collections.singletonMap(tp, accumulator));
  }

  protected ObjectMapper getObjectMapper() {
    return restApp.getJsonMapper();
  }

  protected Map<Integer, List<Integer>> createAssignment(
      List<Integer> replicaIds, int numReplicas) {
    Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    for (int i = 0; i < numReplicas; i++) {
      replicaAssignments.put(i, replicaIds);
    }
    return replicaAssignments;
  }

  protected Map<TopicPartition, Optional<NewPartitionReassignment>> createReassignment(
      List<Integer> replicaIds, String topicName, int numReplicas) {
    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentMap = new HashMap<>();
    for (int i = 0; i < numReplicas; i++) {
      reassignmentMap.put(
          new TopicPartition(topicName, i), Optional.of(new NewPartitionReassignment(replicaIds)));
    }
    return reassignmentMap;
  }

  protected TestInfo getTestInfo() {
    return testInfo;
  }

  protected QuorumTestHarness getQuorumTestHarness() {
    return quorumTestHarness;
  }

  /** A concrete class of QuorumTestHarness so that we can customize for testing purposes */
  static class DefaultQuorumTestHarness extends QuorumTestHarness {
    private final Properties kraftControllerConfig;
    private final SecurityProtocol securityProtocol;

    DefaultQuorumTestHarness(SecurityProtocol securityProtocol, Properties kraftControllerConfig) {
      this.securityProtocol = securityProtocol;
      this.kraftControllerConfig = kraftControllerConfig;
    }

    @Override
    public SecurityProtocol controllerListenerSecurityProtocol() {
      return securityProtocol;
    }

    @Override
    public Seq<Properties> kraftControllerConfigs(TestInfo testInfo) {
      // only one Kraft controller is supported in QuorumTestHarness
      return JavaConverters.asScalaBuffer(Collections.singletonList(kraftControllerConfig));
    }
  }
}
