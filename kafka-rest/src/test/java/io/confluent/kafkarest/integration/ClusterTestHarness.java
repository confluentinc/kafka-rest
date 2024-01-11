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

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.common.CompletableFutures;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
@Tag("IntegrationTest")
public abstract class ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(ClusterTestHarness.class);

  public static final int DEFAULT_NUM_BROKERS = 1;

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

  private int numBrokers;
  private boolean withSchemaRegistry;
  // ZK Config
  protected String zkConnect;
  protected EmbeddedZookeeper zookeeper;
  protected int zkConnectionTimeout = 10000;
  protected int zkSessionTimeout = 6000;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;
  // used for test consumer
  protected String plaintextBrokerList = null;

  // Schema registry config
  protected String schemaRegCompatibility = AvroCompatibilityLevel.NONE.name;
  protected Properties schemaRegProperties = null;
  protected String schemaRegConnect = null;
  protected SchemaRegistryRestApplication schemaRegApp = null;
  protected Server schemaRegServer = null;

  protected Properties restProperties = null;
  protected KafkaRestConfig restConfig = null;
  protected KafkaRestApplication restApp = null;
  protected Server restServer = null;
  protected String restConnect = null;

  private static final long ONE_SECOND_MS = 1000L;

  public ClusterTestHarness() {
    this(DEFAULT_NUM_BROKERS, false);
  }

  public ClusterTestHarness(int numBrokers, boolean withSchemaRegistry) {
    this.numBrokers = numBrokers;
    this.withSchemaRegistry = withSchemaRegistry;

    schemaRegProperties = new Properties();
    restProperties = new Properties();
  }

  public Properties overrideBrokerProperties(int i, Properties props) {
    return props;
  }

  public Properties overrideSchemaRegistryProps(Properties props) {
    return props;
  }

  @BeforeEach
  public void setUp() throws Exception {
    log.info("Starting setup of {}", getClass().getSimpleName());
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
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
      int schemaRegPort = choosePort();
      schemaRegProperties.put(
          SchemaRegistryConfig.PORT_CONFIG, ((Integer) schemaRegPort).toString());
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
      schemaRegProperties.put(
          SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
          SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC);
      schemaRegProperties.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, schemaRegCompatibility);
      String broker =
          SecurityProtocol.PLAINTEXT.name
              + "://"
              + TestUtils.getBrokerListStrFromServers(
                  JavaConverters.asScalaBuffer(servers), SecurityProtocol.PLAINTEXT);
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, broker);
      schemaRegConnect = String.format("http://localhost:%d", schemaRegPort);

      schemaRegProperties = overrideSchemaRegistryProps(schemaRegProperties);

      schemaRegApp =
          new SchemaRegistryRestApplication(new SchemaRegistryConfig(schemaRegProperties));
      schemaRegServer = schemaRegApp.createServer();
      schemaRegServer.start();
    }

    int restPort = choosePort();
    restProperties.put(KafkaRestConfig.PORT_CONFIG, ((Integer) restPort).toString());
    restProperties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    overrideKafkaRestConfigs(restProperties);
    if (withSchemaRegistry) {
      restProperties.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegConnect);
    }
    restConnect = getRestConnectString(restPort);
    restProperties.put("listeners", restConnect);

    // Reduce the metadata fetch timeout so requests for topics that don't exist timeout much
    // faster than the default
    restProperties.put("producer." + ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");

    restConfig = new KafkaRestConfig(restProperties);

    try {
      startRest();
    } catch (IOException e) { // sometimes we get an address already in use exception
      log.warn("IOException when attempting to start rest, trying again", e);
      stopRest();
      Thread.sleep(ONE_SECOND_MS);
      try {
        startRest();
      } catch (IOException e2) {
        log.error("Restart of rest server failed", e2);
        throw e2;
      }
    }
    log.info("Completed setup of {}", getClass().getSimpleName());
  }

  private void startRest() throws Exception {
    restApp = new KafkaRestApplication(restConfig);
    restServer = restApp.createServer();
    restServer.start();
  }

  private void stopRest() throws Exception {
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
                                    TestUtils.createServer(
                                        config,
                                        new MockTime(
                                            System.currentTimeMillis(), System.nanoTime()))))
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

  protected void overrideKafkaRestConfigs(Properties restProperties) {}

  protected Properties getBrokerProperties(int i) {
    final Option<File> noFile = Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol =
        Option.apply(getBrokerSecurityProtocol());
    Properties props =
        TestUtils.createBrokerConfig(
            i,
            zkConnect,
            false,
            false,
            TestUtils.RandomPort(),
            noInterBrokerSecurityProtocol,
            noFile,
            Option.<Properties>empty(),
            true,
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            Option.<String>empty(),
            1,
            false,
            1,
            (short) 1);
    props.setProperty("auto.create.topics.enable", "false");
    // We *must* override this to use the port we allocated (Kafka currently allocates one port
    // that it always uses for ZK
    props.setProperty("zookeeper.connect", this.zkConnect);
    return props;
  }

  @AfterEach
  public void tearDown() throws Exception {
    log.info("Starting teardown of {}", getClass().getSimpleName());
    stopRest();

    if (schemaRegServer != null) {
      schemaRegServer.stop();
      schemaRegServer.join();
    }

    for (KafkaServer server : servers) {
      server.shutdown();
      server.metrics().close();
    }
    for (KafkaServer server : servers) {
      CoreUtils.delete(server.config().logDirs());
    }

    zookeeper.shutdown();
    log.info("Completed teardown of {}", getClass().getSimpleName());
  }

  protected Invocation.Builder request(String path) {
    return request(path, null, null, null);
  }

  protected Invocation.Builder request(String path, Map<String, String> queryParams) {
    return request(path, null, null, queryParams);
  }

  protected Invocation.Builder request(String path, String templateName, Object templateValue) {
    return request(path, templateName, templateValue, null);
  }

  protected Invocation.Builder request(
      String path, String templateName, Object templateValue, Map<String, String> queryParams) {

    Client client = getClient();
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
    AdminClient adminClient = AdminClient.create(properties);

    try {
      return adminClient.describeCluster().clusterId().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected final int getControllerID() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    try {
      return adminClient.describeCluster().controller().get().id();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected final ArrayList<Node> getBrokers() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    try {
      return new ArrayList<>(adminClient.describeCluster().nodes().get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected final Set<String> getTopicNames() {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    ListTopicsResult result = adminClient.listTopics();

    try {
      return result.names().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(String.format("Failed to create topic: %s", e.getMessage()));
    }
  }

  protected final void createTopic(String topicName, int numPartitions, short replicationFactor) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    createTopic(topicName, numPartitions, replicationFactor, properties);
  }

  protected final void createTopic(
      String topicName, int numPartitions, short replicationFactor, Properties properties) {
    AdminClient adminClient = AdminClient.create(properties);

    CreateTopicsResult result =
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(topicName, numPartitions, replicationFactor)));

    try {
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      fail(String.format("Failed to create topic: %s", e.getMessage()));
    }
  }

  protected final void setTopicConfig(String topicName, String configName, String value) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    AlterConfigsResult result =
        adminClient.incrementalAlterConfigs(
            singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, topicName),
                singletonList(
                    new AlterConfigOp(
                        new ConfigEntry(configName, value), AlterConfigOp.OpType.SET))));
    try {
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      fail(
          String.format(
              "Failed to alter config %s for topic %s: %s", configName, topicName, e.getMessage()));
    }
  }

  protected final void createTopic(
      String topicName, Map<Integer, List<Integer>> replicasAssignments) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    CreateTopicsResult result =
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(topicName, replicasAssignments)));

    try {
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      fail(String.format("Failed to create topic: %s", e.getMessage()));
    }
  }

  protected final void alterPartitionReassignment(
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    AdminClient adminClient = AdminClient.create(properties);

    adminClient.alterPartitionReassignments(reassignments);
  }

  protected final void produceAvroMessages(List<ProducerRecord<Object, Object>> records) {
    HashMap<String, Object> serProps = new HashMap<String, Object>();
    serProps.put("schema.registry.url", schemaRegConnect);
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(serProps, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
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
            producer.send(rec).get();
            sent = true;
          } catch (Exception e) {
            log.info("Produce failed within testWithRetry", e);
          }
          producer.close();
          assertTrue(sent);
        });
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
}
