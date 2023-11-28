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

import static io.confluent.kafkarest.TestUtils.choosePort;
import static io.confluent.kafkarest.TestUtils.testWithRetry;
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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.QuorumTestHarness;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
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
  private static final long HALF_SECOND_MILLIS = 500L;

  public static final int DEFAULT_NUM_BROKERS = 1;
  public static final int MAX_MESSAGE_SIZE = (2 << 20) * 10; // 10 MiB

  private final int numBrokers;
  private final boolean withSchemaRegistry;
  private final boolean manageRest;

  // Quorum controller
  private final QuorumTestHarness quorumTestHarness = new QuorumTestHarness() {};
  protected String zkConnect;

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

  public Properties overrideBrokerProperties(int i, Properties props) {
    return props;
  }

  public Properties overrideSchemaRegistryProps(Properties props) {
    return props;
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    log.info("Starting controller of {}", getClass().getSimpleName());
    // start controller (either Zk or Kraft)
    quorumTestHarness.setUp(testInfo);
    zkConnect = quorumTestHarness.zkConnectOrNull();

    log.info("Starting setup of {}", getClass().getSimpleName());
    setupMethod();
    log.info("Completed setup of {}", getClass().getSimpleName());
  }

  // Calling setup() in this class calls the setup() from the calling sub-class, which includes the
  // createTopic calls, which then causes an infinite loop on topic creation.
  // Pulling out the functionality to a separate method so we can call it without this behaviour
  // getting in the way.
  private void setupMethod() throws Exception {
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
          SchemaRegistryConfig.LISTENERS_CONFIG,
          String.format("http://127.0.0.1:%d", schemaRegPort));
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

      schemaRegApp =
          new SchemaRegistryRestApplication(new SchemaRegistryConfig(schemaRegProperties));
      schemaRegServer = schemaRegApp.createServer();
      schemaRegServer.start();
      schemaRegApp.postServerStart();
    }

    if (manageRest) {
      startRest(null, null);
    }
  }

  protected void startRest(RequestLog.Writer requestLogWriter, String requestLogFormat)
      throws Exception {
    log.info("Setting up REST.");
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
    restProperties.put(
        "producer." + ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf((2 << 20) * 10));

    restConfig = new KafkaRestConfig(restProperties);
    restApp = new KafkaRestApplication(restConfig, "", null, requestLogWriter, requestLogFormat);

    try {
      restServer = restApp.createServer();
      restServer.start();
    } catch (IOException e1) { // sometimes we get an address already in use exception
      log.warn("IOException when attempting to start rest, trying again", e1);
      stopRest();
      Thread.sleep(Duration.ofSeconds(1).toMillis());
      try {
        startRest(null, null);
      } catch (IOException e2) {
        log.error("Restart of rest server failed", e2);
        throw e2;
      }
    }
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

  protected void overrideKafkaRestConfigs(Properties restProperties) {}

  protected Properties getBrokerProperties(int i) {
    Properties props =
        TestUtils.createBrokerConfig(
            i,
            zkConnect,
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
    props.setProperty("auto.create.topics.enable", "false");
    props.setProperty("message.max.bytes", String.valueOf(MAX_MESSAGE_SIZE));
    if (zkConnect != null) {
      // We *must* override this to use the port we allocated (Kafka currently allocates one port
      // that it always uses for ZK
      props.setProperty("zookeeper.connect", zkConnect);
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
    log.info("Stopping controller of {}", getClass().getSimpleName());
    quorumTestHarness.tearDown();
    log.info("Completed teardown of {}", getClass().getSimpleName());
  }

  private void tearDownMethod() throws Exception {

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
      String topicName, int numPartitions, short replicationFactor, Properties properties) {
    createTopic(
        topicName,
        Optional.of(numPartitions),
        Optional.of(replicationFactor),
        Optional.empty(),
        properties);
  }

  protected final void createTopic(
      String topicName,
      Optional<Integer> numPartitions,
      Optional<Short> replicationFactor,
      Optional<Map<Integer, List<Integer>>> replicasAssignments,
      Properties properties) {

    CreateTopicsResult result =
        createTopicCall(
            topicName, numPartitions, replicationFactor, replicasAssignments, properties);

    try {
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      pause();
      Set<String> topicNames = getTopicNames();
      if (topicNames.isEmpty()) { // Can restart because no topics exist yet
        log.warn("Restarting the environment as topic creation failed the first time");
        try {
          tearDownMethod();
          pause();
          setupMethod();
          pause();
        } catch (Exception tearDownException) {
          fail(String.format("Failed to create topic: %s", tearDownException.getMessage()));
        }

        result =
            createTopicCall(
                topicName, numPartitions, replicationFactor, replicasAssignments, properties);
        getTopicCreateFutures(result);
      } else if (topicNames.stream().noneMatch(topicName::equals)) {
        // The topic we are trying to make isn't the first topic to be created, the topic list isn't
        // 0 length (that or an exception has been thrown, but the topic was created anyway).
        // We can't easily restart the environment as we will lose the existing topics.
        // While we could store them and recreate them all, for now, let's just wait a bit and have
        // another go with the current topic.
        log.warn("Topic creation failed the first time round, trying again.");
        result =
            createTopicCall(
                topicName, numPartitions, replicationFactor, replicasAssignments, properties);
        pause(); // It's struggling at this point, give it a little time
        getTopicCreateFutures(result);
      } else {
        log.warn(
            String.format(
                "Exception thrown but topic  %s has been created, carrying on: %s",
                topicName, e.getMessage()));
      }
      if (!getTopicNames().stream().anyMatch(topicName::equals)) {
        fail(String.format("Failed to create topic after retry: %s", topicNames));
      }
    }
  }

  protected final void createTopic(
      String topicName, Map<Integer, List<Integer>> replicasAssignments) {
    createTopic(
        topicName,
        Optional.empty(),
        Optional.empty(),
        Optional.of(replicasAssignments),
        restConfig.getAdminProperties());
  }

  private CreateTopicsResult createTopicCall(
      String topicName,
      Optional<Integer> numPartitions,
      Optional<Short> replicationFactor,
      Optional<Map<Integer, List<Integer>>> replicasAssignments,
      Properties properties) {
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      if (replicasAssignments.isPresent()) {
        return adminClient.createTopics(
            Collections.singletonList(new NewTopic(topicName, replicasAssignments.get())));
      } else {
        return adminClient.createTopics(
            Collections.singletonList(new NewTopic(topicName, numPartitions, replicationFactor)));
      }
    }
  }

  private void pause() {
    try {
      Thread.sleep(HALF_SECOND_MILLIS);
    } catch (InterruptedException ie3) {
      // Noop
    }
  }

  private void getTopicCreateFutures(CreateTopicsResult result) {
    try {
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      fail(String.format("Failed to create topic: %s", e.getMessage()));
    }
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
}
