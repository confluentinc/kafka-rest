/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.kafkarest.*;

import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.collection.JavaConversions;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
@Category(IntegrationTest.class)
public abstract class ClusterTestHarness {

  public static final int DEFAULT_NUM_BROKERS = 1;

  /**
   * Choose a number of random available ports
   */
  public static int[] choosePorts(int count) {
    try {
      ServerSocket[] sockets = new ServerSocket[count];
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        sockets[i] = new ServerSocket(0);
        ports[i] = sockets[i].getLocalPort();
      }
      for (int i = 0; i < count; i++)
        sockets[i].close();
      return ports;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Choose an available port
   */
  public static int choosePort() {
    return choosePorts(1)[0];
  }


  private int numBrokers;
  private boolean withSchemaRegistry;

  // ZK Config
  protected String zkConnect;
  protected EmbeddedZookeeper zookeeper;
  protected KafkaZkClient zkClient;
  protected int zkConnectionTimeout = 6000;
  protected int zkSessionTimeout = 6000;

  // Kafka Config
  protected List<KafkaConfig> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;

  // Schema registry config
  protected String schemaRegCompatibility = AvroCompatibilityLevel.NONE.name;
  protected Properties schemaRegProperties = null;
  protected String schemaRegConnect = null;
  protected SchemaRegistryRestApplication schemaRegApp = null;
  protected Server schemaRegServer = null;

  protected Properties restProperties = null;
  protected KafkaRestConfig restConfig = null;
  protected TestKafkaRestApplication restApp = null;
  protected Server restServer = null;
  protected String restConnect = null;

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

  @Before
  public void setUp() throws Exception {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    Time time = Time.SYSTEM;
    zkClient = new KafkaZkClient(
        new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Integer.MAX_VALUE, time,
                "testMetricGroup", "testMetricGroupType"),
        JaasUtils.isZkSecurityEnabled(),
        time);

    configs = new Vector<>();
    servers = new Vector<>();
    for (int i = 0; i < numBrokers; i++) {
      Properties props = getBrokerProperties(i);

      props = overrideBrokerProperties(i, props);

      KafkaConfig config = KafkaConfig.fromProps(props);
      configs.add(config);

      KafkaServer server = TestUtils.createServer(config, time);
      servers.add(server);
    }

    brokerList =
        TestUtils.getBrokerListStrFromServers(JavaConversions.asScalaBuffer(servers),
                                              getBrokerSecurityProtocol());

    setupAcls();
    if (withSchemaRegistry) {
      int schemaRegPort = choosePort();
      schemaRegProperties.put(SchemaRegistryConfig.PORT_CONFIG,
                              ((Integer) schemaRegPort).toString());
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG,
                              zkConnect);
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
                              SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC);
      schemaRegProperties.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG,
                              schemaRegCompatibility);
      String broker = SecurityProtocol.PLAINTEXT.name+"://"+TestUtils
          .getBrokerListStrFromServers(JavaConversions.asScalaBuffer
              (servers), SecurityProtocol.PLAINTEXT);
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, broker);
      schemaRegConnect = String.format("http://localhost:%d", schemaRegPort);

      schemaRegApp =
          new SchemaRegistryRestApplication(new SchemaRegistryConfig(schemaRegProperties));
      schemaRegServer = schemaRegApp.createServer();
      schemaRegServer.start();
    }

    int restPort = choosePort();
    restProperties.put(KafkaRestConfig.PORT_CONFIG, ((Integer) restPort).toString());
    restProperties.put(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    overrideKafkaRestConfigs(restProperties);
    if (withSchemaRegistry) {
      restProperties.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegConnect);
    }
    restConnect = getRestConnectString(restPort);
    restProperties.put("listeners",restConnect);

    restConfig = new KafkaRestConfig(restProperties);
    restApp = new TestKafkaRestApplication(restConfig, getZkUtils(restConfig),
                                           getMetadataObserver(restConfig),
                                           getProducerPool(restConfig),
                                           getConsumerManager(restConfig),
                                           getSimpleConsumerFactory(restConfig),
                                           getSimpleConsumerManager(restConfig));
    restServer = restApp.createServer();
    restServer.start();
  }

  protected void setupAcls() {
  }

  protected SecurityProtocol getBrokerSecurityProtocol(){
    return SecurityProtocol.PLAINTEXT;
  }

  protected String getRestConnectString(int restPort) {
    return String.format("http://localhost:%d", restPort);
  }

  protected void overrideKafkaRestConfigs(Properties restProperties) {

  }

  protected Properties getBrokerProperties(int i) {
    final Option<File> noFile = Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = Option.apply(null);
    Properties props = TestUtils.createBrokerConfig(
        i, zkConnect, false, false, TestUtils.RandomPort(), noInterBrokerSecurityProtocol,
        noFile, Option.<Properties>empty(), true, false, TestUtils.RandomPort(), false,
        TestUtils.RandomPort(), false, TestUtils.RandomPort(), Option.<String>empty(), 1, false);
    props.setProperty("auto.create.topics.enable", "false");
    // We *must* override this to use the port we allocated (Kafka currently allocates one port
    // that it always uses for ZK
    props.setProperty("zookeeper.connect", this.zkConnect);
    return props;
  }

  protected ZkUtils getZkUtils(KafkaRestConfig appConfig) {
    return null;
  }

  protected MetadataObserver getMetadataObserver(KafkaRestConfig appConfig) {
    return null;
  }

  protected ProducerPool getProducerPool(KafkaRestConfig appConfig) {
    return null;
  }

  protected ConsumerManager getConsumerManager(KafkaRestConfig appConfig) {
    return null;
  }

  protected SimpleConsumerFactory getSimpleConsumerFactory(KafkaRestConfig appConfig) {
    return null;
  }

  protected SimpleConsumerManager getSimpleConsumerManager(KafkaRestConfig appConfig) {
    return null;
  }

  @After
  public void tearDown() throws Exception {
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }

    if (schemaRegServer != null) {
      schemaRegServer.stop();
      schemaRegServer.join();
    }

    for (KafkaServer server : servers) {
      server.shutdown();
    }
    for (KafkaServer server : servers) {
      CoreUtils.delete(server.config().logDirs());
    }

    zkClient.close();
    zookeeper.shutdown();
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

  protected Invocation.Builder request(String path, String templateName, Object templateValue,
                                       Map<String, String> queryParams) {

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
}
