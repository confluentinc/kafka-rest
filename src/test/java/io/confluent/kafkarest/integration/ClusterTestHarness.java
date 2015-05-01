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

import io.confluent.kafkarest.*;
import org.I0Itec.zkclient.ZkClient;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;

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
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import scala.collection.JavaConversions;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
public abstract class ClusterTestHarness {

  public static final int DEFAULT_NUM_BROKERS = 1;

  // Shared config
  protected Queue<Integer> ports;

  // ZK Config
  protected int zkPort;
  protected String zkConnect;
  protected EmbeddedZookeeper zookeeper;
  protected ZkClient zkClient;
  protected int zkConnectionTimeout = 6000;
  protected int zkSessionTimeout = 6000;

  // Kafka Config
  protected List<Properties> configs = null;
  protected List<KafkaServer> servers = null;
  protected String brokerList = null;

  // Schema registry config
  protected String schemaRegCompatibility = AvroCompatibilityLevel.NONE.name;
  protected Properties schemaRegProperties = null;
  protected String schemaRegConnect = null;
  protected SchemaRegistryRestApplication schemaRegApp = null;
  protected Server schemaRegServer = null;

  protected String bootstrapServers = null;
  protected Properties restProperties = null;
  protected KafkaRestConfig restConfig = null;
  protected TestKafkaRestApplication restApp = null;
  protected Server restServer = null;
  protected String restConnect = null;

  public ClusterTestHarness() {
    this(DEFAULT_NUM_BROKERS, false);
  }

  public ClusterTestHarness(int numBrokers, boolean withSchemaRegistry) {
    // 1 port per broker + ZK + possibly SchemaReg + REST server
    int numPorts = numBrokers + 3;
    ports = new ArrayDeque<Integer>();
    for (Object portObj : JavaConversions.asJavaList(TestUtils.choosePorts(numPorts))) {
      ports.add((Integer) portObj);
    }
    zkPort = ports.remove();
    zkConnect = String.format("localhost:%d", zkPort);

    configs = new Vector<Properties>();
    bootstrapServers = "";
    for (int i = 0; i < numBrokers; i++) {
      int port = ports.remove();
      Properties props = TestUtils.createBrokerConfig(i, port, false);
      // Turn auto creation *off*, unlike the default. This lets us test errors that should be
      // generated when brokers are configured that way.
      props.put("auto.create.topics.enable", "false");
      // We *must* override this to use the port we allocated (Kafka currently allocates one port
      // that it always uses for ZK
      props.put("zookeeper.connect", this.zkConnect);
      configs.add(props);

      if (bootstrapServers.length() > 0) {
        bootstrapServers += ",";
      }
      bootstrapServers = bootstrapServers + "localhost:" + ((Integer) port).toString();
    }

    if (withSchemaRegistry) {
      schemaRegProperties = new Properties();
      int schemaRegPort = ports.remove();
      schemaRegProperties.put(SchemaRegistryConfig.PORT_CONFIG,
                              ((Integer) schemaRegPort).toString());
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG,
                              zkConnect);
      schemaRegProperties.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,
                              SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC);
      schemaRegProperties.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG,
                              schemaRegCompatibility);
      schemaRegConnect = String.format("http://localhost:%d", schemaRegPort);
    }

    restProperties = new Properties();
    int restPort = ports.remove();
    restProperties.put(KafkaRestConfig.PORT_CONFIG, ((Integer) restPort).toString());
    restProperties.put(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    if (withSchemaRegistry) {
      restProperties.put(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegConnect);
    }
    restConnect = String.format("http://localhost:%d", restPort);
  }

  public Properties overrideBrokerProperties(int i, Properties props) {
    return props;
  }

  @Before
  public void setUp() throws Exception {
    zookeeper = new EmbeddedZookeeper(zkConnect);
    zkClient =
        new ZkClient(zookeeper.connectString(), zkSessionTimeout, zkConnectionTimeout,
                     ZKStringSerializer$.MODULE$);

    if (configs == null || configs.size() <= 0) {
      throw new RuntimeException("Must supply at least one server config.");
    }

    List<KafkaConfig> specializedConfigs = new ArrayList<KafkaConfig>();
    for (int i = 0; i < configs.size(); i++) {
      Properties refinedProps = (Properties) configs.get(i).clone();
      refinedProps = overrideBrokerProperties(i, refinedProps);
      specializedConfigs.add(new KafkaConfig(refinedProps));
    }
    brokerList = TestUtils.getBrokerListStrFromConfigs(
        JavaConversions.asScalaIterable(specializedConfigs).toSeq());
    servers = new Vector<KafkaServer>(specializedConfigs.size());
    for (KafkaConfig config : specializedConfigs) {
      KafkaServer server = TestUtils.createServer(config, SystemTime$.MODULE$);
      servers.add(server);
    }

    if (schemaRegProperties != null) {
      schemaRegApp =
          new SchemaRegistryRestApplication(new SchemaRegistryConfig(schemaRegProperties));
      schemaRegServer = schemaRegApp.createServer();
      schemaRegServer.start();
    }

    restConfig = new KafkaRestConfig(restProperties);
    restApp = new TestKafkaRestApplication(restConfig, getZkClient(restConfig),
                                           getMetadataObserver(restConfig),
                                           getProducerPool(restConfig),
                                           getConsumerManager(restConfig),
                                           getSimpleConsumerFactory(restConfig),
                                           getSimpleConsumerManager(restConfig));
    restServer = restApp.createServer();
    restServer.start();
  }

  protected ZkClient getZkClient(KafkaRestConfig appConfig) {
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
      for (String logDir : JavaConversions.asJavaCollection(server.config().logDirs())) {
        Utils.rm(logDir);
      }
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

    Client client = ClientBuilder.newClient();
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
}
