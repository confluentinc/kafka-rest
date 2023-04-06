/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

public class AuthorizationErrorTest
    extends AbstractProducerTest<BinaryTopicProduceRequest, BinaryPartitionProduceRequest> {

  private static final String TOPIC_NAME = "topic1";
  private static final String CONSUMER_GROUP = "app1-consumer-group";
  private static final String USERNAME = "alice";

  // Produce to topic inputs & results
  private final List<BinaryTopicProduceRecord> topicRecords =
      Arrays.asList(
          new BinaryTopicProduceRecord("key", "value", null),
          new BinaryTopicProduceRecord("key", "value2", null),
          new BinaryTopicProduceRecord("key", "value3", null),
          new BinaryTopicProduceRecord("key", "value4", null));

  private final List<PartitionOffset> produceOffsets =
      Arrays.asList(
          new PartitionOffset(0, 0L, null, null),
          new PartitionOffset(0, 1L, null, null),
          new PartitionOffset(0, 2L, null, null),
          new PartitionOffset(0, 3L, null, null));

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    Properties properties = restConfig.getAdminProperties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put("sasl.jaas.config", createPlainLoginModule("admin", "admin-secret"));
    createTopic(TOPIC_NAME, 1, (short) 1, properties);
  }

  @Override
  protected Properties getBrokerProperties(int i) {

    final Option<SecurityProtocol> securityProtocolOption =
        Option.apply(SecurityProtocol.SASL_PLAINTEXT);
    Properties saslProps = new Properties();
    saslProps.setProperty("sasl.enabled.mechanisms", "PLAIN");
    saslProps.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN");
    Option<Properties> saslProperties = Option.apply(saslProps);
    Properties brokerProps =
        kafka.utils.TestUtils.createBrokerConfig(
            0,
            "",
            false,
            false,
            kafka.utils.TestUtils.RandomPort(),
            securityProtocolOption,
            Option.empty(),
            saslProperties,
            true,
            true,
            kafka.utils.TestUtils.RandomPort(),
            false,
            kafka.utils.TestUtils.RandomPort(),
            false,
            kafka.utils.TestUtils.RandomPort(),
            Option.empty(),
            1,
            false,
            1,
            (short) 1);
    brokerProps.put(KafkaConfig.BrokerIdProp(), Integer.toString(i));
    brokerProps.put(KafkaConfig.ZkConnectProp(), zkConnect);
    brokerProps.setProperty("authorizer.class.name", AclAuthorizer.class.getName());
    brokerProps.setProperty("super.users", "User:admin");
    brokerProps.setProperty(
        "listener.name.sasl_plaintext.plain.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"admin\" "
            + "password=\"admin-secret\" "
            + "user_admin=\"admin-secret\" "
            + "user_alice=\"alice-secret\"; ");
    return brokerProps;
  }

  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    restProperties.put("client.security.protocol", "SASL_PLAINTEXT");
    restProperties.put("client.sasl.mechanism", "PLAIN");
    restProperties.put("client.sasl.jaas.config", createPlainLoginModule(USERNAME, "alice-secret"));
  }

  private static String createPlainLoginModule(String username, String password) {
    return "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + " username=\""
        + username
        + "\""
        + " password=\""
        + password
        + "\";";
  }

  @Test
  public void testConsumerRequest() {
    // test wihout acls
    verifySubscribeToTopic(true);
    // add acls
    SecureTestUtils.setConsumerAcls(zkConnect, TOPIC_NAME, USERNAME, CONSUMER_GROUP);
    verifySubscribeToTopic(false);
  }

  @Test
  public void testProducerAuthorization() {
    BinaryTopicProduceRequest request = BinaryTopicProduceRequest.create(topicRecords);
    // test without any acls
    testProduceToAuthorizationError(TOPIC_NAME, request);
    // add acls
    SecureTestUtils.setProduceAcls(zkConnect, TOPIC_NAME, USERNAME);
    testProduceToTopic(
        TOPIC_NAME,
        request,
        ByteArrayDeserializer.class.getName(),
        ByteArrayDeserializer.class.getName(),
        produceOffsets,
        false,
        request.toProduceRequest().getRecords());
  }

  private void verifySubscribeToTopic(boolean expectFailure) {
    Response createResponse = createConsumerInstance(CONSUMER_GROUP);
    assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);

    // create group
    CreateConsumerInstanceResponse instanceResponse =
        TestUtils.tryReadEntityOrLog(createResponse, CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue(
        instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()),
        "Base URI should contain the consumer instance ID");

    String topicJson = "{\"topics\":[\"" + TOPIC_NAME + "\"]}";

    // subscribe to group
    Response subscribe =
        request(instanceResponse.getBaseUri() + "/subscription")
            .post(Entity.entity(topicJson, Versions.KAFKA_V2_JSON));

    // poll some records
    Response response =
        request(instanceResponse.getBaseUri() + "/records")
            .accept(Versions.KAFKA_V2_JSON_BINARY)
            .get();

    if (expectFailure) {
      assertErrorResponse(
          Response.Status.FORBIDDEN,
          response,
          Errors.KAFKA_AUTHORIZATION_ERROR_CODE,
          "Not authorized to access topics",
          Versions.KAFKA_V2_JSON_BINARY);
    } else {
      assertOKResponse(response, Versions.KAFKA_V2_JSON_BINARY);
    }
  }

  private Response createConsumerInstance(String groupName) {
    CreateConsumerInstanceRequest config = CreateConsumerInstanceRequest.PROTOTYPE;

    return request("/consumers/" + groupName).post(Entity.entity(config, Versions.KAFKA_V2_JSON));
  }

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @Override
  protected void setupAcls() {
    // to allow plaintext consumer
    SecureTestUtils.setConsumerAcls(zkConnect, TOPIC_NAME, KafkaPrincipal.ANONYMOUS.getName(), "*");
  }

  @AfterEach
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
