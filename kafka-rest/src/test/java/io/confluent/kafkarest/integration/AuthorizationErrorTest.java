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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConversions;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuthorizationErrorTest extends AbstractProducerTest {

  private static final String TOPIC_NAME = "topic1";
  private static final String CONSUMER_GROUP = "app1-consumer-group";
  private static final String USERNAME = "alice";

  // Produce to topic inputs & results
  private final List<BinaryTopicProduceRecord> topicRecords = Arrays.asList(
      new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value2".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value3".getBytes()),
      new BinaryTopicProduceRecord("key".getBytes(), "value4".getBytes())
  );

  private final List<PartitionOffset> produceOffsets = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  @Before
  public void setUp() throws Exception {
    super.setUp();
    kafka.utils.TestUtils.createTopic(zkClient, TOPIC_NAME, 1, 1,
        JavaConversions.asScalaBuffer(this.servers),
        new Properties());
  }

  @Override
  protected Properties getBrokerProperties(int i) {

    final Option<SecurityProtocol>
        securityProtocolOption = Option.apply(SecurityProtocol.SASL_PLAINTEXT);
    Properties saslprops = new Properties();
    saslprops.setProperty("sasl.enabled.mechanisms","PLAIN");
    saslprops.setProperty("sasl.mechanism.inter.broker.protocol","PLAIN");
    Option<Properties> saslProperties = Option.apply(saslprops);
    Properties brokerProps = kafka.utils.TestUtils.createBrokerConfig(
        0, "", false, false, kafka.utils.TestUtils.RandomPort(), securityProtocolOption,
        Option.empty(), saslProperties, true, true, kafka.utils.TestUtils.RandomPort(),
        false, kafka.utils.TestUtils.RandomPort(), false, kafka.utils.TestUtils.RandomPort(), Option.empty(), 1,
        false);
    brokerProps.put(KafkaConfig.BrokerIdProp(), Integer.toString(i));
    brokerProps.put(KafkaConfig.ZkConnectProp(), zkConnect);
    brokerProps.setProperty("authorizer.class.name", SimpleAclAuthorizer.class.getName());
    brokerProps.setProperty("super.users", "User:admin");
    brokerProps.setProperty("listener.name.sasl_plaintext.plain.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"admin\" "
            + "password=\"admin-secret\" "
            + "user_admin=\"admin-secret\" "
            + "user_alice=\"alice-secret\"; ");
    return brokerProps;
  }

  protected void overrideKafkaRestConfigs(Properties restProperties) {
    restProperties.put(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    restProperties.put("client.security.protocol","SASL_PLAINTEXT");
    restProperties.put("client.sasl.mechanism", "PLAIN");
    restProperties.put("client.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + " username=\"" + USERNAME + "\""
            + " password=\"alice-secret\";"
    );
  }

  @Test
  public void testConsumerRequest() {
    //test wihout acls
    verifySubscribeToTopic(true);
    //add acls
    SecureTestUtils.setConsumerACls(zkConnect, TOPIC_NAME, USERNAME, CONSUMER_GROUP);
    verifySubscribeToTopic(false);
  }

  @Test
  public void testProducerAuthorization() {
    //test wihout acls
    verifySubscribeToTopic(true);
    //add acls
    SecureTestUtils.setConsumerACls(zkConnect, TOPIC_NAME, USERNAME, CONSUMER_GROUP);
    verifySubscribeToTopic(false);
  }

  private void produceToTopic(String role) {
    // test without any acls
    testProduceToTopicFails(TOPIC_NAME, topicRecords);
    //add acls
    SecureTestUtils.setProduceACls(zkConnect, TOPIC_NAME, USERNAME);
    testProduceToTopic(TOPIC_NAME, topicRecords, ByteArrayDeserializer.class.getName(),
       ByteArrayDeserializer.class.getName(), produceOffsets, false);
  }

  protected void testProduceToTopicFails(String topicName,
                                         List<? extends TopicProduceRecord> records) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName)
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));

    assertErrorResponse(Response.Status.FORBIDDEN, response,
        Errors.KAFKA_AUTHORIZATION_ERROR_CODE,
        "Not authorized to access topics",
        Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertEquals(Errors.KAFKA_AUTHORIZATION_ERROR_CODE, (int) pOffset.getErrorCode());
    }
  }

  private void verifySubscribeToTopic(boolean expectFailure) {
    Response createResponse = createConsumerInstance(CONSUMER_GROUP);
    assertOKResponse(createResponse, Versions.KAFKA_V2_JSON);

    //create group
    CreateConsumerInstanceResponse instanceResponse =
        TestUtils.tryReadEntityOrLog(createResponse, CreateConsumerInstanceResponse.class);
    assertNotNull(instanceResponse.getInstanceId());
    assertTrue("Base URI should contain the consumer instance ID",
        instanceResponse.getBaseUri().contains(instanceResponse.getInstanceId()));

    String topicJson = "{\"topics\":[\""+TOPIC_NAME+ "\"]}";

    //subscribe to group
    request(instanceResponse.getBaseUri() + "/subscription")
        .post(Entity.entity(topicJson, Versions.KAFKA_V2_JSON_JSON));

    //poll some records
    Response response = request(instanceResponse.getBaseUri() + "/records")
        .accept(Versions.KAFKA_V2_JSON).get();

    if (expectFailure) {
      assertErrorResponse(Response.Status.FORBIDDEN, response,
          Errors.KAFKA_AUTHORIZATION_ERROR_CODE,
          "Not authorized to access topics",
          Versions.KAFKA_V2_JSON);
    } else {
      assertOKResponse(response, Versions.KAFKA_V2_JSON);
    }
  }

  private Response createConsumerInstance(String groupName) {
    ConsumerInstanceConfig config = new ConsumerInstanceConfig(null, null, null,
        null, null, null, null);

    return request("/consumers/" + groupName)
        .post(Entity.entity(config, Versions.KAFKA_V2_JSON_JSON));
  }

  @Override
  protected SecurityProtocol getBrokerSecurityProtocol() {
    return SecurityProtocol.SASL_PLAINTEXT;
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
