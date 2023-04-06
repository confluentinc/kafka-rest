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

package io.confluent.kafkarest.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.exceptions.v3.ErrorResponse;
import io.confluent.kafkarest.testing.*;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag("IntegrationTest")
public class SchemaRegistrySaslInheritTest {
  private static final String TOPIC_NAME = "topic-1";

  @Order(1)
  @RegisterExtension
  public final JvmPropertyFileLoginModuleFixture jaasConfig =
      JvmPropertyFileLoginModuleFixture.builder()
          .setName("SchemaRegistryServer")
          .addUser("kafka-rest", "kafka-rest-pass", "user")
          .addUser("schema-registry", "schema-registry-pass", "user")
          .build();

  @Order(2)
  @RegisterExtension
  public final ZookeeperFixture zookeeper = ZookeeperFixture.create();

  @Order(3)
  @RegisterExtension
  public final KafkaClusterFixture kafkaCluster =
      KafkaClusterFixture.builder()
          .addUser("kafka-rest", "kafka-rest-pass")
          .addUser("schema-registry", "schema-registry-pass")
          .addSuperUser("kafka-rest")
          .addSuperUser("schema-registry")
          .setNumBrokers(3)
          .setSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
          .setZookeeper(zookeeper)
          .build();

  @Order(4)
  @RegisterExtension
  public final SchemaRegistryFixture schemaRegistry =
      SchemaRegistryFixture.builder()
          .setClientConfig("basic.auth.credentials.source", "USER_INFO")
          .setClientConfig("basic.auth.user.info", "schema-registry:schema-registry-pass")
          .setConfig("authentication.method", "BASIC")
          .setConfig("authentication.realm", "SchemaRegistryServer")
          .setConfig("authentication.roles", "user")
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("schema-registry", "schema-registry-pass")
          .build();

  @Order(5)
  @RegisterExtension
  public final KafkaRestFixture kafkaRest =
      KafkaRestFixture.builder()
          .setConfig("producer.max.block.ms", "5000")
          .setConfig("schema.registry.basic.auth.credentials.source", "SASL_INHERIT")
          .setKafkaCluster(kafkaCluster)
          .setKafkaUser("kafka-rest", "kafka-rest-pass")
          .setSchemaRegistry(schemaRegistry)
          .build();

  @BeforeEach
  public void setUp() throws Exception {
    kafkaCluster.createTopic(TOPIC_NAME, 3, (short) 1);
  }

  @Test
  public void produceAvroWithRawSchema() throws Exception {
    String clusterId = kafkaCluster.getClusterId();
    String key = "foo";
    String value = "bar";
    ProduceRequest request =
        ProduceRequest.builder()
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(key))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.AVRO)
                    .setRawSchema("{\"type\": \"string\"}")
                    .setData(TextNode.valueOf(value))
                    .build())
            .setOriginalSize(0L)
            .build();

    Response response =
        kafkaRest
            .target()
            .path("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = readProduceResponse(response);
    ConsumerRecord<Object, Object> produced =
        kafkaCluster.getRecord(
            TOPIC_NAME,
            actual.getPartitionId(),
            actual.getOffset(),
            schemaRegistry.createAvroDeserializer(),
            schemaRegistry.createAvroDeserializer());
    assertEquals(key, produced.key());
    assertEquals(value, produced.value());
  }

  private static ProduceResponse readProduceResponse(Response response) {
    response.bufferEntity();
    try {
      return response.readEntity(ProduceResponse.class);
    } catch (ProcessingException e) {
      throw new RuntimeException(response.readEntity(ErrorResponse.class).toString(), e);
    }
  }
}
