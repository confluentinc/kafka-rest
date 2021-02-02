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

package io.confluent.kafkarest.integration.v3;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import io.confluent.kafkarest.entities.v3.ProduceRequest.ProduceRequestData;
import io.confluent.kafkarest.entities.v3.ProduceResponse;
import io.confluent.kafkarest.testing.KafkaClusterEnvironment;
import io.confluent.kafkarest.testing.KafkaRestEnvironment;
import io.confluent.kafkarest.testing.SchemaRegistryEnvironment;
import io.confluent.kafkarest.testing.ZookeeperEnvironment;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ProduceActionIntegrationTest {

  private static final String TOPIC_NAME = "topic-1";

  @Order(1)
  @RegisterExtension
  public static final ZookeeperEnvironment zookeeper = ZookeeperEnvironment.create();

  @Order(2)
  @RegisterExtension
  public static final KafkaClusterEnvironment kafkaCluster =
      KafkaClusterEnvironment.builder()
          .setZookeeper(zookeeper)
          .setNumBrokers(3)
          .build();

  @Order(3)
  @RegisterExtension
  public static final SchemaRegistryEnvironment schemaRegistry =
      SchemaRegistryEnvironment.builder()
          .setKafkaCluster(kafkaCluster)
          .build();

  @Order(4)
  @RegisterExtension
  public static final KafkaRestEnvironment kafkaRest =
      KafkaRestEnvironment.builder()
          .setKafkaCluster(kafkaCluster)
          .setSchemaRegistry(schemaRegistry)
          .build();

  @BeforeEach
  public void setUp() throws Exception {
    kafkaCluster.createTopic(
        TOPIC_NAME, /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
  }

  @Test
  public void foobar() {
    String clusterId = kafkaCluster.getClusterId();
    ProduceRequest request =
        ProduceRequest.builder()
            .setHeaders(emptyList())
            .setKey(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("foo").toByteArray()))
                    .build())
            .setValue(
                ProduceRequestData.builder()
                    .setFormat(EmbeddedFormat.BINARY)
                    .setData(BinaryNode.valueOf(ByteString.copyFromUtf8("bar").toByteArray()))
                    .build())
            .build();

    Response response =
        kafkaRest.request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_NAME + "/records")
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ProduceResponse actual = response.readEntity(ProduceResponse.class);
    ConsumerRecord<ByteString, ByteString> record =
        kafkaCluster.getRecord(TOPIC_NAME, actual.getPartitionId(), actual.getOffset());
    assertEquals(ByteString.copyFromUtf8("foo"), record.key());
    assertEquals(ByteString.copyFromUtf8("bar"), record.value());
  }
}
