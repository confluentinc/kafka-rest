/*
 * Copyright 2020 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.entities.v3.ConsumerAssignmentData;
import io.confluent.kafkarest.entities.v3.ConsumerAssignmentDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerAssignmentsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;

public class ConsumerAssignmentsResourceIntegrationTest extends ClusterTestHarness {

  public ConsumerAssignmentsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listConsumerAssignments_returnsConsumerAssignments() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    ListConsumerAssignmentsResponse expected =
        ListConsumerAssignmentsResponse.create(
            ConsumerAssignmentDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                                + "/consumer-group-1/consumers/"
                                + consumer.groupMetadata().memberId()
                                + "/assignments")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerAssignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer.groupMetadata().memberId()
                                            + "/assignments/topic-1/partitions/0")
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer.groupMetadata().memberId()
                                            + "/assignment=topic-1/partition=0")
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer.groupMetadata().memberId())
                            .setTopicName("topic-1")
                            .setPartitionId(0)
                            .setPartition(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/topics/topic-1"
                                        + "/partitions/0"))
                            .build(),
                        ConsumerAssignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer.groupMetadata().memberId()
                                            + "/assignments/topic-1/partitions/1")
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer.groupMetadata().memberId()
                                            + "/assignment=topic-1/partition=1")
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer.groupMetadata().memberId())
                            .setTopicName("topic-1")
                            .setPartitionId(1)
                            .setPartition(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/topics/topic-1"
                                        + "/partitions/1"))
                            .build(),
                        ConsumerAssignmentData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer.groupMetadata().memberId()
                                            + "/assignments/topic-1/partitions/2")
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer.groupMetadata().memberId()
                                            + "/assignment=topic-1/partition=2")
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer.groupMetadata().memberId())
                            .setTopicName("topic-1")
                            .setPartitionId(2)
                            .setPartition(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/topics/topic-1"
                                        + "/partitions/2"))
                            .build()))
                .build());

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers/"
                + consumer.groupMetadata().memberId() + "/assignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(ListConsumerAssignmentsResponse.class));
  }

  @Test
  public void listConsumerAssignments_nonExistingCluster_returnsNotFound() {
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/foobar/consumer-groups/consumer-group-1/consumers/"
                + consumer.groupMetadata().memberId() + "/assignments")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerAssignment_returnsConsumerAssignment() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    GetConsumerAssignmentResponse expected =
        GetConsumerAssignmentResponse.create(
            ConsumerAssignmentData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId
                                + "/consumer-groups/consumer-group-1/consumers/"
                                + consumer.groupMetadata().memberId()
                                + "/assignments/topic-1/partitions/0")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/consumer-group=consumer-group-1"
                                + "/consumer=" + consumer.groupMetadata().memberId()
                                + "/assignment=topic-1/partition=0")
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId("consumer-group-1")
                .setConsumerId(consumer.groupMetadata().memberId())
                .setTopicName("topic-1")
                .setPartitionId(0)
                .setPartition(
                    Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/topics/topic-1"
                            + "/partitions/0"))
                .build());

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/topic-1/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(GetConsumerAssignmentResponse.class));
  }

  @Test
  public void getConsumerAssignment_nonExistingCluster_returnsNotFound() {
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/foobar/consumer-groups/consumer-group-1"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/topic-1/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerAssignment_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/foobar"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/topic-1/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerAssignment_nonExistingConsumer_returnsNotFound() {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1"
                + "/consumers/foobar"
                + "/assignments/topic-1/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerAssignment_nonExistingConsumerAssignment_returnsNotFound() {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer = createConsumer("consumer-group-1", "client-1");
    consumer.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1"
                + "/consumers/" + consumer.groupMetadata().memberId()
                + "/assignments/foobar/partitions/0")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    properties.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }
}
