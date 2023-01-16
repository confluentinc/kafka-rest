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

import io.confluent.kafkarest.entities.v3.ConsumerData;
import io.confluent.kafkarest.entities.v3.ConsumerDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerResponse;
import io.confluent.kafkarest.entities.v3.ListConsumersResponse;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumersResourceIntegrationTest extends ClusterTestHarness {

  public ConsumersResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listConsumers_returnsConsumers() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other 2 consumers to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    ListConsumersResponse expected =
        ListConsumersResponse.create(
            ConsumerDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                                + "/consumer-group-1/consumers")
                        .build())
                .setData(
                    Arrays.asList(
                        ConsumerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer1.groupMetadata().memberId())
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer1.groupMetadata().memberId())
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer1.groupMetadata().memberId())
                            .setClientId("client-1")
                            .setAssignments(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                                        + "/consumer-group-1/consumers/"
                                        + consumer1.groupMetadata().memberId()
                                        + "/assignments"))
                            .build(),
                        ConsumerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer2.groupMetadata().memberId())
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer2.groupMetadata().memberId())
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer2.groupMetadata().memberId())
                            .setClientId("client-2")
                            .setAssignments(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                                        + "/consumer-group-1/consumers/"
                                        + consumer2.groupMetadata().memberId()
                                        + "/assignments"))
                            .build(),
                        ConsumerData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1/consumers/"
                                            + consumer3.groupMetadata().memberId())
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1"
                                            + "/consumer=" + consumer3.groupMetadata().memberId())
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setConsumerId(consumer3.groupMetadata().memberId())
                            .setClientId("client-3")
                            .setAssignments(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                                        + "/consumer-group-1/consumers/"
                                        + consumer3.groupMetadata().memberId()
                                        + "/assignments"))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(ListConsumersResponse.class));
  }

  @Test
  public void listConsumers_nonExistingCluster_returnsNotFound() {
    Response response =
        request("/v3/clusters/foobar/consumer-groups/consumer-group-1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumer_returnsConsumer() {
    String baseUrl = restConnect;
    String clusterId = getClusterId();

    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other 2 consumers to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    GetConsumerResponse expected =
        GetConsumerResponse.create(
            ConsumerData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId
                                + "/consumer-groups/consumer-group-1/consumers/"
                                + consumer1.groupMetadata().memberId())
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/consumer-group=consumer-group-1"
                                + "/consumer=" + consumer1.groupMetadata().memberId())
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId("consumer-group-1")
                .setConsumerId(consumer1.groupMetadata().memberId())
                .setClientId("client-1")
                .setAssignments(
                    Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups"
                            + "/consumer-group-1/consumers/"
                            + consumer1.groupMetadata().memberId()
                            + "/assignments"))
                .build());

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1"
                + "/consumers/" + consumer1.groupMetadata().memberId())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(GetConsumerResponse.class));
  }

  @Test
  public void getConsumer_nonExistingCluster_returnsNotFound() {
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other 2 consumers to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/foobar/consumer-groups/consumer-group-1/consumers/"
                + consumer1.groupMetadata().memberId())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumer_nonExistingConsumerGroup_returnsNotFound() {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other 2 consumers to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/foobar/consumers/"
                + consumer1.groupMetadata().memberId())
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumer_nonExistingConsumer_returnsNotFound() {
    String clusterId = getClusterId();
    createTopic("topic-1", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-2", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    createTopic("topic-3", /* numPartitions= */ 3, /* replicationFactor= */ (short) 1);
    KafkaConsumer<?, ?> consumer1 = createConsumer("consumer-group-1", "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer("consumer-group-1", "client-2");
    KafkaConsumer<?, ?> consumer3 = createConsumer("consumer-group-1", "client-3");
    consumer1.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer2.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer3.subscribe(Arrays.asList("topic-1", "topic-2", "topic-3"));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other 2 consumers to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    consumer3.poll(Duration.ofSeconds(1));

    Response response =
        request(
            "/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1/consumers/foobar")
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
