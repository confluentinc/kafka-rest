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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.entities.ConsumerGroup.State;
import io.confluent.kafkarest.entities.v3.ConsumerGroupData;
import io.confluent.kafkarest.entities.v3.ConsumerGroupDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerGroupsResponse;
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
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupsResourceIntegrationTest extends ClusterTestHarness {

  public ConsumerGroupsResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Test
  public void listConsumerGroups_returnsConsumerGroups() {
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

    ListConsumerGroupsResponse expected =
        ListConsumerGroupsResponse.create(
            ConsumerGroupDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(baseUrl + "/v3/clusters/" + clusterId + "/consumer-groups")
                        .build())
                .setData(
                    singletonList(
                        ConsumerGroupData.builder()
                            .setMetadata(
                                Resource.Metadata.builder()
                                    .setSelf(
                                        baseUrl + "/v3/clusters/" + clusterId
                                            + "/consumer-groups/consumer-group-1")
                                    .setResourceName(
                                        "crn:///kafka=" + clusterId
                                            + "/consumer-group=consumer-group-1")
                                    .build())
                            .setClusterId(clusterId)
                            .setConsumerGroupId("consumer-group-1")
                            .setSimple(false)
                            .setPartitionAssignor("")
                            .setState(State.PREPARING_REBALANCE)
                            .setCoordinator(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
                            .setConsumers(
                                Relationship.create(
                                    baseUrl + "/v3/clusters/" + clusterId
                                        + "/consumer-groups/consumer-group-1/consumers"))
                            .build()))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(ListConsumerGroupsResponse.class));
  }

  @Test
  public void listConsumerGroups_nonExistingCluster_returnsNotFound() {
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

    Response response =
        request("/v3/clusters/foobar/consumer-groups")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroup_returnsConsumerGroup() {
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

    GetConsumerGroupResponse expected =
        GetConsumerGroupResponse.create(
            ConsumerGroupData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId
                                + "/consumer-groups/consumer-group-1")
                        .setResourceName(
                            "crn:///kafka=" + clusterId
                                + "/consumer-group=consumer-group-1")
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId("consumer-group-1")
                .setSimple(false)
                .setPartitionAssignor("")
                .setState(State.PREPARING_REBALANCE)
                .setCoordinator(
                    Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId + "/brokers/0"))
                .setConsumers(
                    Relationship.create(
                        baseUrl + "/v3/clusters/" + clusterId
                            + "/consumer-groups/consumer-group-1/consumers"))
                .build());

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/consumer-group-1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(expected, response.readEntity(GetConsumerGroupResponse.class));
  }

  @Test
  public void getConsumerGroup_nonExistingCluster_returnsNotFound() {
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

    Response response =
        request("/v3/clusters/foobar/consumer-groups/consumer-group-1")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroup_nonExistingConsumerGroup_returnsNotFound() {
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

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/foobar")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }
}
