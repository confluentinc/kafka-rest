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

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagSummaryData;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagSummaryResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsumerGroupLagSummariesResourceIntegrationTest extends ClusterTestHarness {

  private static final String topic1 = "topic-1";
  private static final String topic2 = "topic-2";
  private static final String group1 = "consumer-group-1";
  private String baseUrl;
  private String clusterId;

  private final List<BinaryPartitionProduceRecord> partitionRecordsWithoutKeys =
      Arrays.asList(
          new BinaryPartitionProduceRecord(null, "value"),
          new BinaryPartitionProduceRecord(null, "value2"),
          new BinaryPartitionProduceRecord(null, "value3"));

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    baseUrl = restConnect;
    clusterId = getClusterId();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    createTopic(topic1, numPartitions, (short) replicationFactor);
    createTopic(topic2, numPartitions, (short) replicationFactor);
  }

  @Test
  public void getConsumerGroupLagSummary_returnsConsumerGroupLagSummary() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer(group1, "client-2");
    consumer1.subscribe(Collections.singletonList(topic1));
    consumer2.subscribe(Collections.singletonList(topic2));
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other consumer to join the group.
    consumer1.poll(Duration.ofSeconds(1));
    consumer2.poll(Duration.ofSeconds(1));

    // produce to topic1 partition0, topic2 partition1
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    produce(topic2, 1, request);

    // consume from subscribed topics
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));

    // group lag request returns maxLag=0, totalLag=0
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag-summary")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ConsumerGroupLagSummaryData consumerGroupLagSummaryData =
        response.readEntity(GetConsumerGroupLagSummaryResponse.class).getValue();
    assertEquals(0, (long) consumerGroupLagSummaryData.getMaxLag());
    assertEquals(0, (long) consumerGroupLagSummaryData.getTotalLag());

    // produce again to topic1 partition0, twice again to topic2 partition1
    produce(topic1, 0, request);
    produce(topic2, 1, request);
    produce(topic2, 1, request);

    GetConsumerGroupLagSummaryResponse expectedResponse =
        GetConsumerGroupLagSummaryResponse.create(
            ConsumerGroupLagSummaryData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/consumer-groups/"
                                + group1
                                + "/lag-summary")
                        .setResourceName(
                            "crn:///kafka="
                                + clusterId
                                + "/consumer-group="
                                + group1
                                + "/lag-summary")
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId(group1)
                .setMaxLagConsumerId(consumer2.groupMetadata().memberId())
                .setMaxLagClientId("client-2")
                .setMaxLagTopicName(topic2)
                .setMaxLagPartitionId(1)
                .setMaxLag(6L)
                .setTotalLag(9L)
                .setMaxLagConsumer(
                    Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/consumer-groups/"
                            + group1
                            + "/consumers/"
                            + consumer2.groupMetadata().memberId()))
                .setMaxLagPartition(
                    Relationship.create(
                        baseUrl
                            + "/v3/clusters/"
                            + clusterId
                            + "/topics/"
                            + topic2
                            + "/partitions/"
                            + 1))
                .build());

    // group lag request returns maxLag=6, totalLag=9, maxTopicName=topic2, maxPartitionId=1
    testWithRetry(
        () -> {
          Response response2 =
              request(
                      "/v3/clusters/"
                          + getClusterId()
                          + "/consumer-groups/"
                          + group1
                          + "/lag-summary")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();
          assertEquals(Status.OK.getStatusCode(), response2.getStatus());
          assertEquals(
              expectedResponse, response2.readEntity(GetConsumerGroupLagSummaryResponse.class));
        });
  }

  @Test
  public void getConsumerGroupLagSummary_nonExistingOffsets_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Arrays.asList(topic1, topic2));

    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    produce(topic2, 1, request);

    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag-summary")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroupLagSummary_nonExistingConsumerGroup_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Arrays.asList(topic1));
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/foo/lag-summary")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroupLagSummary_nonExistingCluster_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Arrays.asList(topic1));
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    consumer.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/foo/consumer-groups/" + group1 + "/lag-summary")
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

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }
}
