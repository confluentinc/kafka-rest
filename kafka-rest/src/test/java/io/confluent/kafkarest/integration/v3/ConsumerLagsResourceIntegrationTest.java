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
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.ConsumerLagDataList;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.ListConsumerLagsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
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

public class ConsumerLagsResourceIntegrationTest extends ClusterTestHarness {

  private static final String topic1 = "topic-1";
  private static final String topic2 = "topic-2";
  private static final String[] topics = new String[] {topic1, topic2};
  private static final String group1 = "consumer-group-1";
  private static final int numTopics = 2;
  private static final int numPartitions = 2;
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
    final int replicationFactor = 1;
    createTopic(topic1, numPartitions, (short) replicationFactor);
    createTopic(topic2, numPartitions, (short) replicationFactor);
  }

  @Test
  public void listConsumerLags_returnsConsumerLags() {
    // produce to topic1 partition0 and topic2 partition1
    BinaryPartitionProduceRequest request1 =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request1);
    produce(topic2, 1, request1);

    // stores expected currentOffsets and logEndOffsets for each topic partition after sending
    // 3 records to topic1 partition0 and topic2 partition1
    long[][] expectedOffsets = new long[numTopics][numPartitions];
    expectedOffsets[0][0] = 3;
    expectedOffsets[1][1] = 3;
    // all other values default to 0L

    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    KafkaConsumer<?, ?> consumer2 = createConsumer(group1, "client-2");

    consumer1.subscribe(Collections.singletonList(topic1));
    consumer2.subscribe(Collections.singletonList(topic2));
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));
    // After polling once, only one of the consumers will be member of the group, so we poll again
    // to force the other consumer to join the group.
    consumer1.poll(Duration.ofSeconds(5));
    consumer2.poll(Duration.ofSeconds(5));
    // commit offsets from consuming from subscribed topics
    consumer1.commitSync();
    consumer2.commitSync();

    testWithRetry(
        () -> {
          Response response =
              request("/v3/clusters/" + clusterId + "/consumer-groups/" + group1 + "/lags")
                  .accept(MediaType.APPLICATION_JSON)
                  .get();

          assertEquals(Status.OK.getStatusCode(), response.getStatus());
          ConsumerLagDataList consumerLagDataList =
              response.readEntity(ListConsumerLagsResponse.class).getValue();

          // checks offsets and lag match what is expected for each topic partition
          for (int t = 0; t < numTopics; t++) {
            for (int p = 0; p < numPartitions; p++) {
              final int finalT = t;
              final int finalP = p;
              ConsumerLagData consumerLagData =
                  consumerLagDataList.getData().stream()
                      .filter(lagData -> lagData.getTopicName().equals(topics[finalT]))
                      .filter(lagData -> lagData.getPartitionId() == finalP)
                      .findAny()
                      .get();

              assertEquals(expectedOffsets[t][p], (long) consumerLagData.getCurrentOffset());
              assertEquals(expectedOffsets[t][p], (long) consumerLagData.getLogEndOffset());
              assertEquals(0, (long) consumerLagData.getLag());
            }
          }
        });

    // produce again to topic2 partition1
    BinaryPartitionProduceRequest request2 =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic2, 1, request2);

    Response response2 =
        request("/v3/clusters/" + clusterId + "/consumer-groups/" + group1 + "/lags")
            .accept(MediaType.APPLICATION_JSON)
            .get();

    ListConsumerLagsResponse expected =
        ListConsumerLagsResponse.create(
            ConsumerLagDataList.builder()
                .setMetadata(
                    ResourceCollection.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/consumer-groups/"
                                + group1
                                + "/lags")
                        .build())
                .setData(
                    Arrays.asList(
                        expectedConsumerLagData(
                            /* topicName= */ topic2,
                            /* partitionId=*/ 1,
                            /* consumerId= */ consumer2.groupMetadata().memberId(),
                            /* clientId= */ "client-2",
                            /* currentOffset= */ 3,
                            /* logEndOffset= */ 6),
                        expectedConsumerLagData(
                            /* topicName= */ topic1,
                            /* partitionId=*/ 0,
                            /* consumerId= */ consumer1.groupMetadata().memberId(),
                            /* clientId= */ "client-1",
                            /* currentOffset= */ 3,
                            /* logEndOffset= */ 3),
                        expectedConsumerLagData(
                            /* topicName= */ topic1,
                            /* partitionId=*/ 1,
                            /* consumerId= */ consumer1.groupMetadata().memberId(),
                            /* clientId= */ "client-1",
                            /* currentOffset= */ 0,
                            /* logEndOffset= */ 0),
                        expectedConsumerLagData(
                            /* topicName= */ topic2,
                            /* partitionId=*/ 0,
                            /* consumerId= */ consumer2.groupMetadata().memberId(),
                            /* clientId= */ "client-2",
                            /* currentOffset= */ 0,
                            /* logEndOffset= */ 0)))
                .build());

    assertEquals(expected, response2.readEntity(ListConsumerLagsResponse.class));
  }

  @Test
  public void listConsumerLags_nonExistingConsumerGroup_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Collections.singletonList(topic1));
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    consumer.poll(Duration.ofSeconds(1));
    consumer.commitSync();

    Response response =
        request("/v3/clusters/" + clusterId + "/consumer-groups/" + "foo" + "/lags")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_returnsConsumerLag() {
    // produce to topic1 partition0 and topic2 partition1
    BinaryPartitionProduceRequest request1 =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request1);
    produce(topic2, 1, request1);

    // stores expected currentOffsets and logEndOffsets for each topic partition after sending
    // 3 records to topic1 partition0 and topic2 partition1
    long[][] expectedOffsets = new long[numTopics][numPartitions];
    expectedOffsets[0][0] = 3;
    expectedOffsets[1][1] = 3;
    // all other values default to 0L

    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Arrays.asList(topic1, topic2));
    // consume from subscribed topics (zero lag)
    consumer.poll(Duration.ofSeconds(5));
    consumer.poll(Duration.ofSeconds(5));
    consumer.commitSync();

    for (int t = 0; t < numTopics; t++) {
      for (int p = 0; p < numPartitions; p++) {
        final int finalP = p;
        final int finalT = t;
        testWithRetry(
            () -> {
              Response response =
                  request(
                          "/v3/clusters/"
                              + clusterId
                              + "/consumer-groups/"
                              + group1
                              + "/lags/"
                              + topics[finalT]
                              + "/partitions/"
                              + finalP)
                      .accept(MediaType.APPLICATION_JSON)
                      .get();

              assertEquals(Status.OK.getStatusCode(), response.getStatus());
              ConsumerLagData consumerLagData =
                  response.readEntity(GetConsumerLagResponse.class).getValue();
              assertEquals(
                  expectedOffsets[finalT][finalP], (long) consumerLagData.getCurrentOffset());
              assertEquals(
                  expectedOffsets[finalT][finalP], (long) consumerLagData.getLogEndOffset());
              assertEquals(0, (long) consumerLagData.getLag());
            });
      }
    }

    // produce again to topic2 partition1
    BinaryPartitionProduceRequest request2 =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic2, 1, request2);

    Response response2 =
        request(
                "/v3/clusters/"
                    + clusterId
                    + "/consumer-groups/"
                    + group1
                    + "/lags/"
                    + topic2
                    + "/partitions/"
                    + 1)
            .accept(MediaType.APPLICATION_JSON)
            .get();

    GetConsumerLagResponse expectedConsumerLagResponse =
        GetConsumerLagResponse.create(
            ConsumerLagData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl
                                + "/v3/clusters/"
                                + clusterId
                                + "/consumer-groups/"
                                + group1
                                + "/lags/"
                                + topic2
                                + "/partitions/"
                                + 1)
                        .setResourceName(
                            "crn:///kafka="
                                + clusterId
                                + "/consumer-group="
                                + group1
                                + "/lag="
                                + topic2
                                + "/partition="
                                + 1)
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId(group1)
                .setTopicName(topic2)
                .setPartitionId(1)
                .setConsumerId(consumer.groupMetadata().memberId())
                .setClientId("client-1")
                .setCurrentOffset(3L)
                .setLogEndOffset(6L)
                .setLag(3L)
                .build());

    assertEquals(expectedConsumerLagResponse, response2.readEntity(GetConsumerLagResponse.class));
  }

  @Test
  public void getConsumerLag_nonExistingOffsets_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Collections.singletonList(topic1));

    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    produce(topic2, 1, request);

    Response response =
        request(
                "/v3/clusters/"
                    + clusterId
                    + "/consumer-groups/"
                    + group1
                    + "/lags/"
                    + topic1
                    + "/partitions/"
                    + 0)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_nonExistingConsumerGroup_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Collections.singletonList(topic1));
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    consumer.poll(Duration.ofSeconds(1));
    consumer.commitSync();

    Response response =
        request(
                "/v3/clusters/"
                    + clusterId
                    + "/consumer-groups/"
                    + "foo"
                    + "/lags/"
                    + topic1
                    + "/partitions/"
                    + 0)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_nonExistingCluster_returnsNotFound() {
    KafkaConsumer<?, ?> consumer = createConsumer(group1, "client-1");
    consumer.subscribe(Collections.singletonList(topic1));
    BinaryPartitionProduceRequest request =
        BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    consumer.poll(Duration.ofSeconds(1));
    consumer.commitSync();

    Response response =
        request(
                "/v3/clusters/"
                    + "foo"
                    + "/consumer-groups/"
                    + group1
                    + "/lags/"
                    + topic1
                    + "/partitions/"
                    + 0)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  private KafkaConsumer<?, ?> createConsumer(String consumerGroup, String clientId) {
    Properties properties = restConfig.getConsumerProperties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return new KafkaConsumer<>(properties, new BytesDeserializer(), new BytesDeserializer());
  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }

  private ConsumerLagData expectedConsumerLagData(
      String topicName,
      int partitionId,
      String consumerId,
      String clientId,
      long currentOffset,
      long logEndOffset) {
    return ConsumerLagData.builder()
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    baseUrl
                        + "/v3/clusters/"
                        + clusterId
                        + "/consumer-groups/"
                        + group1
                        + "/lags/"
                        + topicName
                        + "/partitions/"
                        + partitionId)
                .setResourceName(
                    "crn:///kafka="
                        + clusterId
                        + "/consumer-group="
                        + group1
                        + "/lag="
                        + topicName
                        + "/partition="
                        + partitionId)
                .build())
        .setClusterId(clusterId)
        .setConsumerGroupId(group1)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setConsumerId(consumerId)
        .setClientId(clientId)
        .setCurrentOffset(currentOffset)
        .setLogEndOffset(logEndOffset)
        .setLag(logEndOffset - currentOffset)
        .build();
  }
}
