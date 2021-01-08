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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.ConsumerLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerLagResponse;
import io.confluent.kafkarest.entities.v3.Resource;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetConsumerLagResourceIntegrationTest extends ClusterTestHarness {

  private static final String topic1 = "topic-1";
  private static final String topic2 = "topic-2";
  private static final String[] topics = new String[]{topic1, topic2};
  private static final String group1 = "consumer-group-1";
  private static final int numTopics = 2;
  private static final int numPartitions = 3;
  private String baseUrl;
  private String clusterId;

  private final List<BinaryPartitionProduceRecord> partitionRecordsWithoutKeys = Arrays.asList(
      new BinaryPartitionProduceRecord(null, "value"),
      new BinaryPartitionProduceRecord(null, "value2"),
      new BinaryPartitionProduceRecord(null, "value3")
  );

  @Before
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
  public void getConsumerLag_returnsConsumerLag() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    consumer1.subscribe(Arrays.asList(topic1, topic2));

    // produce to topic1 partition0 and topic2 partition2
    BinaryPartitionProduceRequest request1 = BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request1);
    produce(topic2, 2, request1);

    // consume from subscribed topics (zero lag)
    consumer1.poll(Duration.ofSeconds(10));

    // stores expected currentOffsets and logEndOffsets for each topic partition after sending
    // 3 records to topic1 partition0 and topic2 partition2
    long[][] expectedOffsets = new long[2][3];
    expectedOffsets[0][0] = 3;                         // topic1 partition0
    expectedOffsets[1][2] = 3;                         // topic2 partition2
    // all other values default to 0L

    for (int t = 0; t < numTopics; t++) {
      for (int p = 0; p < numPartitions; p++) {
        Response response =
            request("/v3/clusters/" + clusterId + "/topics/" + topics[t] +
                "/partitions/" + p + "/lags/" + group1)
                .accept(MediaType.APPLICATION_JSON)
                .get();

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        ConsumerLagData consumerLagData =
            response.readEntity(GetConsumerLagResponse.class).getValue();
        assertEquals(expectedOffsets[t][p], (long) consumerLagData.getCurrentOffset());
        assertEquals(expectedOffsets[t][p], (long) consumerLagData.getLogEndOffset());
        assertEquals(0, (long) consumerLagData.getLag());
      }
    }

    // produce again to topic2 partition1
    BinaryPartitionProduceRequest request2 = BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic2, 2, request2);

    Response response2 =
        request("/v3/clusters/" + clusterId + "/topics/" + topic2 +
            "/partitions/" + 2 + "/lags/" + group1)
            .accept(MediaType.APPLICATION_JSON)
            .get();

    GetConsumerLagResponse expectedConsumerLagResponse =
        GetConsumerLagResponse.create(
            ConsumerLagData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId + "/topics/" + topic2 +
                                "/partitions/" + 2 + "/lags/" + group1)
                        .setResourceName(
                            "crn:///kafka=" + clusterId + "/topic=" + topic2 +
                                "/partition=" + 2 + "/lag=" + group1)
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId(group1)
                .setTopicName(topic2)
                .setPartitionId(2)
                .setConsumerId(consumer1.groupMetadata().memberId())
                .setClientId("client-1")
                .setCurrentOffset(3L)
                .setLogEndOffset(6L)
                .setLag(3L)
                .build());

    assertEquals(expectedConsumerLagResponse, response2.readEntity(GetConsumerLagResponse.class));
  }

  @Test
  public void getConsumerLag_nonExistingOffsets_returnsNotFound() {
    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 +
            "/partitions/" + 0 + "/lags/" + group1)
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_nonExistingConsumerGroup_returnsNotFound() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    consumer1.subscribe(Collections.singletonList(topic1));
    BinaryPartitionProduceRequest request1 = BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request1);
    consumer1.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 +
            "/partitions/" + 0 + "/lags/" + "foo")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerLag_nonExistingCluster_returnsNotFound() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    consumer1.subscribe(Collections.singletonList(topic1));
    BinaryPartitionProduceRequest request1 = BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request1);
    consumer1.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/" + "foo" + "/topics/" + topic1 +
            "/partitions/" + 0 + "/lags/" + group1)
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

  // produces request to topicName and partitionId
  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }
}
