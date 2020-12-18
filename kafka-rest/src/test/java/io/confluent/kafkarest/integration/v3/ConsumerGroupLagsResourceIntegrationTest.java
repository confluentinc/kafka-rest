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
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.ConsumerGroupLagData;
import io.confluent.kafkarest.entities.v3.GetConsumerGroupLagResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.Resource.Relationship;
import io.confluent.kafkarest.integration.AbstractConsumerTest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsumerGroupLagsResourceIntegrationTest extends AbstractConsumerTest {

  private static final String topic1 = "topic-1";
  private static final String topic2 = "topic-2";
  private static final String group1 = "consumer-group-1";
  private String baseUrl;
  private String clusterId;
  private final List<ProducerRecord<byte[], byte[]>> recordsOnlyValues = Arrays.asList(
      new ProducerRecord<>(topic1, "value".getBytes()),
      new ProducerRecord<>(topic1, "value2".getBytes()),
      new ProducerRecord<>(topic1, "value3".getBytes())
  );
  private final List<BinaryPartitionProduceRecord> partitionRecordsWithoutKeys = Arrays.asList(
      new BinaryPartitionProduceRecord(null, "value"),
      new BinaryPartitionProduceRecord(null, "value2"),
      new BinaryPartitionProduceRecord(null, "value3")
  );

//  public ConsumerGroupLagsResourceIntegrationTest() {
//    super(/* numBrokers= */1, /*withSchemaRegistry= */ false);
//  }

  private static final GenericType<List<BinaryConsumerRecord>> binaryConsumerRecordType
      = new GenericType<List<BinaryConsumerRecord>>() {
  };

  @Before
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
  public void getConsumerGroupLag_returnsMaxLagPartition() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    consumer1.subscribe(Arrays.asList(topic1, topic2));
    // String instanceUri = startConsumeMessages(group1, Arrays.asList(topic1, topic2), null, Versions.KAFKA_V2_JSON_BINARY);

    // produce to topic1 partition0, topic2 partition1
    BinaryPartitionProduceRequest request = BinaryPartitionProduceRequest.create(partitionRecordsWithoutKeys);
    produce(topic1, 0, request);
    produce(topic2, 1, request);

    // consume
    consumer1.poll(Duration.ofSeconds(10));
    // request(instanceUri + "/records").accept(Versions.KAFKA_V2_JSON_BINARY).get();
    // commitOffsets(instanceUri);

    // group lag request returns maxLag=0, totalLag=0
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ConsumerGroupLagData consumerGroupLagData =
        response.readEntity(GetConsumerGroupLagResponse.class).getValue();
    assertEquals(0, (long) consumerGroupLagData.getMaxLag());
    assertEquals(0, (long) consumerGroupLagData.getTotalLag());

    // produce again to topic1 partition0, twice again to topic2 partition1
    produce(topic1, 0, request);
    produce(topic2, 1, request);
    produce(topic2, 1, request);

    // group lag request returns maxLag=6, totalLag=9, maxTopicName=topic2, maxPartitionId=1
    Response response2 =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response2.getStatus());

    GetConsumerGroupLagResponse expectedGroupLagResponse =
        GetConsumerGroupLagResponse.create(
            ConsumerGroupLagData.builder()
                .setMetadata(
                    Resource.Metadata.builder()
                        .setSelf(
                            baseUrl + "/v3/clusters/" + clusterId
                                + "/consumer-groups/" + group1 + "/lag")
                        .setResourceName(
                            "crn:///kafka=" + clusterId + "/consumer-group=" + group1 + "/lag")
                        .build())
                .setClusterId(clusterId)
                .setConsumerGroupId(group1)
                .setMaxLag(6L)
                .setTotalLag(9L)
                .setMaxLagConsumerId(consumer1.groupMetadata().memberId())
                .setMaxLagClientId("client-1")
                .setMaxLagTopicName(topic2)
                .setMaxLagPartitionId(1)
                .setMaxLagPartition(
                    Relationship
                        .create(baseUrl + "/v3/clusters/" + clusterId
                            + "/topics/" + topic2 + "/partitions/" + 1))
                .build());

    assertEquals(expectedGroupLagResponse, response2.readEntity(GetConsumerGroupLagResponse.class));
  }

  @Test
  public void getConsumerGroupLag_returnsConsumerGroupLag() {
    String instanceUri = startConsumeMessages(group1, topic1, null, Versions.KAFKA_V2_JSON_BINARY);
    produceBinaryMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroupLag_nonExistingOffsets_returnsNotFound() {
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/" + group1 + "/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroupLag_nonExistingConsumerGroup_returnsNotFound() {
    String instanceUri = startConsumeMessages(group1, topic1, null, Versions.KAFKA_V2_JSON_BINARY);
    produceBinaryMessages(recordsOnlyValues);
    consumeMessages(
        instanceUri,
        recordsOnlyValues,
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_BINARY,
        binaryConsumerRecordType,
        null,
        BinaryConsumerRecord::toConsumerRecord);
    commitOffsets(instanceUri);
    Response response =
        request("/v3/clusters/" + getClusterId() + "/consumer-groups/foo/lag")
            .accept(MediaType.APPLICATION_JSON)
            .get();
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void getConsumerGroupLag_nonExistingCluster_returnsNotFound() {
    KafkaConsumer<?, ?> consumer1 = createConsumer(group1, "client-1");
    consumer1.subscribe(Arrays.asList(topic1));
    consumer1.poll(Duration.ofSeconds(1));

    Response response =
        request("/v3/clusters/foo/consumer-groups/" + group1 + "/lag")
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
