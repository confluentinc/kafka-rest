/*
 * Copyright 2018 Confluent Inc.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractProducerTest extends ClusterTestHarness {

  protected <K, V> void testProduceToTopic(String topicName,
                                           List<? extends TopicProduceRecord> records,
                                           String keyDeserializerClassName,
                                           String valueDeserializerClassName,
                                           List<PartitionOffset> offsetResponses,
                                           boolean matchPartitions) {
    testProduceToTopic(topicName,
        records,
        keyDeserializerClassName,
        valueDeserializerClassName,
        offsetResponses,
        matchPartitions,
        Collections.emptyMap());
  }

  protected <K, V> void testProduceToTopic(String topicName,
                                           List<? extends TopicProduceRecord> records,
                                           String keyDeserializerClassName,
                                           String valueDeserializerClassName,
                                           List<PartitionOffset> offsetResponses,
                                           boolean matchPartitions,
                                           Map<String, String> queryParams) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(payload, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    if (matchPartitions) {
      TestUtils.assertPartitionsEqual(offsetResponses, produceResponse.getOffsets());
    }
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(plaintextBrokerList, topicName,
        payload.getRecords(), null,
        keyDeserializerClassName, valueDeserializerClassName, true);
  }

  protected <K, V> void testProduceToPartition(String topicName,
                                               int partition,
                                               List<? extends ProduceRecord<K, V>> records,
                                               String keySerializerClassName,
                                               String valueSerializerClassName,
                                               List<PartitionOffset> offsetResponse) {
    testProduceToPartition(topicName, partition, records, keySerializerClassName,
        valueSerializerClassName, offsetResponse, Collections.emptyMap());
  }

  protected <K, V> void testProduceToPartition(String topicName,
                                               int partition,
                                               List<? extends ProduceRecord<K, V>> records,
                                               String keySerializerClassName,
                                               String valueSerializerClassName,
                                               List<PartitionOffset> offsetResponse,
                                               Map<String, String> queryParams) {
    PartitionProduceRequest payload = new PartitionProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName + "/partitions/0", queryParams)
        .post(Entity.entity(payload, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(plaintextBrokerList, topicName,
        payload.getRecords(), partition,
        keySerializerClassName, valueSerializerClassName, true);
  }

  protected void testProduceToTopicFails(String topicName,
                                         List<? extends TopicProduceRecord> records) {
    testProduceToTopicFails(topicName, records, Collections.emptyMap());
  }

  protected void testProduceToTopicFails(String topicName,
                                         List<? extends TopicProduceRecord> records,
                                         Map<String, String> queryParams) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertNotNull(pOffset.getError());
    }
  }

  protected void testProduceToAuthorizationError(String topicName,
                                                 List<? extends TopicProduceRecord> records) {
    testProduceToAuthorizationError(topicName, records, Collections.emptyMap());
  }

  protected void testProduceToAuthorizationError(String topicName,
                                         List<? extends TopicProduceRecord> records,
                                         Map<String, String> queryParams) {
    TopicProduceRequest payload = new TopicProduceRequest();
    payload.setRecords(records);
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_MOST_SPECIFIC_DEFAULT));

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    final ProduceResponse produceResponse = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertEquals(Errors.KAFKA_AUTHORIZATION_ERROR_CODE, (int) pOffset.getErrorCode());
    }
  }

  protected String getEmbeddedContentType() {
    return Versions.KAFKA_MOST_SPECIFIC_DEFAULT;
  }
}
