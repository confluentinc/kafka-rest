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

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class AbstractProducerTest<TopicRequestT, PartitionRequestT> extends ClusterTestHarness {

  protected <K, V> void testProduceToTopic(
      String topicName,
      TopicRequestT request,
      String keyDeserializerClassName,
      String valueDeserializerClassName,
      List<PartitionOffset> offsetResponses,
      boolean matchPartitions,
      List<ProduceRecord<K, V>> expected
  ) {
    testProduceToTopic(topicName,
        request,
        keyDeserializerClassName,
        valueDeserializerClassName,
        offsetResponses,
        matchPartitions,
        Collections.emptyMap(),
        expected);
  }

  protected <K, V> void testProduceToTopic(
      String topicName,
      TopicRequestT request,
      String keyDeserializerClassName,
      String valueDeserializerClassName,
      List<PartitionOffset> offsetResponses,
      boolean matchPartitions,
      Map<String, String> queryParams,
      List<ProduceRecord<K, V>> expected
  ) {
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(request, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse produceResponse =
        TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    if (matchPartitions) {
      TestUtils.assertPartitionsEqual(offsetResponses, produceResponse.getOffsets());
    }
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(plaintextBrokerList, topicName,
        expected, null,
        keyDeserializerClassName, valueDeserializerClassName, true);
  }

  protected <K, V> void testProduceToPartition(
      String topicName,
      int partition,
      PartitionRequestT request,
      String keySerializerClassName,
      String valueSerializerClassName,
      List<PartitionOffset> offsetResponse,
      List<ProduceRecord<K, V>> expected
  ) {
    testProduceToPartition(topicName, partition, request, keySerializerClassName,
        valueSerializerClassName, offsetResponse, Collections.emptyMap(), expected);
  }

  protected <K, V> void testProduceToPartition(
      String topicName,
      int partition,
      PartitionRequestT request,
      String keySerializerClassName,
      String valueSerializerClassName,
      List<PartitionOffset> offsetResponse,
      Map<String, String> queryParams,
      List<ProduceRecord<K, V>> expected
  ) {
    Response response = request("/topics/" + topicName + "/partitions/0", queryParams)
        .post(Entity.entity(request, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(plaintextBrokerList, topicName,
        expected, partition,
        keySerializerClassName, valueSerializerClassName, true);
  }

  protected void testProduceToTopicFails(String topicName, TopicRequestT request) {
    testProduceToTopicFails(topicName, request, Collections.emptyMap());
  }

  protected void testProduceToTopicFails(
      String topicName,
      TopicRequestT request,
      Map<String, String> queryParams
  ) {
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse produceResponse =
        TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertNotNull(pOffset.getError());
    }
  }

  protected void testProduceToAuthorizationError(String topicName, TopicRequestT request) {
    testProduceToAuthorizationError(topicName, request, Collections.emptyMap());
  }

  protected void testProduceToAuthorizationError(
      String topicName,
      TopicRequestT request,
      Map<String, String> queryParams
  ) {
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    final ProduceResponse produceResponse =
        TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    for (PartitionOffset pOffset : produceResponse.getOffsets()) {
      assertEquals(Errors.KAFKA_AUTHORIZATION_ERROR_CODE, (int) pOffset.getErrorCode());
    }
  }

  protected String getEmbeddedContentType() {
    return Versions.KAFKA_V2_JSON_BINARY;
  }
}
