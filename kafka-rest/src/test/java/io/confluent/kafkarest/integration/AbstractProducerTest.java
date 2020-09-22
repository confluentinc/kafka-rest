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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.serialization.Deserializer;

public class AbstractProducerTest extends ClusterTestHarness {



  protected <K, V> void testProduceToTopic(
      String topicName,
      ProduceRequest request,
      Function<ProduceRecord, V> valueSerializer,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      List<PartitionOffset> offsetResponses,
      boolean matchPartitions,
      List<ProduceRecord> expected
  ) {
    testProduceToTopic(topicName,
        request,
        valueSerializer,
        keyDeserializer,
        valueDeserializer,
        offsetResponses,
        matchPartitions,
        Collections.emptyMap(),
        expected);
  }

  protected <K, V> void testProduceToTopic(
      String topicName,
      ProduceRequest request,
      Function<ProduceRecord, V> valueSerializer,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      List<PartitionOffset> offsetResponses,
      boolean matchPartitions,
      Map<String, String> queryParams,
      List<ProduceRecord> expected
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
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        null,
        expected,
        valueSerializer,
        keyDeserializer,
        valueDeserializer,
        true);
  }

  protected <K, V> void testProduceToPartition(
      String topicName,
      int partition,
      ProduceRequest request,
      Function<ProduceRecord, V> valueSerializer,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      List<PartitionOffset> offsetResponse,
      List<ProduceRequest.ProduceRecord> expected
  ) {
    testProduceToPartition(
        topicName,
        partition,
        request,
        valueSerializer,
        keyDeserializer,
        valueDeserializer,
        offsetResponse,
        Collections.emptyMap(),
        expected);
  }

  protected <K, V> void testProduceToPartition(
      String topicName,
      int partition,
      ProduceRequest request,
      Function<ProduceRecord, V> valueSerializer,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      List<PartitionOffset> offsetResponse,
      Map<String, String> queryParams,
      List<ProduceRecord> expected
  ) {
    Response response = request("/topics/" + topicName + "/partitions/0", queryParams)
        .post(Entity.entity(request, getEmbeddedContentType()));
    assertOKResponse(response, Versions.KAFKA_V2_JSON);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        partition,
        expected,
        valueSerializer,
        keyDeserializer,
        valueDeserializer, true);
  }

  protected void testProduceToTopicFails(String topicName, ProduceRequest request) {
    testProduceToTopicFails(topicName, request, Collections.emptyMap());
  }

  protected void testProduceToTopicFails(
      String topicName,
      ProduceRequest request,
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

  protected void testProduceToAuthorizationError(String topicName, ProduceRequest request) {
    testProduceToAuthorizationError(topicName, request, Collections.emptyMap());
  }

  protected void testProduceToAuthorizationError(
      String topicName,
      ProduceRequest request,
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
