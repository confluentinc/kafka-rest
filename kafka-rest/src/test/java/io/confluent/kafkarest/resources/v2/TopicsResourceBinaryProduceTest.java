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

package io.confluent.kafkarest.resources.v2;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.ByteString;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.controllers.ProduceController;
import io.confluent.kafkarest.controllers.RecordSerializer;
import io.confluent.kafkarest.controllers.SchemaManager;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceResult;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceRequest;
import io.confluent.kafkarest.entities.v2.ProduceRequest.ProduceRecord;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.RestServerErrorException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class TopicsResourceBinaryProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private static final String TOPIC_NAME = "topic1";
  private static final List<ProduceRecord> PRODUCE_RECORDS_ONLY_VALUES =
      Arrays.asList(
          ProduceRecord.create(/* key= */ null, TextNode.valueOf("value")),
          ProduceRecord.create(/* key= */ null, TextNode.valueOf("value2")));
  private static final List<ProduceRecord> PRODUCE_RECORDS_WITH_KEYS =
      Arrays.asList(
          ProduceRecord.create(TextNode.valueOf("key"), TextNode.valueOf("value")),
          ProduceRecord.create(TextNode.valueOf("key2"), TextNode.valueOf("value2")));
  private static final List<ProduceRecord> PRODUCE_RECORDS_WITH_PARTITIONS =
      Arrays.asList(
          ProduceRecord.create(/* partition= */ 0, /* key= */ null, TextNode.valueOf("value")),
          ProduceRecord.create(/* partition= */ 0, /* key= */ null, TextNode.valueOf("value2")));
  private static final List<ProduceRecord> PRODUCE_RECORDS_WITH_PARTITIONS_AND_KEYS =
      Arrays.asList(
          ProduceRecord.create(
              /* partition= */ 0, TextNode.valueOf("key"), TextNode.valueOf("value")),
          ProduceRecord.create(
              /* partition= */ 0, TextNode.valueOf("key2"), TextNode.valueOf("value2")));
  private static final List<ProduceRecord> PRODUCE_RECORDS_WITH_NULL_VALUES =
      Arrays.asList(
          ProduceRecord.create(TextNode.valueOf("key"), /* value= */ null),
          ProduceRecord.create(TextNode.valueOf("key2"), /* value= */ null));
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC_NAME, 0);
  private static final List<CompletableFuture<RecordMetadata>> PRODUCE_RESULTS =
      Arrays.asList(
          CompletableFuture.completedFuture(new RecordMetadata(PARTITION, 0L, 0L, 0L, 0L, 1, 1)),
          CompletableFuture.completedFuture(new RecordMetadata(PARTITION, 0L, 1L, 0L, 0L, 1, 1)));
  private static final List<PartitionOffset> OFFSETS =
      Arrays.asList(new PartitionOffset(0, 0L, null, null), new PartitionOffset(0, 1L, null, null));
  private static final List<ProduceRecord> PRODUCE_EXCEPTION_DATA =
      Collections.singletonList(ProduceRecord.create(/* key= */ null, /* value= */ null));
  private static final String EXCEPTION_MESSAGE = "Error message";
  private static final List<CompletableFuture<RecordMetadata>> PRODUCE_GENERIC_EXCEPTION_RESULTS =
      Collections.singletonList(CompletableFutures.failedFuture(new Exception(EXCEPTION_MESSAGE)));
  private static final List<CompletableFuture<RecordMetadata>> PRODUCE_KAFKA_EXCEPTION_RESULTS =
      Collections.singletonList(
          CompletableFutures.failedFuture(new KafkaException(EXCEPTION_MESSAGE)));
  private static final List<PartitionOffset> KAFKA_EXCEPTION_RESULTS =
      Collections.singletonList(
          new PartitionOffset(null, null, Errors.KAFKA_ERROR_ERROR_CODE, EXCEPTION_MESSAGE));
  private static final List<CompletableFuture<RecordMetadata>>
      PRODUCE_KAFKA_RETRIABLE_EXCEPTION_RESULTS =
          Collections.singletonList(
              CompletableFutures.failedFuture(new RetriableException(EXCEPTION_MESSAGE) {}));
  private static final List<PartitionOffset> KAFKA_RETRIABLE_EXCEPTION_RESULTS =
      Collections.singletonList(
          new PartitionOffset(
              null, null, Errors.KAFKA_RETRIABLE_ERROR_ERROR_CODE, EXCEPTION_MESSAGE));
  private static final List<CompletableFuture<RecordMetadata>>
      PRODUCE_KAFKA_AUTHENTICATION_EXCEPTION_RESULTS =
          Collections.singletonList(
              CompletableFutures.failedFuture(new SaslAuthenticationException(TOPIC_NAME) {}));
  private static final List<PartitionOffset> KAFKA_AUTHENTICATION_EXCEPTION_RESULTS =
      Collections.singletonList(
          new PartitionOffset(
              null,
              null,
              Errors.KAFKA_AUTHENTICATION_ERROR_CODE,
              new SaslAuthenticationException(TOPIC_NAME).getMessage()));
  private static final List<CompletableFuture<RecordMetadata>>
      PRODUCE_KAFKA_AUTHORIZATION_EXCEPTION_RESULTS =
          Collections.singletonList(
              CompletableFutures.failedFuture(new TopicAuthorizationException(TOPIC_NAME) {}));
  private static final List<PartitionOffset> KAFKA_AUTHORIZATION_EXCEPTION_RESULTS =
      Collections.singletonList(
          new PartitionOffset(
              null,
              null,
              Errors.KAFKA_AUTHORIZATION_ERROR_CODE,
              new TopicAuthorizationException(TOPIC_NAME).getMessage()));

  @Mock private SchemaManager schemaManager;

  @Mock private RecordSerializer recordSerializer;

  @Mock private ProduceController produceController;

  public TopicsResourceBinaryProduceTest() throws RestConfigException {
    addResource(
        new ProduceToTopicAction(
            () -> schemaManager, () -> recordSerializer, () -> produceController));
  }

  private Response produceToTopic(
      List<ProduceRecord> records, List<CompletableFuture<RecordMetadata>> results) {
    ProduceRequest request = ProduceRequest.create(records);

    for (int i = 0; i < request.getRecords().size(); i++) {
      ProduceRecord record = request.getRecords().get(i);
      Optional<ByteString> serializedKey =
          record.getKey().map(key -> ByteString.copyFromUtf8(String.valueOf(record.getKey())));
      Optional<ByteString> serializedValue =
          record
              .getValue()
              .map(value -> ByteString.copyFromUtf8(String.valueOf(record.getValue())));

      expect(
              recordSerializer.serialize(
                  EmbeddedFormat.BINARY,
                  TOPIC_NAME,
                  /* schema= */ Optional.empty(),
                  record.getKey().orElse(NullNode.getInstance()),
                  /* isKey= */ true))
          .andReturn(serializedKey);
      expect(
              recordSerializer.serialize(
                  EmbeddedFormat.BINARY,
                  TOPIC_NAME,
                  /* schema= */ Optional.empty(),
                  record.getValue().orElse(NullNode.getInstance()),
                  /* isKey= */ false))
          .andReturn(serializedValue);

      expect(
              produceController.produce(
                  /* clusterId= */ eq(""),
                  eq(TOPIC_NAME),
                  eq(record.getPartition()),
                  /* headers= */ eq(ImmutableMultimap.of()),
                  eq(serializedKey),
                  eq(serializedValue),
                  /* timestamp= */ isA(Instant.class)))
          .andReturn(results.get(i).thenApply(ProduceResult::fromRecordMetadata));
    }

    replay(schemaManager, recordSerializer, produceController);

    Response response =
        request("/topics/" + TOPIC_NAME, Versions.KAFKA_V2_JSON)
            .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));

    verify(schemaManager, recordSerializer, produceController);

    return response;
  }

  private void testProduceToTopicSuccess(List<ProduceRecord> records) {
    Response rawResponse = produceToTopic(records, PRODUCE_RESULTS);
    assertOKResponse(rawResponse, Versions.KAFKA_V2_JSON);
    ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

    assertEquals(OFFSETS, response.getOffsets());
    assertNull(response.getKeySchemaId());
    assertNull(response.getValueSchemaId());
  }

  @Test
  public void testProduceToTopicOnlyValues() {
    testProduceToTopicSuccess(PRODUCE_RECORDS_ONLY_VALUES);
  }

  @Test
  public void testProduceToTopicByKey() {
    testProduceToTopicSuccess(PRODUCE_RECORDS_WITH_KEYS);
  }

  @Test
  public void testProduceToTopicByPartition() {
    testProduceToTopicSuccess(PRODUCE_RECORDS_WITH_PARTITIONS);
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    testProduceToTopicSuccess(PRODUCE_RECORDS_WITH_PARTITIONS_AND_KEYS);
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    testProduceToTopicSuccess(PRODUCE_RECORDS_WITH_NULL_VALUES);
  }

  @Test
  public void testProduceInvalidRequest() {
    Response response =
        request("/topics/topic1", Versions.KAFKA_V2_JSON)
            .post(Entity.entity("{}", Versions.KAFKA_V2_JSON_BINARY));
    assertErrorResponse(
        ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
        response,
        ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
        null,
        Versions.KAFKA_V2_JSON);
  }

  private void testProduceToTopicException(
      List<CompletableFuture<RecordMetadata>> produceResults,
      List<PartitionOffset> produceExceptionResults) {
    Response rawResponse = produceToTopic(PRODUCE_EXCEPTION_DATA, produceResults);

    if (produceExceptionResults == null) {
      assertErrorResponse(
          Response.Status.INTERNAL_SERVER_ERROR,
          rawResponse,
          RestServerErrorException.DEFAULT_ERROR_CODE,
          AbstractProduceAction.UNEXPECTED_PRODUCER_EXCEPTION,
          Versions.KAFKA_V2_JSON);
    } else {
      assertOKResponse(rawResponse, Versions.KAFKA_V2_JSON);
      ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);
      assertEquals(produceExceptionResults, response.getOffsets());
      assertNull(response.getKeySchemaId());
      assertNull(response.getValueSchemaId());
    }
  }

  @Test
  public void testProduceToTopicGenericException() {
    // No results expected since a non-Kafka exception should cause an HTTP-level error
    testProduceToTopicException(PRODUCE_GENERIC_EXCEPTION_RESULTS, null);
  }

  @Test
  public void testProduceToTopicKafkaException() {
    testProduceToTopicException(PRODUCE_KAFKA_EXCEPTION_RESULTS, KAFKA_EXCEPTION_RESULTS);
  }

  @Test
  public void testProduceToTopicKafkaRetriableException() {
    testProduceToTopicException(
        PRODUCE_KAFKA_RETRIABLE_EXCEPTION_RESULTS, KAFKA_RETRIABLE_EXCEPTION_RESULTS);
  }

  @Test
  public void testProduceToTopicKafkaAuthorizationException() {
    testProduceSecurityException(
        PRODUCE_KAFKA_AUTHORIZATION_EXCEPTION_RESULTS,
        KAFKA_AUTHORIZATION_EXCEPTION_RESULTS,
        Response.Status.FORBIDDEN);
  }

  @Test
  public void testProduceToTopicKafkaAuthenticationException() {
    testProduceSecurityException(
        PRODUCE_KAFKA_AUTHENTICATION_EXCEPTION_RESULTS,
        KAFKA_AUTHENTICATION_EXCEPTION_RESULTS,
        Response.Status.UNAUTHORIZED);
  }

  private void testProduceSecurityException(
      List<CompletableFuture<RecordMetadata>> produceResults,
      List<PartitionOffset> produceExceptionResults,
      Response.Status expectedStatus) {
    Response rawResponse = produceToTopic(PRODUCE_EXCEPTION_DATA, produceResults);

    assertEquals(expectedStatus.getStatusCode(), rawResponse.getStatus());
    ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);
    assertEquals(produceExceptionResults, response.getOffsets());
    assertNull(response.getKeySchemaId());
    assertNull(response.getValueSchemaId());
  }
}
