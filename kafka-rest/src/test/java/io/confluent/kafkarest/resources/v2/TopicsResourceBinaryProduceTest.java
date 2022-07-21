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
import static org.junit.Assert.assertEquals;

import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Utils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryTopicProduceRequest.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.RestServerErrorException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

public class TopicsResourceBinaryProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  private static final String topicName = "topic1";

  private List<BinaryTopicProduceRecord> produceRecordsOnlyValues;
  private List<BinaryTopicProduceRecord> produceRecordsWithKeys;
  private List<BinaryTopicProduceRecord> produceRecordsWithPartitions;
  private List<BinaryTopicProduceRecord> produceRecordsWithPartitionsAndKeys;
  private List<BinaryTopicProduceRecord> produceRecordsWithNullValues;
  private List<RecordMetadataOrException> produceResults;
  private List<PartitionOffset> offsetResults;

  private List<BinaryTopicProduceRecord> produceExceptionData;
  private List<RecordMetadataOrException> produceGenericExceptionResults;
  private List<RecordMetadataOrException> produceKafkaExceptionResults;
  private List<PartitionOffset> kafkaExceptionResults;
  private List<RecordMetadataOrException> produceKafkaRetriableExceptionResults;
  private List<PartitionOffset> kafkaRetriableExceptionResults;
  private List<RecordMetadataOrException> produceKafkaAuthenticationExceptionResults;
  private List<PartitionOffset> kafkaAuthenticationExceptionResults;
  private List<RecordMetadataOrException> produceKafkaAuthorizationExceptionResults;
  private List<PartitionOffset> kafkaAuthorizationExceptionResults;
  private static final String exceptionMessage = "Error message";

  public TopicsResourceBinaryProduceTest() throws RestConfigException {
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config, producerPool, /* kafkaConsumerManager= */ null);

    addResource(new ProduceToTopicAction(ctx));

    produceRecordsOnlyValues = Arrays.asList(
        new BinaryTopicProduceRecord(null, "value", null),
        new BinaryTopicProduceRecord(null, "value2", null)
    );
    produceRecordsWithKeys = Arrays.asList(
        new BinaryTopicProduceRecord("key", "value", null),
        new BinaryTopicProduceRecord("key2", "value2", null)
    );
    produceRecordsWithPartitions = Arrays.asList(
        new BinaryTopicProduceRecord(null, "value", 0),
        new BinaryTopicProduceRecord(null, "value2", 0)
    );
    produceRecordsWithPartitionsAndKeys = Arrays.asList(
        new BinaryTopicProduceRecord("key", "value", 0),
        new BinaryTopicProduceRecord("key2", "value2", 0)
    );
    produceRecordsWithNullValues = Arrays.asList(
        new BinaryTopicProduceRecord("key", null, null),
        new BinaryTopicProduceRecord("key2", null, null)
    );

    TopicPartition tp0 = new TopicPartition(topicName, 0);
    produceResults = Arrays.asList(
        new RecordMetadataOrException(new RecordMetadata(tp0, 0L, 0L, 0L, 0L, 1, 1), null),
        new RecordMetadataOrException(new RecordMetadata(tp0, 0L, 1L, 0L, 0L, 1, 1), null)
    );

    offsetResults = Arrays.asList(
        new PartitionOffset(0, 0L, null, null),
        new PartitionOffset(0, 1L, null, null)
    );

    produceExceptionData = Arrays.asList(
        new BinaryTopicProduceRecord(null, null, null)
    );

    produceGenericExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new Exception(exceptionMessage))
    );

    produceKafkaExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new KafkaException(exceptionMessage))
    );
    kafkaExceptionResults = Arrays.asList(
        new PartitionOffset(null, null, Errors.KAFKA_ERROR_ERROR_CODE, exceptionMessage)
    );

    produceKafkaRetriableExceptionResults = Arrays.asList(
        new RecordMetadataOrException(null, new RetriableException(exceptionMessage) {
        })
    );
    kafkaRetriableExceptionResults = Arrays.asList(
        new PartitionOffset(null, null, Errors.KAFKA_RETRIABLE_ERROR_ERROR_CODE,
            exceptionMessage)
    );

    produceKafkaAuthorizationExceptionResults = Collections.singletonList(
        new RecordMetadataOrException(null, new TopicAuthorizationException(topicName) {
        })
    );
    kafkaAuthorizationExceptionResults = Collections.singletonList(
        new PartitionOffset(null, null, Errors.KAFKA_AUTHORIZATION_ERROR_CODE,
            new TopicAuthorizationException(topicName).getMessage())
    );

    produceKafkaAuthenticationExceptionResults = Collections.singletonList(
        new RecordMetadataOrException(null, new SaslAuthenticationException(topicName) {
        })
    );
    kafkaAuthenticationExceptionResults = Collections.singletonList(
        new PartitionOffset(null, null, Errors.KAFKA_AUTHENTICATION_ERROR_CODE,
            new SaslAuthenticationException(topicName).getMessage())
    );
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(producerPool);
  }

  private <K, V> Response produceToTopic(String topic, String acceptHeader, String requestMediatype,
      EmbeddedFormat recordFormat,
      List<BinaryTopicProduceRecord> records,
      final List<RecordMetadataOrException> results) {
    BinaryTopicProduceRequest request = BinaryTopicProduceRequest.create(records);
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        Capture.newInstance();
    producerPool.produce(EasyMock.eq(topic),
        EasyMock.eq((Integer) null),
        EasyMock.eq(recordFormat),
        EasyMock.anyObject(),
        EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (results == null) {
          throw new Exception();
        } else {
          produceCallback.getValue().onCompletion((Integer) null, (Integer) null, results);
        }
        return null;
      }
    });
    EasyMock.replay(producerPool);

    Response response = request("/topics/" + topic, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(producerPool);

    return response;
  }

  private void testProduceToTopicSuccess(List<BinaryTopicProduceRecord> records) {
    Response rawResponse =
        produceToTopic("topic1", Versions.KAFKA_V2_JSON, Versions.KAFKA_V2_JSON_BINARY,
            EmbeddedFormat.BINARY, records, produceResults);
    assertOKResponse(rawResponse, Versions.KAFKA_V2_JSON);
    ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

    assertEquals(offsetResults, response.getOffsets());
    assertEquals(null, response.getKeySchemaId());
    assertEquals(null, response.getValueSchemaId());

    EasyMock.reset(producerPool);
  }

  @Test
  public void testProduceToTopicOnlyValues() {
    testProduceToTopicSuccess(produceRecordsOnlyValues);
  }

  @Test
  public void testProduceToTopicByKey() {
    testProduceToTopicSuccess(produceRecordsWithKeys);
  }

  @Test
  public void testProduceToTopicByPartition() {
    testProduceToTopicSuccess(produceRecordsWithPartitions);
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    testProduceToTopicSuccess(produceRecordsWithPartitionsAndKeys);
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    testProduceToTopicSuccess(produceRecordsWithNullValues);
  }

  @Test
  public void testProduceToTopicFailure() {
    // null offsets triggers a generic exception
    Response
        rawResponse =
        produceToTopic("topic1", Versions.KAFKA_V2_JSON, Versions.KAFKA_V2_JSON_BINARY,
            EmbeddedFormat.BINARY,
            produceRecordsWithKeys, null);
    assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
        Versions.KAFKA_V2_JSON);

    EasyMock.reset(producerPool);
  }

  @Test
  public void testProduceInvalidRequest() {
    Response response = request("/topics/topic1", Versions.KAFKA_V2_JSON)
        .post(Entity.entity("{}", Versions.KAFKA_V2_JSON_BINARY));
    assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
        response,
        ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
        null,
        Versions.KAFKA_V2_JSON);

    // Invalid base64 encoding
    response = request("/topics/topic1", Versions.KAFKA_V2_JSON)
        .post(Entity
            .entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", Versions.KAFKA_V2_JSON_BINARY));
    assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
        response,
        ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
        null,
        Versions.KAFKA_V2_JSON);
  }

  private void testProduceToTopicException(List<RecordMetadataOrException> produceResults,
      List<PartitionOffset> produceExceptionResults) {
    Response
        rawResponse =
        produceToTopic("topic1", Versions.KAFKA_V2_JSON, Versions.KAFKA_V2_JSON_BINARY,
            EmbeddedFormat.BINARY, produceExceptionData, produceResults);

    if (produceExceptionResults == null) {
      assertErrorResponse(
          Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
          RestServerErrorException.DEFAULT_ERROR_CODE, Utils.UNEXPECTED_PRODUCER_EXCEPTION,
          Versions.KAFKA_V2_JSON
      );
    } else {
      assertOKResponse(rawResponse, Versions.KAFKA_V2_JSON);
      ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);
      assertEquals(produceExceptionResults, response.getOffsets());
      assertEquals(null, response.getKeySchemaId());
      assertEquals(null, response.getValueSchemaId());
    }

    EasyMock.reset(producerPool);
  }

  @Test
  public void testProduceToTopicGenericException() {
    // No results expected since a non-Kafka exception should cause an HTTP-level error
    testProduceToTopicException(produceGenericExceptionResults, null);
  }

  @Test
  public void testProduceToTopicKafkaException() {
    testProduceToTopicException(produceKafkaExceptionResults, kafkaExceptionResults);
  }

  @Test
  public void testProduceToTopicKafkaRetriableException() {
    testProduceToTopicException(produceKafkaRetriableExceptionResults,
        kafkaRetriableExceptionResults);
  }

  @Test
  public void testProduceToTopicKafkaAuthorizationException() {
    testProduceSecurityException(produceKafkaAuthorizationExceptionResults,
        kafkaAuthorizationExceptionResults, Response.Status.FORBIDDEN);
  }

  @Test
  public void testProduceToTopicKafkaAuthenticationException() {
    testProduceSecurityException(produceKafkaAuthenticationExceptionResults,
        kafkaAuthenticationExceptionResults, Response.Status.UNAUTHORIZED);
  }

  private void testProduceSecurityException(List<RecordMetadataOrException> produceResults,
      List<PartitionOffset> produceExceptionResults,
      Response.Status expectedStatus) {
    Response
        rawResponse =
        produceToTopic("topic1", Versions.KAFKA_V2_JSON, Versions.KAFKA_V2_JSON_BINARY,
            EmbeddedFormat.BINARY, produceExceptionData, produceResults);

    assertEquals(expectedStatus.getStatusCode(), rawResponse.getStatus());
    ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);
    assertEquals(produceExceptionResults, response.getOffsets());
    assertEquals(null, response.getKeySchemaId());
    assertEquals(null, response.getValueSchemaId());

    EasyMock.reset(producerPool);
  }
}
