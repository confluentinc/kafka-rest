/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.unit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.AdminClientWrapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.BinaryProduceRecord;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class PartitionsResourceBinaryProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private AdminClientWrapper adminClientWrapper;
  private ProducerPool producerPool;
  private DefaultKafkaRestContext ctx;

  private final String topicName = "topic1";

  private List<BinaryProduceRecord> produceRecordsOnlyValues;
  private List<BinaryProduceRecord> produceRecordsWithKeys;
  private List<RecordMetadataOrException> produceResults;
  private final List<PartitionOffset> offsetResults;

  public PartitionsResourceBinaryProduceTest() throws RestConfigException {
    adminClientWrapper = EasyMock.createMock(AdminClientWrapper.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new DefaultKafkaRestContext(config,
            null,
            producerPool,
            null,
            null,
            null,
            adminClientWrapper
        );

    addResource(new TopicsResource(ctx));
    addResource(new PartitionsResource(ctx));

    produceRecordsOnlyValues = Arrays.asList(
        new BinaryProduceRecord("value".getBytes()),
        new BinaryProduceRecord("value2".getBytes())
    );
    produceRecordsWithKeys = Arrays.asList(
        new BinaryProduceRecord("key".getBytes(), "value".getBytes()),
        new BinaryProduceRecord("key2".getBytes(), "value2".getBytes())
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
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(adminClientWrapper, producerPool);
  }

  private <K, V> Response produceToPartition(String topic, int partition, String acceptHeader,
      String requestMediatype,
      EmbeddedFormat recordFormat,
      List<? extends ProduceRecord<K, V>> records,
                                             final List<RecordMetadataOrException> results) {
    final PartitionProduceRequest request = new PartitionProduceRequest();
    request.setRecords(records);
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        new Capture<ProducerPool.ProduceRequestCallback>();
    EasyMock.expect(adminClientWrapper.topicExists(topic)).andReturn(true);
    EasyMock.expect(adminClientWrapper.partitionExists(topic, partition)).andReturn(true);
    producerPool.produce(EasyMock.eq(topic),
        EasyMock.eq(partition),
        EasyMock.eq(recordFormat),
        EasyMock.<SchemaHolder>anyObject(),
        EasyMock.<Collection<? extends ProduceRecord<K, V>>>anyObject(),
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
    EasyMock.replay(adminClientWrapper, producerPool);

    Response
        response =
        request("/topics/" + topic + "/partitions/" + ((Integer) partition).toString(),
                acceptHeader)
            .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(producerPool);

    return response;
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                               EmbeddedFormat.BINARY,
                               produceRecordsOnlyValues, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }

  @Test
  public void testProduceToPartitionByKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                EmbeddedFormat.BINARY,
                               produceRecordsWithKeys, produceResults);
        assertOKResponse(rawResponse, mediatype.expected);
        ProduceResponse response = TestUtils.tryReadEntityOrLog(rawResponse, ProduceResponse.class);

        assertEquals(offsetResults, response.getOffsets());
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }

  @Test
  public void testProduceToPartitionFailure() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        // null offsets triggers a generic exception
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                               EmbeddedFormat.BINARY,
                               produceRecordsWithKeys, null);
        assertErrorResponse(
            Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
            mediatype.expected
        );

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }

  @Test
  public void testProduceInvalidRequest() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        EasyMock.expect(adminClientWrapper.topicExists(topicName)).andReturn(true);
        EasyMock.replay(adminClientWrapper);
        Response response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity("{}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
            response,
            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
            null,
            mediatype.expected);
        EasyMock.verify();

        // Invalid base64 encoding
        response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
            response,
            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
            null,
            mediatype.expected);
        EasyMock.verify();

        // Invalid data -- include partition in request
        TopicProduceRequest topicRequest = new TopicProduceRequest();
        topicRequest.setRecords(
            Arrays.asList(new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes(), 0)));
        response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity(topicRequest, requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
            response,
            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
            null,
            mediatype.expected);
        EasyMock.verify();

        EasyMock.reset(adminClientWrapper, producerPool);
      }
    }
  }
}
