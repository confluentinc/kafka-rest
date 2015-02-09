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

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.BinaryTopicProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.entities.TopicProduceResponse;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class TopicsResourceBinaryProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private Context ctx;

  private List<BinaryTopicProduceRecord> produceRecordsOnlyValues;
  private List<BinaryTopicProduceRecord> produceRecordsWithKeys;
  private List<BinaryTopicProduceRecord> produceRecordsWithPartitions;
  private List<BinaryTopicProduceRecord> produceRecordsWithPartitionsAndKeys;
  private List<BinaryTopicProduceRecord> produceRecordsWithNullValues;
  // Partition -> New offset
  private Map<Integer, Long> produceOffsets;

  public TopicsResourceBinaryProduceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new Context(config, mdObserver, producerPool, null);

    addResource(new TopicsResource(ctx));

    produceRecordsOnlyValues = Arrays.asList(
        new BinaryTopicProduceRecord("value".getBytes()),
        new BinaryTopicProduceRecord("value2".getBytes())
    );
    produceRecordsWithKeys = Arrays.asList(
        new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes()),
        new BinaryTopicProduceRecord("key2".getBytes(), "value2".getBytes())
    );
    produceRecordsWithPartitions = Arrays.asList(
        new BinaryTopicProduceRecord("value".getBytes(), 0),
        new BinaryTopicProduceRecord("value2".getBytes(), 0)
    );
    produceRecordsWithPartitionsAndKeys = Arrays.asList(
        new BinaryTopicProduceRecord("key".getBytes(), "value".getBytes(), 0),
        new BinaryTopicProduceRecord("key2".getBytes(), "value2".getBytes(), 0)
    );
    produceRecordsWithNullValues = Arrays.asList(
        new BinaryTopicProduceRecord("key".getBytes(), (byte[]) null),
        new BinaryTopicProduceRecord("key2".getBytes(), (byte[]) null)
    );
    produceOffsets = new HashMap<Integer, Long>();
    produceOffsets.put(0, 1L);
    produceOffsets.put(1, 2L);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, producerPool);
  }

  private <K, V> Response produceToTopic(String topic, String acceptHeader, String requestMediatype,
                                         EmbeddedFormat recordFormat,
                                         List<? extends TopicProduceRecord<K, V>> records,
                                         final Map<Integer, Long> resultOffsets) {
    final TopicProduceRequest request = new TopicProduceRequest();
    request.setRecords(records);
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        new Capture<ProducerPool.ProduceRequestCallback>();
    EasyMock.expect(mdObserver.topicExists(topic)).andReturn(true);
    producerPool.produce(EasyMock.eq(topic),
                         EasyMock.eq((Integer) null),
                         EasyMock.eq(recordFormat),
                         EasyMock.<SchemaHolder>anyObject(),
                         EasyMock.<Collection<? extends ProduceRecord<K, V>>>anyObject(),
                         EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (resultOffsets == null) {
          produceCallback.getValue().onException(new Exception());
        } else {
          produceCallback.getValue().onCompletion((Integer) null, (Integer) null, resultOffsets);
        }
        return null;
      }
    });
    EasyMock.replay(mdObserver, producerPool);

    Response response = request("/topics/" + topic, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(mdObserver, producerPool);

    return response;
  }

  @Test
  public void testProduceToTopicOnlyValues() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsOnlyValues, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicByKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsWithKeys, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicByPartition() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsWithPartitions, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicWithPartitionAndKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsWithPartitionsAndKeys, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicWithNullValues() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsWithNullValues, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals(null, response.getKeySchemaId());
        assertEquals(null, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToTopicFailure() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        // null offsets triggers a generic exception
        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.BINARY,
                           produceRecordsWithKeys, null);
        assertErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
                            mediatype.expected);

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceInvalidRequest() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_BINARY) {
        Response response = request("/topics/topic1", mediatype.header)
            .post(Entity.entity("{}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                            response,
                            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
                            null,
                            mediatype.expected);

        // Invalid base64 encoding
        response = request("/topics/topic1", mediatype.header)
            .post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                            response,
                            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
                            null,
                            mediatype.expected);
      }
    }
  }
}
