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

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
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
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.entities.TopicProduceResponse;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

// This test is much lighter than the Binary one since they would otherwise be mostly duplicated
// -- this just sanity checks the Jersey processing of these requests.
public class TopicsResourceAvroProduceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private Context ctx;

  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";
  private static final Schema keySchema = new Schema.Parser().parse(keySchemaStr);
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);

  private final static JsonNode[] testKeys = {
      TestUtils.jsonTree("1"),
      TestUtils.jsonTree("2"),
  };

  private final static JsonNode[] testValues = {
      TestUtils.jsonTree("{\"field\": 1}"),
      TestUtils.jsonTree("{\"field\": 2}"),
  };

  private List<AvroTopicProduceRecord> produceRecordsWithPartitionsAndKeys;

  // Partition -> New offset
  private Map<Integer, Long> produceOffsets;

  public TopicsResourceAvroProduceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new Context(config, mdObserver, producerPool, null);

    addResource(new TopicsResource(ctx));

    produceRecordsWithPartitionsAndKeys = Arrays.asList(
        new AvroTopicProduceRecord(testKeys[0], testValues[0], 0),
        new AvroTopicProduceRecord(testKeys[1], testValues[1], 0)
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
                                         TopicProduceRequest request,
                                         final Map<Integer, Long> resultOffsets) {
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
          produceCallback.getValue().onCompletion(1, 2, resultOffsets);
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
  public void testProduceToTopicWithPartitionAndKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchema(keySchemaStr);
        request.setValueSchema(valueSchemaStr);

        Response
            rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.AVRO,
                           request, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        TopicProduceResponse response = rawResponse.readEntity(TopicProduceResponse.class);

        assertEquals(
            Arrays.asList(new PartitionOffset(0, 1L), new PartitionOffset(1, 2L)),
            response.getOffsets()
        );
        assertEquals((Integer) 1, response.getKeySchemaId());
        assertEquals((Integer) 2, response.getValueSchemaId());

        EasyMock.reset(mdObserver, producerPool);
      }
    }

    // Test using schema IDs
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchemaId(1);
        request.setValueSchemaId(2);

        Response rawResponse =
            produceToTopic("topic1", mediatype.header, requestMediatype,
                           EmbeddedFormat.AVRO,
                           request, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  private void produceToTopicExpectFailure(String topicName, String acceptHeader,
                                           String requestMediatype, TopicProduceRequest request,
                                           String responseMediaType, int errorCode) {
    Response rawResponse = request("/topics/" + topicName, acceptHeader)
        .post(Entity.entity(request, requestMediatype));

    assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                        rawResponse, errorCode, null, responseMediaType);
  }

  @Test
  public void testMissingKeySchema() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setValueSchema(valueSchemaStr);

        produceToTopicExpectFailure("topic1", mediatype.header, requestMediatype,
                                    request, mediatype.expected,
                                    Errors.KEY_SCHEMA_MISSING_ERROR_CODE);
      }
    }
  }

  @Test
  public void testMissingValueSchema() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES_AVRO) {
        final TopicProduceRequest request = new TopicProduceRequest();
        request.setRecords(produceRecordsWithPartitionsAndKeys);
        request.setKeySchema(keySchemaStr);

        produceToTopicExpectFailure("topic1", mediatype.header, requestMediatype,
                                    request, mediatype.expected,
                                    Errors.VALUE_SCHEMA_MISSING_ERROR_CODE);
      }
    }
  }
}
