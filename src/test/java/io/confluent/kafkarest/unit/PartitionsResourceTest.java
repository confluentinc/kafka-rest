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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.MetadataObserver;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.ProducerRecordProxyCollection;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.entities.TopicProduceRecord;
import io.confluent.kafkarest.entities.TopicProduceRequest;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.rest.EmbeddedServerTestHarness;
import io.confluent.rest.RestConfigException;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;

import static io.confluent.kafkarest.TestUtils.assertErrorResponse;
import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

public class PartitionsResourceTest
    extends EmbeddedServerTestHarness<KafkaRestConfig, KafkaRestApplication> {

  private MetadataObserver mdObserver;
  private ProducerPool producerPool;
  private Context ctx;

  private final String topicName = "topic1";
  Topic topic = new Topic("topic1", 2);
  private final List<Partition> partitions = Arrays.asList(
      new Partition(0, 0, Arrays.asList(
          new PartitionReplica(0, true, true),
          new PartitionReplica(1, false, false)
      )),
      new Partition(1, 1, Arrays.asList(
          new PartitionReplica(0, false, true),
          new PartitionReplica(1, true, true)
      ))
  );

  private List<ProduceRecord> produceRecordsOnlyValues;
  private List<ProduceRecord> produceRecordsWithKeys;
  // Partition -> New offset
  private Map<Integer, Long> produceOffsets;


  public PartitionsResourceTest() throws RestConfigException {
    mdObserver = EasyMock.createMock(MetadataObserver.class);
    producerPool = EasyMock.createMock(ProducerPool.class);
    ctx = new Context(config, mdObserver, producerPool, null);

    addResource(new TopicsResource(ctx));
    addResource(PartitionsResource.class);

    produceRecordsOnlyValues = Arrays.asList(
        new ProduceRecord("value".getBytes()),
        new ProduceRecord("value2".getBytes())
    );
    produceRecordsWithKeys = Arrays.asList(
        new ProduceRecord("key".getBytes(), "value".getBytes()),
        new ProduceRecord("key2".getBytes(), "value2".getBytes())
    );
    produceOffsets = new HashMap<Integer, Long>();
    produceOffsets.put(0, 1L);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    EasyMock.reset(mdObserver, producerPool);
  }

  @Test
  public void testGetPartitions() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
      EasyMock.expect(mdObserver.getTopicPartitions(topicName))
          .andReturn(partitions);

      EasyMock.replay(mdObserver);

      Response response = request("/topics/topic1/partitions", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      List<Partition> partitionsResponse = response.readEntity(new GenericType<List<Partition>>() {
      });
      assertEquals(partitions, partitionsResponse);

      EasyMock.verify(mdObserver);
      EasyMock.reset(mdObserver);
    }
  }

  @Test
  public void testGetPartition() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
      EasyMock.expect(mdObserver.getTopicPartition(topicName, 0))
          .andReturn(partitions.get(0));
      EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
      EasyMock.expect(mdObserver.getTopicPartition(topicName, 1))
          .andReturn(partitions.get(1));

      EasyMock.replay(mdObserver);

      Response response = request("/topics/topic1/partitions/0", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      Partition partition = response.readEntity(new GenericType<Partition>() {
      });
      assertEquals(partitions.get(0), partition);

      response = request("/topics/topic1/partitions/1", mediatype.header).get();
      assertOKResponse(response, mediatype.expected);
      partition = response.readEntity(new GenericType<Partition>() {
      });
      assertEquals(partitions.get(1), partition);

      EasyMock.verify(mdObserver);
      EasyMock.reset(mdObserver);
    }
  }

  @Test
  public void testGetInvalidPartition() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
      EasyMock.expect(mdObserver.getTopicPartition(topicName, 1000))
          .andReturn(null);
      EasyMock.replay(mdObserver);

      Response response = request("/topics/topic1/partitions/1000", mediatype.header).get();
      assertErrorResponse(Response.Status.NOT_FOUND, response,
                          Errors.PARTITION_NOT_FOUND_ERROR_CODE,
                          Errors.PARTITION_NOT_FOUND_MESSAGE,
                          mediatype.expected);

      EasyMock.verify(mdObserver);
      EasyMock.reset(mdObserver);
    }
  }


  private Response produceToPartition(String topic, int partition, String acceptHeader,
                                      String requestMediatype, List<ProduceRecord> records,
                                      final Map<Integer, Long> resultOffsets) {
    final PartitionProduceRequest request = new PartitionProduceRequest();
    request.setRecords(records);
    final Capture<ProducerPool.ProduceRequestCallback>
        produceCallback =
        new Capture<ProducerPool.ProduceRequestCallback>();
    EasyMock.expect(mdObserver.topicExists(topic)).andReturn(true);
    EasyMock.expect(mdObserver.partitionExists(topic, partition)).andReturn(true);
    producerPool.produce(EasyMock.<ProducerRecordProxyCollection>anyObject(),
                         EasyMock.capture(produceCallback));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        if (resultOffsets == null) {
          produceCallback.getValue().onException(new Exception());
        } else {
          produceCallback.getValue().onCompletion(resultOffsets);
        }
        return null;
      }
    });
    EasyMock.replay(mdObserver, producerPool);

    Response
        response =
        request("/topics/" + topic + "/partitions/" + ((Integer) partition).toString(),
                acceptHeader)
            .post(Entity.entity(request, requestMediatype));

    EasyMock.verify(mdObserver, producerPool);

    return response;
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                               produceRecordsOnlyValues, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        PartitionOffset response = rawResponse.readEntity(PartitionOffset.class);

        assertEquals(new PartitionOffset(0, 1L), response);
        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToPartitionByKey() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                               produceRecordsWithKeys, produceOffsets);
        assertOKResponse(rawResponse, mediatype.expected);
        PartitionOffset response = rawResponse.readEntity(PartitionOffset.class);

        assertEquals(new PartitionOffset(0, 1L), response);
        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceToPartitionFailure() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        // null offsets triggers a generic exception
        Response
            rawResponse =
            produceToPartition(topicName, 0, mediatype.header, requestMediatype,
                               produceRecordsWithKeys, null);
        assertErrorResponse(
            Response.Status.INTERNAL_SERVER_ERROR, rawResponse,
            mediatype.expected
        );

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }

  @Test
  public void testProduceInvalidRequest() {
    for (TestUtils.RequestMediaType mediatype : TestUtils.V1_ACCEPT_MEDIATYPES) {
      for (String requestMediatype : TestUtils.V1_REQUEST_ENTITY_TYPES) {
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        Response response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity("{}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                            response,
                            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
                            null,
                            mediatype.expected);
        EasyMock.verify();

        // Invalid base64 encoding
        EasyMock.reset(mdObserver);
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity("{\"records\":[{\"value\":\"aGVsbG8==\"}]}", requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                            response,
                            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
                            null,
                            mediatype.expected);
        EasyMock.verify();

        // Invalid data -- include partition in request
        EasyMock.reset(mdObserver);
        EasyMock.expect(mdObserver.topicExists(topicName)).andReturn(true);
        EasyMock.replay(mdObserver);
        TopicProduceRequest topicRequest = new TopicProduceRequest();
        topicRequest.setRecords(
            Arrays.asList(new TopicProduceRecord("key".getBytes(), "value".getBytes(), 0)));
        response = request("/topics/" + topicName + "/partitions/0", mediatype.header)
            .post(Entity.entity(topicRequest, requestMediatype));
        assertErrorResponse(ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY,
                            response,
                            ConstraintViolationExceptionMapper.UNPROCESSABLE_ENTITY_CODE,
                            null,
                            mediatype.expected);
        EasyMock.verify();

        EasyMock.reset(mdObserver, producerPool);
      }
    }
  }
}
